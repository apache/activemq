/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ReplicaSequencer {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSequencer.class);

    private static final String SOURCE_CONSUMER_CLIENT_ID = "DUMMY_SOURCE_CONSUMER";
    private static final String SEQUENCE_NAME = "primarySeq";

    private final Broker broker;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final Object ackIteratingMutex = new Object();
    private final Object sendIteratingMutex = new Object();
    private final AtomicLong pendingAckWakeups = new AtomicLong();
    private final AtomicLong pendingSendWakeups = new AtomicLong();
    private final AtomicLong pendingSendTriggeredWakeups = new AtomicLong();
    final Set<String> deliveredMessages = new HashSet<>();
    final LinkedList<String> messageToAck = new LinkedList<>();
    private final ReplicaAckHelper replicaAckHelper;
    private final ReplicaPolicy replicaPolicy;
    private final ReplicaBatcher replicaBatcher;

    ReplicaCompactor replicaCompactor;
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator customerIdGenerator = new LongSequenceGenerator();
    private TaskRunner ackTaskRunner;
    private TaskRunner sendTaskRunner;
    private Queue mainQueue;
    private ConnectionContext connectionContext;
    private ScheduledExecutorService scheduler;

    private PrefetchSubscription subscription;
    boolean hasConsumer;
    ReplicaSequenceStorage sequenceStorage;

    BigInteger sequence = BigInteger.ZERO;
    MessageId recoveryMessageId;

    private final AtomicLong lastProcessTime = new AtomicLong();

    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSequencer(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ReplicationMessageProducer replicationMessageProducer, ReplicaPolicy replicaPolicy) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaAckHelper = new ReplicaAckHelper(broker);
        this.replicaPolicy = replicaPolicy;
        this.replicaBatcher = new ReplicaBatcher(replicaPolicy);

        scheduleExecutor();
    }

    void initialize() throws Exception {
        TaskRunnerFactory taskRunnerFactory = broker.getBrokerService().getTaskRunnerFactory();
        ackTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateAck, "ReplicationPlugin.Sequencer.Ack");
        sendTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateSend, "ReplicationPlugin.Sequencer.Send");

        Queue intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        mainQueue = broker.getDestinations(queueProvider.getMainQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        if (connectionContext == null) {
            connectionContext = createConnectionContext();
        }
        if (sequenceStorage == null) {
            sequenceStorage = new ReplicaSequenceStorage(broker, connectionContext,
                    queueProvider, replicaInternalMessageProducer, SEQUENCE_NAME);
        }

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, customerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10000);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        replicaCompactor = new ReplicaCompactor(broker, connectionContext, queueProvider, subscription,
                replicaPolicy.getCompactorAdditionalMessagesLimit());

        String savedSequence = sequenceStorage.initialize();
        restoreSequence(savedSequence, intermediateQueue);

        initialized.compareAndSet(false, true);
        asyncSendWakeup();
    }

    void deinitialize() throws Exception {
        if (!initialized.get()) {
            return;
        }

        if (ackTaskRunner != null) {
            ackTaskRunner.shutdown();
            ackTaskRunner = null;
        }

        if (sendTaskRunner != null) {
            sendTaskRunner.shutdown();
            sendTaskRunner = null;
        }

        mainQueue = null;

        if (subscription != null) {
            try {
                broker.removeConsumer(connectionContext, subscription.getConsumerInfo());
            } catch (BrokerStoppedException ignored) {}
            subscription = null;
        }

        replicaCompactor = null;

        if (sequenceStorage != null) {
            sequenceStorage.deinitialize();
        }

        initialized.compareAndSet(true, false);

    }

    void scheduleExecutor() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::asyncSendWakeup,
                replicaPolicy.getSourceSendPeriod(), replicaPolicy.getSourceSendPeriod(), TimeUnit.MILLISECONDS);
    }

    void terminateScheduledExecutor() {
        scheduler.shutdownNow();
    }

    void restoreSequence(String savedSequence, Queue intermediateQueue) throws Exception {
        if (savedSequence == null) {
            return;
        }
        String[] split = savedSequence.split("#");
        if (split.length != 2) {
            return;
        }
        sequence = new BigInteger(split[0]);

        recoveryMessageId = new MessageId(split[1]);
        int index = intermediateQueue.getAllMessageIds().indexOf(recoveryMessageId);
        if (index == -1) {
            return;
        }

        sequence = sequence.subtract(BigInteger.valueOf(index + 1));
    }

    @SuppressWarnings("unchecked")
    void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        List<MessageReference> messagesToAck = replicaAckHelper.getMessagesToAck(ack, mainQueue);

        if (messagesToAck == null || messagesToAck.isEmpty()) {
            throw new IllegalStateException("Could not find messages for ack");
        }
        List<String> messageIds = new ArrayList<>();
        for (MessageReference reference : messagesToAck) {
            messageIds.addAll((List<String>) reference.getMessage().getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));
        }

        broker.acknowledge(consumerExchange, ack);
        synchronized (messageToAck) {
            messageIds.forEach(messageToAck::addLast);
        }
        asyncAckWakeup();
    }

    void updateMainQueueConsumerStatus() {
        try {
            if (!hasConsumer && !mainQueue.getConsumers().isEmpty()) {
                hasConsumer = true;
                asyncSendWakeup();
            } else if (hasConsumer && mainQueue.getConsumers().isEmpty()) {
                hasConsumer = false;
            }
        } catch (Exception error) {
            logger.error("Failed to update replica consumer count.", error);
        }
    }

    void asyncAckWakeup() {
        try {
            pendingAckWakeups.incrementAndGet();
            ackTaskRunner.wakeup();
        } catch (InterruptedException e) {
            logger.warn("Async task runner failed to wakeup ", e);
        }
    }

    void asyncSendWakeup() {
        try {
            long l = pendingSendWakeups.incrementAndGet();
            if (l % replicaPolicy.getMaxBatchLength() == 0) {
                pendingSendTriggeredWakeups.incrementAndGet();
                sendTaskRunner.wakeup();
                pendingSendWakeups.addAndGet(-replicaPolicy.getMaxBatchLength());
                return;
            }

            if (System.currentTimeMillis() - lastProcessTime.get() > replicaPolicy.getSourceSendPeriod()) {
                pendingSendTriggeredWakeups.incrementAndGet();
                sendTaskRunner.wakeup();
            }

            if (!hasConsumer) {
                pendingSendTriggeredWakeups.incrementAndGet();
                sendTaskRunner.wakeup();
            }
        } catch (InterruptedException e) {
            logger.warn("Async task runner failed to wakeup ", e);
        }
    }

    boolean iterateAck() {
        synchronized (ackIteratingMutex) {
            iterateAck0();

            if (pendingAckWakeups.get() > 0) {
                pendingAckWakeups.decrementAndGet();
            }
        }

        return pendingAckWakeups.get() > 0;
    }

    private void iterateAck0() {
        MessageAck ack = new MessageAck();
        List<String> messages;
        synchronized (messageToAck) {
            if (!messageToAck.isEmpty()) {
                ack.setFirstMessageId(new MessageId(messageToAck.getFirst()));
                ack.setLastMessageId(new MessageId(messageToAck.getLast()));
                ack.setMessageCount(messageToAck.size());
                ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                ack.setDestination(queueProvider.getIntermediateQueue());
            }
            messages = new ArrayList<>(messageToAck);
        }

        if (!messages.isEmpty()) {
            TransactionId transactionId = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
            boolean rollbackOnFail = false;
            try {
                ack.setTransactionId(transactionId);

                synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                    broker.beginTransaction(connectionContext, transactionId);
                    rollbackOnFail = true;

                    ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
                    consumerExchange.setConnectionContext(connectionContext);
                    consumerExchange.setSubscription(subscription);

                    broker.acknowledge(consumerExchange, ack);

                    broker.commitTransaction(connectionContext, transactionId, true);
                }
                synchronized (messageToAck) {
                    messageToAck.removeAll(messages);
                }

                synchronized (deliveredMessages) {
                    messages.forEach(deliveredMessages::remove);
                }
            } catch (Exception e) {
                logger.error("Could not acknowledge replication messages", e);
                if (rollbackOnFail) {
                    try {
                        broker.rollbackTransaction(connectionContext, transactionId);
                    } catch (Exception ex) {
                        logger.error("Could not rollback transaction", ex);
                    }
                }
            }
        }
    }

    boolean iterateSend() {
        synchronized (sendIteratingMutex) {
            lastProcessTime.set(System.currentTimeMillis());
            if (!initialized.get()) {
                return false;
            }

            iterateSend0();

            if (pendingSendTriggeredWakeups.get() > 0) {
                pendingSendTriggeredWakeups.decrementAndGet();
            }
        }

        return pendingSendTriggeredWakeups.get() > 0;
    }

    private void iterateSend0() {
        List<MessageReference> dispatched = subscription.getDispatched();
        List<MessageReference> toProcess = new ArrayList<>();

        MessageReference recoveryMessage = null;

        synchronized (deliveredMessages) {
            Collections.reverse(dispatched);
            for (MessageReference reference : dispatched) {
                MessageId messageId = reference.getMessageId();
                if (deliveredMessages.contains(messageId.toString())) {
                    break;
                }
                toProcess.add(reference);
                if (messageId.equals(recoveryMessageId)) {
                    recoveryMessage = reference;
                }
            }
        }

        if (toProcess.isEmpty() && hasConsumer) {
            return;
        }

        Collections.reverse(toProcess);

        if (recoveryMessage != null) {
            toProcess = toProcess.subList(0, toProcess.indexOf(recoveryMessage) + 1);
        }

        if (recoveryMessageId == null) {
            try {
                toProcess = replicaCompactor.compactAndFilter(toProcess, !hasConsumer && subscription.isFull());
            } catch (Exception e) {
                logger.error("Failed to compact messages in the intermediate replication queue", e);
                return;
            }
            if (!hasConsumer) {
                asyncSendWakeup();
                return;
            }
        }

        if (toProcess.isEmpty()) {
            return;
        }

        List<List<MessageReference>> batches;
        try {
            batches = replicaBatcher.batches(toProcess);
        } catch (Exception e) {
            logger.error("Filed to batch messages in the intermediate replication queue", e);
            return;
        }

        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
        boolean rollbackOnFail = false;

        try {
            broker.beginTransaction(connectionContext, transactionId);

            BigInteger newSequence = sequence;
            for (List<MessageReference> batch : batches) {
                rollbackOnFail = true;
                newSequence = enqueueReplicaEvent(batch, newSequence, transactionId);
            }

            sequenceStorage.enqueue(transactionId, newSequence.toString() + "#" + toProcess.get(toProcess.size() - 1).getMessageId());

            broker.commitTransaction(connectionContext, transactionId, true);

            sequence = newSequence;
        } catch (Exception e) {
            logger.error("Failed to persist messages in the main replication queue", e);
            if (rollbackOnFail) {
                try {
                    broker.rollbackTransaction(connectionContext, transactionId);
                } catch (Exception ex) {
                    logger.error("Could not rollback transaction", ex);
                }
            }
            return;
        }

        synchronized (deliveredMessages) {
            deliveredMessages.addAll(toProcess.stream().map(MessageReference::getMessageId).map(MessageId::toString).collect(Collectors.toList()));
        }

        if (recoveryMessage != null) {
            recoveryMessageId = null;
        }
    }

    private BigInteger enqueueReplicaEvent(List<MessageReference> batch, BigInteger sequence, TransactionId transactionId) throws Exception {
        List<String> messageIds = new ArrayList<>();
        List<DataStructure> messages = new ArrayList<>();
        for (MessageReference reference : batch) {
            ActiveMQMessage originalMessage = (ActiveMQMessage) reference.getMessage();
            sequence = sequence.add(BigInteger.ONE);

            ActiveMQMessage message = (ActiveMQMessage) originalMessage.copy();

            message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, sequence.toString());

            message.setDestination(null);
            message.setTransactionId(null);
            message.setPersistent(false);

            messageIds.add(reference.getMessageId().toString());
            messages.add(message);
        }

        ReplicaEvent replicaEvent = new ReplicaEvent()
                .setEventType(ReplicaEventType.BATCH)
                .setEventData(eventSerializer.serializeListOfObjects(messages))
                .setTransactionId(transactionId)
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds);

        replicationMessageProducer.enqueueMainReplicaEvent(connectionContext, replicaEvent);

        return sequence;
    }

    private ConnectionContext createConnectionContext() {
        ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setClientId(SOURCE_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection() {
            @Override
            public void dispatchAsync(Command command) {
                dispatchSync(command);
            }

            @Override
            public void dispatchSync(Command command) {
                MessageDispatch messageDispatch = (MessageDispatch) (command.isMessageDispatch() ? command : null);
                if (messageDispatch != null && ReplicaSupport.isIntermediateReplicationQueue(messageDispatch.getDestination())) {
                    asyncSendWakeup();
                }
            }
        });
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        return connectionContext;
    }
}
