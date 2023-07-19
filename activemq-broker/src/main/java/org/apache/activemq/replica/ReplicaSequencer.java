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
import org.apache.activemq.broker.BrokerService;
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
import org.apache.activemq.replica.storage.ReplicaRecoverySequenceStorage;
import org.apache.activemq.replica.storage.ReplicaSequenceStorage;
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
    private static final String RESTORE_SEQUENCE_NAME = "primaryRestoreSeq";

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
    final LinkedList<String> sequenceMessageToAck = new LinkedList<>();
    private final ReplicaAckHelper replicaAckHelper;
    private final ReplicaPolicy replicaPolicy;
    private final ReplicaBatcher replicaBatcher;
    private final ReplicaStatistics replicaStatistics;

    ReplicaCompactor replicaCompactor;
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator customerIdGenerator = new LongSequenceGenerator();
    private TaskRunner ackTaskRunner;
    private TaskRunner sendTaskRunner;
    private Queue mainQueue;
    private ConnectionContext subscriptionConnectionContext;
    private ScheduledExecutorService scheduler;

    private PrefetchSubscription subscription;
    boolean hasConsumer;
    ReplicaSequenceStorage sequenceStorage;
    ReplicaRecoverySequenceStorage restoreSequenceStorage;

    BigInteger sequence = BigInteger.ZERO;

    private final AtomicLong lastProcessTime = new AtomicLong();

    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSequencer(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ReplicationMessageProducer replicationMessageProducer, ReplicaPolicy replicaPolicy, 
            ReplicaStatistics replicaStatistics) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaAckHelper = new ReplicaAckHelper(broker);
        this.replicaPolicy = replicaPolicy;
        this.replicaBatcher = new ReplicaBatcher(replicaPolicy);
        this.replicaStatistics = replicaStatistics;
    }

    void initialize() throws Exception {
        if (initialized.get()) {
            return;
        }

        BrokerService brokerService = broker.getBrokerService();
        TaskRunnerFactory taskRunnerFactory = brokerService.getTaskRunnerFactory();
        ackTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateAck, "ReplicationPlugin.Sequencer.Ack");
        sendTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateSend, "ReplicationPlugin.Sequencer.Send");

        Queue intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        mainQueue = broker.getDestinations(queueProvider.getMainQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        if (subscriptionConnectionContext == null) {
            subscriptionConnectionContext = createSubscriptionConnectionContext();
        }
        if (sequenceStorage == null) {
            sequenceStorage = new ReplicaSequenceStorage(broker, queueProvider, replicaInternalMessageProducer,
                    SEQUENCE_NAME);
        }
        if (restoreSequenceStorage == null) {
            restoreSequenceStorage = new ReplicaRecoverySequenceStorage(broker, queueProvider,
                    replicaInternalMessageProducer, RESTORE_SEQUENCE_NAME);
        }

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, customerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(subscriptionConnectionContext, consumerInfo);

        replicaCompactor = new ReplicaCompactor(broker, queueProvider, subscription,
                replicaPolicy.getCompactorAdditionalMessagesLimit(), replicaStatistics);

        intermediateQueue.iterate();
        String savedSequences = sequenceStorage.initialize(subscriptionConnectionContext);
        List<String> savedSequencesToRestore = restoreSequenceStorage.initialize(subscriptionConnectionContext);
        restoreSequence(savedSequences, savedSequencesToRestore);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::asyncSendWakeup,
                replicaPolicy.getSourceSendPeriod(), replicaPolicy.getSourceSendPeriod(), TimeUnit.MILLISECONDS);

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
                broker.removeConsumer(subscriptionConnectionContext, subscription.getConsumerInfo());
            } catch (BrokerStoppedException ignored) {}
            subscription = null;
        }

        replicaCompactor = null;

        if (sequenceStorage != null) {
            sequenceStorage.deinitialize(subscriptionConnectionContext);
        }
        if (restoreSequenceStorage != null) {
            restoreSequenceStorage.deinitialize(subscriptionConnectionContext);
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        initialized.compareAndSet(true, false);

    }

    void restoreSequence(String savedSequence, List<String> savedSequencesToRestore) throws Exception {
        if (savedSequence != null) {
            String[] split = savedSequence.split("#");
            if (split.length != 2) {
                throw new IllegalStateException("Unknown sequence message format: " + savedSequence);
            }
            sequence = new BigInteger(split[0]);
        }

        if (savedSequencesToRestore.isEmpty()) {
            return;
        }

        String lastMessage = savedSequencesToRestore.get(savedSequencesToRestore.size() - 1);
        String[] splitLast = lastMessage.split("#");
        if (splitLast.length != 3) {
            throw new IllegalStateException("Unknown sequence message format: " + lastMessage);
        }

        MessageId recoveryMessageId = new MessageId(splitLast[2]);
        List<MessageReference> matchingMessages = new ArrayList<>();
        boolean found = false;
        for (MessageReference mr : subscription.getDispatched()) {
            matchingMessages.add(mr);
            if (mr.getMessageId().equals(recoveryMessageId)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new IllegalStateException("Can't recover sequence. Message with id: " + recoveryMessageId + " not found");
        }

        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
        boolean rollbackOnFail = false;

        ConnectionContext connectionContext = createConnectionContext();
        BigInteger sequence = null;
        try {
            broker.beginTransaction(connectionContext, transactionId);
            rollbackOnFail = true;
            for (String seq : savedSequencesToRestore) {
                String[] split = seq.split("#");
                if (split.length != 3) {
                    throw new IllegalStateException("Unknown sequence message format: " + seq);
                }

                if (sequence != null && !sequence.equals(new BigInteger(split[0]))) {
                    throw new IllegalStateException("Sequence recovery error. Incorrect sequence. Expected sequence: " +
                            sequence + " saved sequence: " + seq);
                }

                List<MessageReference> batch = getBatch(matchingMessages, new MessageId(split[1]), new MessageId(split[2]));

                sequence = enqueueReplicaEvent(connectionContext, batch, new BigInteger(split[0]), transactionId);
            }

            broker.commitTransaction(connectionContext, transactionId, true);
        } catch (Exception e) {
            logger.error("Failed to persist messages in the main replication queue", e);
            if (rollbackOnFail) {
                try {
                    broker.rollbackTransaction(connectionContext, transactionId);
                } catch (Exception ex) {
                    logger.error("Could not rollback transaction", ex);
                }
            }
            throw e;
        }

        synchronized (deliveredMessages) {
            deliveredMessages.addAll(matchingMessages.stream().map(MessageReference::getMessageId).map(MessageId::toString).collect(Collectors.toList()));
        }
    }

    private List<MessageReference> getBatch(List<MessageReference> list, MessageId firstMessageId, MessageId lastMessageId) {
        List<MessageReference> result = new ArrayList<>();
        boolean inAckRange = false;
        for (MessageReference node : list) {
            MessageId messageId = node.getMessageId();
            if (firstMessageId.equals(messageId)) {
                inAckRange = true;
            }
            if (inAckRange) {
                result.add(node);
                if (lastMessageId.equals(messageId)) {
                    break;
                }
            }
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    List<MessageReference> acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        List<MessageReference> messagesToAck = replicaAckHelper.getMessagesToAck(ack, mainQueue);

        if (messagesToAck == null || messagesToAck.isEmpty()) {
            throw new IllegalStateException("Could not find messages for ack");
        }
        List<String> messageIds = new ArrayList<>();
        List<String> sequenceMessageIds = new ArrayList<>();
        long timestamp = messagesToAck.get(0).getMessage().getTimestamp();
        for (MessageReference reference : messagesToAck) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();
            List<String> messageIdsProperty;
            if (ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY)) == ReplicaEventType.BATCH) {
                messageIdsProperty = (List<String>) message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);
            } else {
                messageIdsProperty = List.of(message.getMessageId().toString());
            }
            messageIds.addAll(messageIdsProperty);
            sequenceMessageIds.add(messageIdsProperty.get(0));

            timestamp = Math.max(timestamp, message.getTimestamp());
        }

        broker.acknowledge(consumerExchange, ack);

        synchronized (messageToAck) {
            messageIds.forEach(messageToAck::addLast);
            sequenceMessageIds.forEach(sequenceMessageToAck::addLast);
        }

        long currentTime = System.currentTimeMillis();
        replicaStatistics.setTotalReplicationLag(currentTime - timestamp);
        replicaStatistics.setSourceLastProcessedTime(currentTime);

        asyncAckWakeup();

        return messagesToAck;
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
        List<String> messages;
        List<String> sequenceMessages;
        synchronized (messageToAck) {
            messages = new ArrayList<>(messageToAck);
            sequenceMessages = new ArrayList<>(sequenceMessageToAck);
        }

        if (!messages.isEmpty()) {
            ConnectionContext connectionContext = createConnectionContext();
            TransactionId transactionId = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
            boolean rollbackOnFail = false;
            try {
                broker.beginTransaction(connectionContext, transactionId);
                rollbackOnFail = true;

                ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
                consumerExchange.setConnectionContext(connectionContext);
                consumerExchange.setSubscription(subscription);

                for (String messageId : messages) {
                    MessageAck ack = new MessageAck();
                    ack.setTransactionId(transactionId);
                    ack.setMessageID(new MessageId(messageId));
                    ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
                    ack.setDestination(queueProvider.getIntermediateQueue());
                    broker.acknowledge(consumerExchange, ack);
                }

                restoreSequenceStorage.acknowledge(connectionContext, transactionId, sequenceMessages);

                broker.commitTransaction(connectionContext, transactionId, true);

                replicaStatistics.increaseTpsCounter(messages.size());

                synchronized (messageToAck) {
                    messageToAck.removeAll(messages);
                    sequenceMessageToAck.removeAll(sequenceMessages);
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

        synchronized (deliveredMessages) {
            Collections.reverse(dispatched);
            for (MessageReference reference : dispatched) {
                MessageId messageId = reference.getMessageId();
                if (deliveredMessages.contains(messageId.toString())) {
                    break;
                }
                toProcess.add(reference);
            }
        }

        if (toProcess.isEmpty() && hasConsumer) {
            return;
        }

        ConnectionContext connectionContext = createConnectionContext();

        Collections.reverse(toProcess);

        try {
            toProcess = replicaCompactor.compactAndFilter(connectionContext, toProcess, !hasConsumer && subscription.isFull());
        } catch (Exception e) {
            logger.error("Failed to compact messages in the intermediate replication queue", e);
            return;
        }
        if (!hasConsumer) {
            asyncSendWakeup();
            return;
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
            rollbackOnFail = true;

            BigInteger newSequence = sequence;
            for (List<MessageReference> batch : batches) {
                BigInteger newSequence1 = enqueueReplicaEvent(connectionContext, batch, newSequence, transactionId);

                restoreSequenceStorage.send(connectionContext, transactionId, newSequence + "#" +
                        batch.get(0).getMessageId() + "#" +
                        batch.get(batch.size() - 1).getMessageId(), batch.get(0).getMessageId());

                newSequence = newSequence1;
            }
            sequenceStorage.enqueue(connectionContext, transactionId, newSequence + "#" + toProcess.get(toProcess.size() - 1).getMessageId());

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
    }

    private BigInteger enqueueReplicaEvent(ConnectionContext connectionContext, List<MessageReference> batch,
            BigInteger sequence, TransactionId transactionId) throws Exception {
        if (batch.size() == 1) {
            MessageReference reference = batch.stream().findFirst()
                    .orElseThrow(() -> new IllegalStateException("Cannot get message reference from batch"));

            ActiveMQMessage originalMessage = (ActiveMQMessage) reference.getMessage();
            ActiveMQMessage message = (ActiveMQMessage) originalMessage.copy();

            message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, sequence.toString());
            message.setDestination(queueProvider.getMainQueue());
            message.setTransactionId(transactionId);
            message.setPersistent(false);
            replicaInternalMessageProducer.sendForcingFlowControl(connectionContext, message);
            sequence = sequence.add(BigInteger.ONE);
            return sequence;
        }

        List<String> messageIds = new ArrayList<>();
        List<DataStructure> messages = new ArrayList<>();
        long timestamp = batch.get(0).getMessage().getTimestamp();
        for (MessageReference reference : batch) {
            ActiveMQMessage originalMessage = (ActiveMQMessage) reference.getMessage();
            ActiveMQMessage message = (ActiveMQMessage) originalMessage.copy();

            message.setStringProperty(ReplicaSupport.SEQUENCE_PROPERTY, sequence.toString());

            message.setDestination(null);
            message.setTransactionId(null);
            message.setPersistent(false);

            messageIds.add(reference.getMessageId().toString());
            messages.add(message);

            sequence = sequence.add(BigInteger.ONE);

            // take timestamp from the newest message for statistics
            timestamp = Math.max(timestamp, message.getTimestamp());
        }

        ReplicaEvent replicaEvent = new ReplicaEvent()
                .setEventType(ReplicaEventType.BATCH)
                .setEventData(eventSerializer.serializeListOfObjects(messages))
                .setTransactionId(transactionId)
                .setTimestamp(timestamp)
                .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds);

        replicationMessageProducer.enqueueMainReplicaEvent(connectionContext, replicaEvent);

        return sequence;
    }

    private ConnectionContext createSubscriptionConnectionContext() {
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

    private ConnectionContext createConnectionContext() {
        ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        return connectionContext;
    }
}
