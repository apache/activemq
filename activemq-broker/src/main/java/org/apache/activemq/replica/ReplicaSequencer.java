package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
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
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaSequencer {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSequencer.class);

    private static final String SOURCE_CONSUMER_CLIENT_ID = "DUMMY_SOURCE_CONSUMER";
    public static final int ITERATE_SEND_PERIOD = 5_000;

    private final Broker broker;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final Object ackIteratingMutex = new Object();
    private final Object sendIteratingMutex = new Object();
    private final AtomicLong pendingAckWakeups = new AtomicLong();
    private final AtomicLong pendingSendWakeups = new AtomicLong();
    private final AtomicLong pendingSendTriggeredWakeups = new AtomicLong();
    final Set<String> deliveredMessages = new HashSet<>();
    final LinkedList<String> messageToAck = new LinkedList<>();
    private final ReplicaStorage replicaStorage;
    ReplicaCompactor replicaCompactor;
    private final ReplicaAckHelper replicaAckHelper;
    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator customerIdGenerator = new LongSequenceGenerator();
    private TaskRunner ackTaskRunner;
    private TaskRunner sendTaskRunner;
    private Queue mainQueue;
    private ConnectionContext connectionContext;

    private PrefetchSubscription subscription;
    boolean hasConsumer;

    BigInteger sequence = BigInteger.ZERO;
    MessageId recoveryMessageId;

    private final AtomicLong lastProcessTime = new AtomicLong();

    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSequencer(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicationMessageProducer replicationMessageProducer) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaStorage = new ReplicaStorage("source_sequence");
        this.replicaAckHelper = new ReplicaAckHelper(broker);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::asyncSendWakeup,
                ITERATE_SEND_PERIOD, ITERATE_SEND_PERIOD, TimeUnit.MILLISECONDS);
    }

    void initialize() throws Exception {
        TaskRunnerFactory taskRunnerFactory = broker.getBrokerService().getTaskRunnerFactory();
        ackTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateAck, "ReplicationPlugin.Sequencer.Ack");
        sendTaskRunner = taskRunnerFactory.createTaskRunner(this::iterateSend, "ReplicationPlugin.Sequencer.Send");

        Queue intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        mainQueue = broker.getDestinations(queueProvider.getMainQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setClientId(SOURCE_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection() {
            @Override
            public void dispatchAsync(Command command) {
                asyncSendWakeup();
            }

            @Override
            public void dispatchSync(Command message) {
                asyncSendWakeup();
            }
        });
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, sessionIdGenerator.getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, customerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10000);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        replicaCompactor = new ReplicaCompactor(broker, connectionContext, queueProvider, subscription);

        replicaStorage.initialize(new File(broker.getBrokerService().getBrokerDataDirectory(),
                ReplicaSupport.REPLICATION_PLUGIN_STORAGE_DIRECTORY));

        restoreSequence(intermediateQueue);

        initialized.compareAndSet(false, true);
        asyncSendWakeup();
    }

    void restoreSequence(Queue intermediateQueue) throws Exception {
        String line = replicaStorage.read();
        if (line == null) {
            return;
        }
        String[] split = line.split("#");
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
            if (l % ReplicaBatcher.MAX_BATCH_LENGTH == 0) {
                pendingSendTriggeredWakeups.incrementAndGet();
                sendTaskRunner.wakeup();
                pendingSendWakeups.addAndGet(-ReplicaBatcher.MAX_BATCH_LENGTH);
                return;
            }

            if (System.currentTimeMillis() - lastProcessTime.get() > ITERATE_SEND_PERIOD) {
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
            try {
                TransactionId transactionId = new LocalTransactionId(
                        new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        localTransactionIdGenerator.getNextSequenceId());
                ack.setTransactionId(transactionId);

                synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                    broker.beginTransaction(connectionContext, transactionId);

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
            batches = ReplicaBatcher.batches(toProcess);
        } catch (Exception e) {
            logger.error("Filed to batch messages in the intermediate replication queue", e);
            return;
        }

        MessageId lastProcessedMessageId = null;
        for (List<MessageReference> batch : batches) {
            try {
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
                        .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds);

                replicationMessageProducer.enqueueMainReplicaEvent(connectionContext, replicaEvent);

                synchronized (deliveredMessages) {
                    deliveredMessages.addAll(messageIds);
                }
                lastProcessedMessageId = batch.get(batch.size() - 1).getMessageId();
            } catch (Exception e) {
                sequence = sequence.subtract(BigInteger.valueOf(batch.size()));
                logger.error("Filed to persist message in the main replication queue", e);
                break;
            }
        }

        if (lastProcessedMessageId != null) {
            try {
                replicaStorage.write(sequence.toString() + "#" + lastProcessedMessageId);
            } catch (Exception e) {
                logger.error("Filed to write source sequence to disk", e);
            }
        }

        if (recoveryMessage != null) {
            recoveryMessageId = null;
        }
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
}
