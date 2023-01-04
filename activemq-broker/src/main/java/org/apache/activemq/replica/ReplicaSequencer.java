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
import org.apache.activemq.thread.Task;
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaSequencer implements Task {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSequencer.class);

    private static final String SOURCE_CONSUMER_CLIENT_ID = "DUMMY_SOURCE_CONSUMER";
    static final int MAX_BATCH_LENGTH = 500;
    static final int MAX_BATCH_SIZE = 5_000_000; // 5 Mb

    private final Broker broker;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final Object iteratingMutex = new Object();
    private final AtomicLong pendingWakeups = new AtomicLong();
    final Set<String> deliveredMessages = new HashSet<>();
    final LinkedList<String> messageToAck = new LinkedList<>();
    private final ReplicaStorage replicaStorage;

    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();
    private TaskRunner taskRunner;
    private Queue intermediateQueue;
    private Queue mainQueue;
    private ConnectionContext connectionContext;

    private PrefetchSubscription subscription;
    private ConsumerId consumerId;
    private boolean hasConsumer;

    BigInteger sequence = BigInteger.ZERO;
    MessageId recoveryMessageId;


    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSequencer(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicationMessageProducer replicationMessageProducer) {
        this.broker = broker;
        this.queueProvider = queueProvider;
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaStorage = new ReplicaStorage("source_sequence");
    }

    void initialize() throws Exception {
        TaskRunnerFactory taskRunnerFactory = broker.getBrokerService().getTaskRunnerFactory();
        taskRunner = taskRunnerFactory.createTaskRunner(this, "ReplicationPlugin.Sequencer");

        intermediateQueue = broker.getDestinations(queueProvider.getIntermediateQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        mainQueue = broker.getDestinations(queueProvider.getMainQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setClientId(SOURCE_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection() {
            @Override
            public void dispatchAsync(Command command) {
                asyncWakeup();
            }

            @Override
            public void dispatchSync(Command message) {
                asyncWakeup();
            }
        });
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10000);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        replicaStorage.initialize(new File(broker.getBrokerService().getBrokerDataDirectory(),
                ReplicaSupport.REPLICATION_PLUGIN_STORAGE_DIRECTORY));

        restoreSequence();

        initialized.compareAndSet(false, true);
        asyncWakeup();
    }

    void restoreSequence() throws Exception {
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
        PrefetchSubscription subscription = mainQueue.getConsumers().stream()
                .filter(c -> c.getConsumerInfo().getConsumerId().equals(ack.getConsumerId()))
                .filter(PrefetchSubscription.class::isInstance)
                .map(PrefetchSubscription.class::cast)
                .findFirst().orElseThrow();
        MessageReference reference = subscription.getDispatched().stream()
                .filter(mr -> mr.getMessageId().equals(ack.getLastMessageId()))
                .findFirst().orElseThrow();

        List<String> messageIds = (List<String>) reference.getMessage().getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);

        broker.acknowledge(consumerExchange, ack);
        synchronized (messageToAck) {
            messageIds.forEach(messageToAck::addLast);
        }
        asyncWakeup();
    }

    void asyncWakeup() {
        try {
            pendingWakeups.incrementAndGet();
            taskRunner.wakeup();
        } catch (InterruptedException e) {
            logger.warn("Async task runner failed to wakeup ", e);
        }
    }

    @Override
    public boolean iterate() {
        synchronized (iteratingMutex) {
            if (!initialized.get()) {
                return false;
            }

            iterateAck();
            iterateSend();

            if (pendingWakeups.get() > 0) {
                pendingWakeups.decrementAndGet();
            }
        }

        return pendingWakeups.get() > 0;
    }

    void iterateAck() {
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

                asyncWakeup();
            } catch (Exception e) {
                logger.error("Could not acknowledge replication messages", e);
            }
        }
    }

    void iterateSend() {
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

        if (toProcess.isEmpty()) {
            return;
        }

        Collections.reverse(toProcess);

        if (recoveryMessage != null) {
            toProcess = toProcess.subList(0, toProcess.indexOf(recoveryMessage) + 1);
        }

        if (recoveryMessageId == null) {
            try {
                toProcess = compactAndFilter(toProcess);
            } catch (Exception e) {
                logger.error("Filed to compact messages in the intermediate replication queue", e);
                return;
            }
        }

        if (!hasConsumer) {
            return;
        }
        List<List<MessageReference>> batches = batches(toProcess);

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
        if (!hasConsumer && !mainQueue.getConsumers().isEmpty()) {
            hasConsumer = !mainQueue.getConsumers().isEmpty();
            asyncWakeup();
        } else {
            hasConsumer = !mainQueue.getConsumers().isEmpty();
        }
    }

    List<List<MessageReference>> batches(List<MessageReference> list) {
        List<List<MessageReference>> result = new ArrayList<>();

        List<MessageReference> batch = new ArrayList<>();
        int batchSize = 0;
        for (MessageReference reference : list) {
            if (batch.size() > 0
                    && (batch.size() + 1 > MAX_BATCH_LENGTH || batchSize + reference.getSize() > MAX_BATCH_SIZE)) {
                result.add(batch);
                batch = new ArrayList<>();
                batchSize = 0;
            }

            batch.add(reference);
            batchSize += reference.getSize();
        }
        if (batch.size() > 0) {
            result.add(batch);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    List<MessageReference> compactAndFilter(List<MessageReference> list) throws Exception {
        List<MessageReference> result = new ArrayList<>(list);
        Map<String, MessageId> sendMap = new LinkedHashMap<>();
        Map<List<String>, ActiveMQMessage> ackMap = new LinkedHashMap<>();
        for (MessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();

            if (!message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY)
                    || message.getBooleanProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY)) {
                continue;
            }

            ReplicaEventType eventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
            MessageId messageId = reference.getMessageId();
            if (eventType == ReplicaEventType.MESSAGE_SEND) {
                sendMap.put(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY), messageId);
            }
            if (eventType == ReplicaEventType.MESSAGE_ACK) {
                List<String> messageIds = (List<String>)
                        Optional.ofNullable(message.getProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY))
                                .orElse(message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));

                ackMap.put(messageIds, message);
            }
        }

        List<MessageId> toDelete = new ArrayList<>();

        for (Map.Entry<List<String>, ActiveMQMessage> ack : ackMap.entrySet()) {
            List<String> sends = new ArrayList<>();
            List<String> messagesToAck = ack.getKey();
            for (String id : messagesToAck) {
                if (sendMap.containsKey(id)) {
                    sends.add(id);
                    toDelete.add(sendMap.get(id));
                }
            }
            if (sends.size() == 0) {
                continue;
            }

            ActiveMQMessage message = ack.getValue();
            if (messagesToAck.size() == sends.size() && new HashSet<>(messagesToAck).containsAll(sends)) {
                toDelete.add(message.getMessageId());
                continue;
            }

            message.setProperty(ReplicaSupport.ORIGINAL_MESSAGE_IDS_PROPERTY, messagesToAck);
            ArrayList<String> newList = new ArrayList<>(messagesToAck);
            newList.removeAll(sends);
            message.setProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, newList);

            synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                intermediateQueue.getMessageStore().updateMessage(message);
            }
        }

        if (toDelete.isEmpty()) {
            return result;
        }

        TransactionId transactionId = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                localTransactionIdGenerator.getNextSequenceId());

        synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
            broker.beginTransaction(connectionContext, transactionId);

            ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(connectionContext);
            consumerExchange.setSubscription(subscription);

            for (MessageId id : toDelete) {
                MessageAck ack = new MessageAck();
                ack.setMessageID(id);
                ack.setMessageCount(1);
                ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
                ack.setDestination(queueProvider.getIntermediateQueue());
                broker.acknowledge(consumerExchange, ack);
            }

            broker.commitTransaction(connectionContext, transactionId, true);
        }

        result.removeIf(reference -> toDelete.contains(reference.getMessageId()));

        return result;
    }
}
