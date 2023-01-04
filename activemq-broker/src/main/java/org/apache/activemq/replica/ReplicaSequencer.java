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
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SessionId;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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

    private TaskRunner taskRunner;
    private Queue intermediateQueue;
    private Queue mainQueue;

    private PrefetchSubscription subscription;
    private ConsumerId consumerId;

    BigInteger sequence = BigInteger.ZERO;

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

        ConnectionContext context = broker.getAdminConnectionContext().copy();
        context.setClientId(SOURCE_CONSUMER_CLIENT_ID);
        context.setConnection(new DummyConnection() {
            @Override
            public void dispatchAsync(Command command) {
                asyncWakeup();
            }

            @Override
            public void dispatchSync(Command message) {
                asyncWakeup();
            }
        });

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.Sequencer").generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10000);
        consumerInfo.setDestination(queueProvider.getIntermediateQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(context, consumerInfo);

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

        MessageId messageId = new MessageId(split[1]);
        int index = intermediateQueue.getAllMessageIds().indexOf(messageId);
        if (index == -1) {
            return;
        }

        sequence = sequence.subtract(BigInteger.valueOf(index + 1));
    }

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
                synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
                    subscription.acknowledge(broker.getAdminConnectionContext(), ack);
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

        if (!toProcess.isEmpty()) {
            Collections.reverse(toProcess);
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

                    replicationMessageProducer.enqueueMainReplicaEvent(broker.getAdminConnectionContext(),
                            replicaEvent);

                    synchronized (deliveredMessages) {
                        deliveredMessages.addAll(messageIds);
                    }
                    lastProcessedMessageId = batch.get(batch.size() - 1).getMessageId();
                } catch (Exception e) {
                    sequence = sequence.subtract(BigInteger.valueOf(batch.size()));
                    logger.error("Could not persist message in the main replication queue", e);
                    break;
                }
            }

            if (lastProcessedMessageId != null) {
                try {
                    replicaStorage.write(sequence.toString() + "#" + lastProcessedMessageId);
                } catch (Exception e) {
                    logger.error("Could not write source sequence to disk", e);
                }
            }
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
}
