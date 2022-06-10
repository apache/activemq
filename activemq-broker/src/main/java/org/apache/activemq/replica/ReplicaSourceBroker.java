package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.cursors.OrderedPendingList;
import org.apache.activemq.broker.region.cursors.PendingList;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaSourceBroker extends BrokerFilter implements Task {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {}; // used in destination map to indicate mirrored status
    static final String REPLICATION_CONNECTOR_NAME = "replication";

    final DestinationMap destinationsToReplicate = new DestinationMap();

    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    final ReplicaReplicationQueueSupplier queueProvider;
    private final URI transportConnectorUri;
    private ReplicaInternalMessageProducer replicaInternalMessageProducer;

    private final ReentrantReadWriteLock dropMessagesLock = new ReentrantReadWriteLock();
    final PendingList dropMessages = new OrderedPendingList();
    private final Object iteratingMutex = new Object();
    private final Object sendingMutex = new Object();
    private final AtomicLong pendingWakeups = new AtomicLong();
    private TaskRunner taskRunner;

    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
        replicationProducerId.setConnectionId(idGenerator.generateId());
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(transportConnectorUri);
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);

        queueProvider.initialize();
        logger.info("Replica plugin initialized with queue {}", queueProvider.get());
        initialized.compareAndSet(false, true);

        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(next, getAdminConnectionContext());

        taskRunner = getBrokerService().getTaskRunnerFactory().createTaskRunner(this, "ReplicationPlugin.dropMessages");

        super.start();

        ensureDestinationsAreReplicated();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        if (taskRunner != null) {
            taskRunner.shutdown();
        }
    }

    private void ensureDestinationsAreReplicated() throws Exception { // TODO: probably not needed
        for (ActiveMQDestination d : getDurableDestinations()) { // TODO: support non-durable?
            if (shouldReplicateDestination(d)) { // TODO: specific queues?
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary)
            throws Exception {
        Destination newDestination = super.addDestination(context, destination, createIfTemporary);
        if (shouldReplicateDestination(destination)) {
            replicateDestinationCreation(context, destination);
        }
        return newDestination;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        replicateDestinationRemoval(context, destination);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ActiveMQDestination destination = messageSend.getDestination();
        replicateSend(producerExchange, messageSend, destination);
        try {
            super.send(producerExchange, messageSend);
        } catch (Exception e) {
            if (destination.isQueue()) {
                queueMessageDropped(producerExchange.getConnectionContext(), new IndirectMessageReference(messageSend));
            }
            if (destination.isTopic()) {
                // TODO have correct handling of durable subscribers if there is such a situation
            }
            throw e;
        }
    }
    @Override
    public void queueMessageDropped(ConnectionContext context, QueueMessageReference reference) {
        if (isReplicaContext(context)) {
            return;
        }
        Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }

        dropMessagesLock.writeLock().lock();
        try {
            dropMessages.addMessageLast(reference);
        } finally {
            dropMessagesLock.writeLock().unlock();
        }
        asyncWakeup();
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
        replicateBeginTransaction(context, xid);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int id = super.prepareTransaction(context, xid);
        replicatePrepareTransaction(context, xid);
        return id;
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
        replicateForgetTransaction(context, transactionId);
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
        replicateRollbackTransaction(context, xid);
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        super.commitTransaction(context, xid, onePhase);
        replicateCommitTransaction(context, xid, onePhase);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        assertAuthorized(context, consumerInfo.getDestination());

        Subscription subscription = super.addConsumer(context, consumerInfo);
        replicateAddConsumer(context, consumerInfo);
        return subscription;
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        super.removeConsumer(context, consumerInfo);
        replicateRemoveConsumer(context, consumerInfo);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo producerInfo) throws Exception {
        // JMS allows producers to be created without first specifying a destination.  In these cases, every send
        // operation must specify a destination.  Because of this, we only authorize 'addProducer' if a destination is
        // specified. If not specified, the authz check in the 'send' method below will ensure authorization.
        if (producerInfo.getDestination() != null) {
            assertAuthorized(context, producerInfo.getDestination());
        }
        super.addProducer(context, producerInfo);
    }

    @Override
    public void topicMessageAcknowledged(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TOPIC_MESSAGE_ACK)
                            .setEventData(eventSerializer.serializeReplicationData(node.getMessage()))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, sub.getConsumerInfo().getClientId())
                            .setReplicationProperty(ReplicaSupport.ACK_TYPE_PROPERTY, ack.getAckType())
            );
        } catch (Exception e) {
            logger.error(
                    "Failed to replicate ACK {} for consumer {}",
                    node.getMessageId(),
                    sub.getConsumerInfo()
            );
        }
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = isReplicationQueue(destination);
        boolean isAdvisoryDestination = isAdvisoryDestination(destination);
        boolean isTemporaryDestination = destination.isTemporary();
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination && !isTemporaryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        if (isTemporaryDestination) reason += "it is a temporary destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "": "not ", destination, reason);
        return shouldReplicate;
    }

    private boolean isReplicationQueue(ActiveMQDestination destination) {
        return ReplicaSupport.REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    private boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }

    private boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                        .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                        .setEventData(eventSerializer.serializeReplicationData(destination))
        );
        if (destinationsToReplicate.chooseValue(destination) == null) {
            destinationsToReplicate.put(destination, IS_REPLICATED);
        }
    }

    private void replicateDestinationRemoval(ConnectionContext context, ActiveMQDestination destination) {
        if (!isReplicatedDestination(destination)) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_DELETE)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
        }
    }

    private void replicateSend(ProducerBrokerExchange context, Message message, ActiveMQDestination destination) {
        if (isReplicationQueue(message.getDestination())) {
            return;
        }
        if (destination.isTemporary()) {
            return;
        }
        if (message.isAdvisory()) {  // TODO: only replicate what we care about
            return;
        }

        try {
            enqueueReplicaEvent(
                    context.getConnectionContext(),
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate message {} for destination {}", message.getMessageId(), destination.getPhysicalName());
        }
    }


    private void replicateBeginTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate begin of transaction [{}]", xid);
        }
    }

    private void replicatePrepareTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction prepare [{}]", xid);
        }
    }

    private void replicateForgetTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction forget [{}]", xid);
        }
    }

    private void replicateRollbackTransaction(ConnectionContext context, TransactionId xid) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction rollback [{}]", xid);
        }
    }

    private void replicateCommitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_COMMIT)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
                            .setReplicationProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY, onePhase)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate commit of transaction [{}]", xid);
        }
    }

    private void replicateAddConsumer(ConnectionContext context, ConsumerInfo consumerInfo) {
        if (!needToReplicateConsumer(consumerInfo)) {
            return;
        }
        if (isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.ADD_DURABLE_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
                            .setReplicationProperty(ReplicaSupport.CLIENT_ID_PROPERTY, context.getClientId())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
        }
    }

    private void replicateRemoveConsumer(ConnectionContext context, ConsumerInfo consumerInfo) {
        if (!needToReplicateConsumer(consumerInfo)) {
            return;
        }
        if (isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.REMOVE_DURABLE_CONSUMER)
                            .setEventData(eventSerializer.serializeReplicationData(consumerInfo))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate adding {}", consumerInfo, e);
        }
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
    }

    private void enqueueReplicaEvent(ConnectionContext initialContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(initialContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }

        synchronized (sendingMutex) {
            logger.debug("Replicating {} event", event.getEventType());
            logger.trace("Replicating {} event: data:\n{}", event.getEventType(), new Object() {
                @Override
                public String toString() {
                    try {
                        return eventSerializer.deserializeMessageData(event.getEventData()).toString();
                    } catch (IOException e) {
                        return "<some event data>";
                    }
                }
            }); // FIXME: remove
            ActiveMQMessage eventMessage = new ActiveMQMessage();
            eventMessage.setPersistent(true);
            eventMessage.setType("ReplicaEvent");
            eventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
            eventMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
            eventMessage.setDestination(queueProvider.get());
            eventMessage.setProducerId(replicationProducerId);
            eventMessage.setResponseRequired(false);
            eventMessage.setContent(event.getEventData());
            eventMessage.setProperties(event.getReplicationProperties());
            replicaInternalMessageProducer.produceToReplicaQueue(eventMessage);
        }
    }

    private void asyncWakeup() {
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
            PendingList messages = new OrderedPendingList();
            dropMessagesLock.readLock().lock();
            try {
                messages.addAll(dropMessages);
            } finally {
                dropMessagesLock.readLock().unlock();
            }

            if (!messages.isEmpty()) {
                Map<ActiveMQDestination, Set<String>> map = new HashMap<>();
                for (MessageReference message : messages) {
                    Set<String> messageIds = map.computeIfAbsent(message.getMessage().getDestination(), k -> new HashSet<>());
                    messageIds.add(message.getMessageId().toString());

                    dropMessagesLock.writeLock().lock();
                    try {
                        dropMessages.remove(message);
                    } finally {
                        dropMessagesLock.writeLock().unlock();
                    }
                }

                for (Map.Entry<ActiveMQDestination, Set<String>> entry : map.entrySet()) {
                    replicateDropMessages(entry.getKey(), new ArrayList<>(entry.getValue()));
                }
            }

            if (pendingWakeups.get() > 0) {
                pendingWakeups.decrementAndGet();
            }
            return pendingWakeups.get() > 0;
        }
    }

    private void replicateDropMessages(ActiveMQDestination destination, List<String> messageIds) {
        try {
            enqueueReplicaEvent(
                    null,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGES_DROPPED)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
                            .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIds)
            );
        } catch (Exception e) {
            logger.error("Failed to replicate drop messages {} - {}", destination, messageIds, e);
        }
    }

    protected void assertAuthorized(ConnectionContext context, ActiveMQDestination destination) {
        boolean replicationQueue = isReplicationQueue(destination);
        boolean replicationTransport = isReplicationTransport(context.getConnector());

        if (isSystemBroker(context)) {
            return;
        }
        if (replicationTransport && (replicationQueue || isAdvisoryDestination(destination))) {
            return;
        }
        if (!replicationTransport && !replicationQueue) {
            return;
        }

        String msg = createUnauthorizedMessage(destination);
        throw new ActiveMQReplicaException(msg);
    }

    private boolean isReplicationTransport(Connector connector) {
        return connector instanceof TransportConnector && ((TransportConnector) connector).getName().equals(REPLICATION_CONNECTOR_NAME);
    }

    private boolean isSystemBroker(ConnectionContext context) {
        SecurityContext securityContext = context.getSecurityContext();
        return securityContext != null && securityContext.isBrokerContext();
    }

    private String createUnauthorizedMessage(ActiveMQDestination destination) {
        return "Not authorized to access destination: " + destination;
    }

    private boolean needToReplicateConsumer(ConsumerInfo consumerInfo) {
        return consumerInfo.getDestination().isTopic() &&
                consumerInfo.isDurable() &&
                !consumerInfo.isNetworkSubscription();
    }
}
