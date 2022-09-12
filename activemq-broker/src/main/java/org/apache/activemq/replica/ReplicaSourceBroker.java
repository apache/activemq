package org.apache.activemq.replica;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.Connection;
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
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicaSourceBroker extends ReplicaSourceBaseBroker implements Task {

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {
    }; // used in destination map to indicate mirrored status
    static final String REPLICATION_CONNECTOR_NAME = "replication";
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);

    private final URI transportConnectorUri;

    final DestinationMap destinationsToReplicate = new DestinationMap();
    private final ReentrantReadWriteLock dropMessagesLock = new ReentrantReadWriteLock();
    final PendingList dropMessages = new OrderedPendingList();
    private final Object iteratingMutex = new Object();
    private final AtomicLong pendingWakeups = new AtomicLong();
    private TaskRunner taskRunner;

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(transportConnectorUri);
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);
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

    private void ensureDestinationsAreReplicated() throws Exception {
        for (ActiveMQDestination d : getDurableDestinations()) { // TODO: support non-durable?
            if (shouldReplicateDestination(d)) { // TODO: specific queues?
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) {
        if (destinationsToReplicate.get(destination) != null) {
            return;
        }

        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
            if (destinationsToReplicate.chooseValue(destination) == null) {
                destinationsToReplicate.put(destination, IS_REPLICATED);
            }
        } catch (Exception e) {
            logger.error("Failed to replicate creation of destination {}", destination.getPhysicalName(), e);
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

    private boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }

    private boolean isReplicationQueue(ActiveMQDestination destination) {
        return ReplicaSupport.REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
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
            final String jobId = (String) message.getProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
            if (isScheduled(message) || jobId != null) {
                return;
            }
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

    private boolean isScheduled(Message message) throws IOException {
        return hasProperty(message, ScheduledMessage.AMQ_SCHEDULED_DELAY)
                || hasProperty(message, ScheduledMessage.AMQ_SCHEDULED_CRON)
                || hasProperty(message, ScheduledMessage.AMQ_SCHEDULED_PERIOD);
    }

    private boolean hasProperty(Message message, String property) throws IOException {
        return message.getProperty(property) != null;
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
            destinationsToReplicate.remove(destination, IS_REPLICATED);
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
        }
    }

    @Override
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        return super.getDestinations(destination);
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        return super.messagePull(context, pull);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        assertAuthorized(context, consumerInfo.getDestination());

        Subscription subscription = super.addConsumer(context, consumerInfo);
        replicateAddConsumer(context, consumerInfo);
        return subscription;
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

    private boolean needToReplicateConsumer(ConsumerInfo consumerInfo) {
        return consumerInfo.getDestination().isTopic() &&
                consumerInfo.isDurable() &&
                !consumerInfo.isNetworkSubscription();
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        super.removeConsumer(context, consumerInfo);
        replicateRemoveConsumer(context, consumerInfo);
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

    private boolean isReplicationTransport(Connector connector) {
        return connector instanceof TransportConnector && ((TransportConnector) connector).getName().equals(REPLICATION_CONNECTOR_NAME);
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

    private boolean isSystemBroker(ConnectionContext context) {
        SecurityContext securityContext = context.getSecurityContext();
        return securityContext != null && securityContext.isBrokerContext();
    }

    private String createUnauthorizedMessage(ActiveMQDestination destination) {
        return "Not authorized to access destination: " + destination;
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        super.commitTransaction(context, xid, onePhase);
        replicateCommitTransaction(context, xid, onePhase);
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        super.removeSubscription(context, info); // TODO: durable subscribers?
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        return super.getPreparedTransactions(context);
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int id = super.prepareTransaction(context, xid);
        replicatePrepareTransaction(context, xid);
        return id;
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
        replicateRollbackTransaction(context, xid);
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
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
        replicateBeginTransaction(context, xid);
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
        replicateForgetTransaction(context, transactionId);
    }

    @Override
    public Connection[] getClients() throws Exception {
        return super.getClients();
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
    public ActiveMQDestination[] getDestinations() throws Exception {
        return super.getDestinations();
    }

    @Override
    public BrokerInfo[] getPeerBrokerInfos() {
        return super.getPeerBrokerInfos();
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        super.processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return super.getDurableDestinations();
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        super.addDestinationInfo(context, info);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        super.removeDestinationInfo(context, info);
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription,
                                         Throwable poisonCause) {
        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
    }

    @Override
    public void messageDelivered(ConnectionContext context, MessageReference messageReference) {
        super.messageDelivered(context, messageReference);
    }

    @Override
    public void virtualDestinationAdded(ConnectionContext context, VirtualDestination virtualDestination) {
        super.virtualDestinationAdded(context, virtualDestination);
    }

    @Override
    public void virtualDestinationRemoved(ConnectionContext context, VirtualDestination virtualDestination) {
        super.virtualDestinationRemoved(context, virtualDestination);
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
}
