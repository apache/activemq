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

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.SecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaSourceBroker extends ReplicaSourceBaseBroker {

    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {
    }; // used in destination map to indicate mirrored status
    static final String REPLICATION_CONNECTOR_NAME = "replication";
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);

    private final ReplicaSequencer replicaSequencer;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private ReplicaPolicy replicaPolicy;

    final DestinationMap destinationsToReplicate = new DestinationMap();

    public ReplicaSourceBroker(Broker next, ReplicationMessageProducer replicationMessageProducer,
            ReplicaSequencer replicaSequencer, ReplicaReplicationQueueSupplier queueProvider, ReplicaPolicy replicaPolicy) {
        super(next, replicationMessageProducer);
        this.replicaSequencer = replicaSequencer;
        this.queueProvider = queueProvider;
        this.replicaPolicy = replicaPolicy;
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(replicaPolicy.getTransportConnectorUri());
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);
        queueProvider.initialize();
        queueProvider.initializeSequenceQueue();
        super.start();
        replicaSequencer.initialize();
        ensureDestinationsAreReplicated();
    }

    private void ensureDestinationsAreReplicated() {
        for (ActiveMQDestination d : getDurableDestinations()) { // TODO: support non-durable?
            if (shouldReplicateDestination(d)) { // TODO: specific queues?
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) != null) {
            return;
        }

        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
            destinationsToReplicate.put(destination, IS_REPLICATED);
        } catch (Exception e) {
            logger.error("Failed to replicate creation of destination {}", destination.getPhysicalName(), e);
        }
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = ReplicaSupport.isReplicationQueue(destination);
        boolean isAdvisoryDestination = isAdvisoryDestination(destination);
        boolean isTemporaryDestination = destination.isTemporary();
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination && !isTemporaryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        if (isTemporaryDestination) reason += "it is a temporary destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "" : "not ", destination, reason);
        return shouldReplicate;
    }

    private boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
    }

    private void replicateSend(ConnectionContext context, Message message, TransactionId transactionId) {
        try {
            TransactionId originalTransactionId = message.getTransactionId();
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
                            .setTransactionId(transactionId)
                            .setReplicationProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, message.getMessageId().toString())
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY,
                                    message.getDestination().isQueue())
                            .setReplicationProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY,
                                    message.getDestination().toString())
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY,
                                    originalTransactionId != null && originalTransactionId.isXATransaction())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate message {} for destination {}", message.getMessageId(), message.getDestination().getPhysicalName(), e);
        }
    }

    private boolean needToReplicateSend(ConnectionContext connectionContext, Message message) {
        if (isReplicaContext(connectionContext)) {
            return false;
        }
        if (ReplicaSupport.isReplicationQueue(message.getDestination())) {
            return false;
        }
        if (message.getDestination().isTemporary()) {
            return false;
        }
        if (message.isAdvisory()) {  // TODO: only replicate what we care about
            return false;
        }

        try {
            String jobId = (String) message.getProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
            if (isScheduled(message) || jobId != null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("Failed to get jobId", e);
        }

        return true;
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
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
        assertAuthorized(context, consumerInfo.getDestination());

        Subscription subscription = super.addConsumer(context, consumerInfo);
        replicateAddConsumer(context, consumerInfo);

        if (ReplicaSupport.isMainReplicationQueue(consumerInfo.getDestination())) {
            replicaSequencer.updateMainQueueConsumerStatus();
        }

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
        if (ReplicaSupport.isMainReplicationQueue(consumerInfo.getDestination())) {
            replicaSequencer.updateMainQueueConsumerStatus();
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
        boolean replicationQueue = ReplicaSupport.isReplicationQueue(destination);
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
        if (xid.isXATransaction()) {
            replicateCommitTransaction(context, xid, onePhase);
        }
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        int id = super.prepareTransaction(context, xid);
        if (xid.isXATransaction()) {
            replicatePrepareTransaction(context, xid);
        }
        return id;
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.rollbackTransaction(context, xid);
        if (xid.isXATransaction()) {
            replicateRollbackTransaction(context, xid);
        }
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext connectionContext = producerExchange.getConnectionContext();
        if (!needToReplicateSend(connectionContext, messageSend)) {
            super.send(producerExchange, messageSend);
            return;
        }

        boolean isInternalTransaction = false;
        TransactionId transactionId = null;
        if (messageSend.getTransactionId() != null && !messageSend.getTransactionId().isXATransaction()) {
            transactionId = messageSend.getTransactionId();
        } else if (messageSend.getTransactionId() == null) {
            transactionId = new LocalTransactionId(new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
            if (connectionContext.getTransactions() == null) {
                connectionContext.setTransactions(new ConcurrentHashMap<>());
            }
            super.beginTransaction(connectionContext, transactionId);
            messageSend.setTransactionId(transactionId);
            isInternalTransaction = true;
        }
        try {
            super.send(producerExchange, messageSend);
            replicateSend(connectionContext, messageSend, transactionId);
            if (isInternalTransaction) {
                super.commitTransaction(connectionContext, transactionId, true);
            }
        } catch (Exception e) {
            if (isInternalTransaction) {
                super.rollbackTransaction(connectionContext, transactionId);
            }
            throw e;
        }
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        super.beginTransaction(context, xid);
        if (xid.isXATransaction()) {
            replicateBeginTransaction(context, xid);
        }
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        super.forgetTransaction(context, transactionId);
        if (transactionId.isXATransaction()) {
            replicateForgetTransaction(context, transactionId);
        }
    }

    private boolean needToReplicateAck(ConnectionContext connectionContext, MessageAck ack, PrefetchSubscription subscription) {
        if (isReplicaContext(connectionContext)) {
            return false;
        }
        if (ReplicaSupport.isReplicationQueue(ack.getDestination())) {
            return false;
        }
        if (ack.getDestination().isTemporary()) {
            return false;
        }
        if (!ack.isStandardAck() && !ack.isIndividualAck()) {
            return false;
        }
        if (subscription instanceof QueueBrowserSubscription && !connectionContext.isNetworkConnection()) {
            return false;
        }

        return true;
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
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (ack.isDeliveredAck() || ack.isUnmatchedAck()) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        if (ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME.equals(ack.getDestination().getPhysicalName())) {
            replicaSequencer.acknowledge(consumerExchange, ack);
            return;
        }

        ConnectionContext connectionContext = consumerExchange.getConnectionContext();

        PrefetchSubscription subscription = getDestinations(ack.getDestination()).stream().findFirst()
                .map(Destination::getConsumers).stream().flatMap(Collection::stream)
                .filter(c -> c.getConsumerInfo().getConsumerId().equals(ack.getConsumerId()))
                .findFirst().filter(PrefetchSubscription.class::isInstance).map(PrefetchSubscription.class::cast)
                .orElse(null);
        if (subscription == null) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        if (!needToReplicateAck(connectionContext, ack, subscription)) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        List<String> messageIdsToAck = getMessageIdsToAck(ack, subscription);
        if (messageIdsToAck == null) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        boolean isInternalTransaction = false;
        TransactionId transactionId = null;
        if (ack.getTransactionId() != null && !ack.getTransactionId().isXATransaction()) {
            transactionId = ack.getTransactionId();
        } else if (ack.getTransactionId() == null) {
            transactionId = new LocalTransactionId(new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
            super.beginTransaction(connectionContext, transactionId);
            ack.setTransactionId(transactionId);
            isInternalTransaction = true;
        }
        try {
            super.acknowledge(consumerExchange, ack);
            replicateAck(connectionContext, ack, transactionId, messageIdsToAck);
            if (isInternalTransaction) {
                super.commitTransaction(connectionContext, transactionId, true);
            }
        } catch (Exception e) {
            if (isInternalTransaction) {
                super.rollbackTransaction(connectionContext, transactionId);
            }
            throw e;
        }
    }

    private List<String> getMessageIdsToAck(MessageAck ack, PrefetchSubscription subscription) {
        if (ack.isStandardAck() || ack.isExpiredAck() || ack.isPoisonAck()) {
            boolean inAckRange = false;
            List<String> removeList = new ArrayList<>();
            for (final MessageReference node : subscription.getDispatched()) {
                MessageId messageId = node.getMessageId();
                if (ack.getFirstMessageId() == null || ack.getFirstMessageId().equals(messageId)) {
                    inAckRange = true;
                }
                if (inAckRange) {
                    removeList.add(messageId.toString());
                    if (ack.getLastMessageId().equals(messageId)) {
                        break;
                    }
                }
            }

            return removeList;
        }

        if (ack.isIndividualAck()) {
            return List.of(ack.getLastMessageId().toString());
        }

        return null;
    }

    private void replicateAck(ConnectionContext connectionContext, MessageAck ack, TransactionId transactionId,
            List<String> messageIdsToAck) {
        try {
            TransactionId originalTransactionId = ack.getTransactionId();
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_ACK)
                            .setEventData(eventSerializer.serializeReplicationData(ack))
                            .setTransactionId(transactionId)
                            .setReplicationProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY, messageIdsToAck)
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_SENT_TO_QUEUE_PROPERTY,
                                    ack.getDestination().isQueue())
                            .setReplicationProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY,
                                    ack.getDestination().toString())
                            .setReplicationProperty(ReplicaSupport.IS_ORIGINAL_MESSAGE_IN_XA_TRANSACTION_PROPERTY,
                                    originalTransactionId != null && originalTransactionId.isXATransaction())
            );
        } catch (Exception e) {
            logger.error("Failed to replicate ack messages [{} <-> {}] for consumer {}",
                    ack.getFirstMessageId(),
                    ack.getLastMessageId(),
                    ack.getConsumerId(), e);
        }
    }

    @Override
    public void queuePurged(ConnectionContext context, ActiveMQDestination destination) {
        super.queuePurged(context, destination);
        replicateQueuePurged(context, destination);

    }

    private void replicateQueuePurged(ConnectionContext connectionContext, ActiveMQDestination destination) {
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.QUEUE_PURGED)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate queue purge {}", destination, e);
        }
    }
}
