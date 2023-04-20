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
import org.apache.activemq.Service;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.replica.storage.ReplicaFailOverStateStorage;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.activemq.replica.ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME;

public class ReplicaSourceBroker extends BrokerFilter implements MutativeRoleBroker {
    private static final String FAIL_OVER_CONSUMER_CLIENT_ID = "DUMMY_FAIL_OVER_CONSUMER";
    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {
    }; // used in destination map to indicate mirrored status

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean initialized = new AtomicBoolean();

    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaSequencer replicaSequencer;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaPolicy replicaPolicy;
    private final ReplicaAckHelper replicaAckHelper;
    private final ReplicaFailOverStateStorage replicaFailOverStateStorage;
    private final WebConsoleAccessController webConsoleAccessController;

    final DestinationMap destinationsToReplicate = new DestinationMap();

    private ActionListenerCallback actionListenerCallback;
    private ConnectionContext connectionContext;

    public ReplicaSourceBroker(Broker next, ReplicationMessageProducer replicationMessageProducer,
            ReplicaSequencer replicaSequencer, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaPolicy replicaPolicy, ReplicaFailOverStateStorage replicaFailOverStateStorage,
            WebConsoleAccessController webConsoleAccessController) {
        super(next);
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaSequencer = replicaSequencer;
        this.queueProvider = queueProvider;
        this.replicaPolicy = replicaPolicy;
        this.replicaAckHelper = new ReplicaAckHelper(next);
        this.replicaFailOverStateStorage = replicaFailOverStateStorage;
        this.webConsoleAccessController = webConsoleAccessController;
    }

    @Override
    public void start() throws Exception {
        logger.info("Starting Source broker");
        installTransportConnector();
        initQueueProvider();
        initialized.compareAndSet(false, true);
        replicaSequencer.initialize();
        ensureDestinationsAreReplicated();
        initializeContext();
    }

    @Override
    public void stop() throws Exception {
        replicaSequencer.deinitialize();
        super.stop();
        initialized.compareAndSet(true, false);
    }

    @Override
    public void initializeRoleChangeCallBack(ActionListenerCallback actionListenerCallback) {
        this.actionListenerCallback = actionListenerCallback;
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        logger.info("Stopping Source broker. Forced [{}]", force);
        if (force) {
            stopBeforeForcedRoleChange();
        } else {
            stopBeforeRoleChange();
        }
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        logger.info("Starting Source broker after role change");
        installTransportConnector();
        getBrokerService().startAllConnectors();
        webConsoleAccessController.start();

        initQueueProvider();
        initialized.compareAndSet(false, true);
        replicaSequencer.initialize();
        ensureDestinationsAreReplicated();
        replicaSequencer.updateMainQueueConsumerStatus();
        replicaSequencer.scheduleExecutor();
        initializeContext();
    }


    private void stopBeforeForcedRoleChange() throws Exception {
        getBrokerService().stopAllConnectors(new ServiceStopper());
        replicaSequencer.deinitialize();
        replicaSequencer.terminateScheduledExecutor();
        removeReplicationQueues();
    }

    private void stopBeforeRoleChange() throws Exception {
        getBrokerService().stopAllConnectors(new ServiceStopper() {
            @Override
            public void stop(Service service) {
                if (service instanceof TransportConnector &&
                        ((TransportConnector) service).getName().equals(ReplicaSupport.REPLICATION_CONNECTOR_NAME)) {
                    return;
                }
                super.stop(service);
            }
        });
        webConsoleAccessController.stop();

        sendFailOverMessage();
    }

    private void initializeContext() {
        connectionContext = next.getAdminConnectionContext().copy();
        connectionContext.setClientId(FAIL_OVER_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }
    }

    private void completeDeinitialization() {
        logger.info("completing source broker deinitialization");
        try {
            getBrokerService().getTransportConnectors().stream()
                    .filter(transportConnector -> transportConnector.getName().equals(ReplicaSupport.REPLICATION_CONNECTOR_NAME))
                    .forEach(transportConnector -> {
                        try {
                            transportConnector.stop();
                            logger.info("Successfully stopped connector {}", transportConnector.getName());
                        } catch (Exception e) {
                            logger.error("Failed to stop connector {}", transportConnector.getName(), e);
                        }
                    });


            replicaSequencer.deinitialize();
            replicaSequencer.terminateScheduledExecutor();
            removeReplicationQueues();

            this.actionListenerCallback.onDeinitializationSuccess();
        } catch (Exception e) {
            logger.error("Failed to deinitialize source broker.", e);
        }
    }

    private void initQueueProvider() {
        queueProvider.initialize();
        queueProvider.initializeSequenceQueue();
    }

    private void installTransportConnector() throws Exception {
        logger.info("Installing Transport Connector for Source broker");
        TransportConnector replicationConnector = getBrokerService().getConnectorByName(ReplicaSupport.REPLICATION_CONNECTOR_NAME);
        if (replicationConnector == null) {
            TransportConnector transportConnector = getBrokerService().addConnector(replicaPolicy.getTransportConnectorUri());
            transportConnector.setUri(replicaPolicy.getTransportConnectorUri());
            transportConnector.setName(ReplicaSupport.REPLICATION_CONNECTOR_NAME);
        }
    }

    private void removeReplicationQueues() {
        ReplicaSupport.REPLICATION_QUEUE_NAMES.stream()
                .filter(queueName -> !queueName.equals(ReplicaSupport.FAIL_OVER_SATE_QUEUE_NAME))
                .forEach(queueName -> {
                    try {
                        getBrokerService().removeDestination(new ActiveMQQueue(queueName));
                    } catch (Exception e) {
                        logger.error("Failed to delete replication queue [{}]", queueName, e);
                    }
                });
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
        boolean isAdvisoryDestination = ReplicaSupport.isAdvisoryDestination(destination);
        boolean isTemporaryDestination = destination.isTemporary();
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination && !isTemporaryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        if (isTemporaryDestination) reason += "it is a temporary destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "" : "not ", destination, reason);
        return shouldReplicate;
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
    }

    public void replicateSend(ConnectionContext context, Message message, TransactionId transactionId) {
        try {
            TransactionId originalTransactionId = message.getTransactionId();
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
                            .setTransactionId(transactionId)
                            .setReplicationProperty(ReplicaSupport.MESSAGE_ID_PROPERTY, message.getMessageId().toProducerKey())
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

    public boolean needToReplicateSend(ConnectionContext connectionContext, Message message) {
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
        if (!message.isPersistent()) {
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

    private void sendFailOverMessage() throws Exception {

        ReplicaRole currentBrokerState = replicaFailOverStateStorage.getBrokerState();
        if (currentBrokerState != null && ReplicaRole.await_ack == currentBrokerState) {
            return;
        }

        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        next.beginTransaction(connectionContext, tid);
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.FAIL_OVER)
                            .setTransactionId(tid)
                            .setEventData(eventSerializer.serializeReplicationData(connectionContext.getXid()))
                            .setReplicationProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY,
                                    ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME)
            );

            replicaFailOverStateStorage.updateBrokerState(connectionContext, tid, ReplicaRole.await_ack.name());
            next.commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            next.rollbackTransaction(connectionContext, tid);
            logger.error("Failed to send fail over message", e);
            throw e;
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
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
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
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
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
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
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) throws Exception {
        super.removeSubscription(context, subscriptionInfo);
        replicateRemoveSubscription(context, subscriptionInfo);
    }

    private void replicateRemoveSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) {
        if (ReplicaSupport.isReplicationTransport(context.getConnector())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.REMOVE_DURABLE_CONSUMER_SUBSCRIPTION)
                            .setEventData(eventSerializer.serializeReplicationData(subscriptionInfo))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate removing subscription {}", subscriptionInfo, e);
        }
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

        if (MAIN_REPLICATION_QUEUE_NAME.equals(ack.getDestination().getPhysicalName())) {
            List<MessageReference> ackedMessageList = replicaSequencer.acknowledge(consumerExchange, ack);

            MessageReference ackedMessage = ackedMessageList.stream().findFirst().orElseThrow();
            String eventType = (String) ackedMessage.getMessage().getProperty(ReplicaEventType.EVENT_TYPE_PROPERTY);

            if (ReplicaEventType.FAIL_OVER.equals(ReplicaEventType.valueOf(eventType))) {
                LocalTransactionId tid = new LocalTransactionId(
                        new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

                getNext().beginTransaction(connectionContext, tid);
                try {
                    replicaFailOverStateStorage.updateBrokerState(connectionContext, tid, ReplicaRole.replica.name());
                    getNext().commitTransaction(connectionContext, tid, true);
                } catch (Exception e) {
                    getNext().rollbackTransaction(connectionContext, tid);
                    logger.error("Failed to send broker fail over state", e);
                    throw e;
                }
                executor.execute(this::completeDeinitialization);
            }
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
        if (messageIdsToAck == null || messageIdsToAck.isEmpty()) {
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
        List<MessageReference> messagesToAck = replicaAckHelper.getMessagesToAck(ack, subscription);
        if (messagesToAck == null) {
            return null;
        }
        return messagesToAck.stream()
                .filter(MessageReference::isPersistent)
                .map(MessageReference::getMessageId)
                .map(MessageId::toProducerKey)
                .collect(Collectors.toList());
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
        if(!ReplicaSupport.isReplicationQueue(destination)) {
            replicateQueuePurged(context, destination);
        } else {
            logger.error("Replication queue was purged {}", destination.getPhysicalName());
        }
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

    private void enqueueReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(connectionContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }
        replicationMessageProducer.enqueueIntermediateReplicaEvent(connectionContext, event);
    }

    private boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }
}
