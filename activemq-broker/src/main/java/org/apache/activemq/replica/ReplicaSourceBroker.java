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
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.activemq.replica.ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME;

public class ReplicaSourceBroker extends MutativeRoleBroker {
    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {
    }; // used in destination map to indicate mirrored status

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final ScheduledExecutorService heartBeatPoller = Executors.newSingleThreadScheduledExecutor();
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final AtomicBoolean initialized = new AtomicBoolean();

    private final ReplicationMessageProducer replicationMessageProducer;
    private final ReplicaSequencer replicaSequencer;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaPolicy replicaPolicy;
    private final ReplicaAckHelper replicaAckHelper;
    private ScheduledFuture<?> heartBeatScheduledFuture;

    final DestinationMap destinationsToReplicate = new DestinationMap();

    public ReplicaSourceBroker(Broker broker, ReplicaRoleManagement management, ReplicationMessageProducer replicationMessageProducer,
            ReplicaSequencer replicaSequencer, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaPolicy replicaPolicy) {
        super(broker, management);
        this.replicationMessageProducer = replicationMessageProducer;
        this.replicaSequencer = replicaSequencer;
        this.queueProvider = queueProvider;
        this.replicaPolicy = replicaPolicy;
        this.replicaAckHelper = new ReplicaAckHelper(next);
    }

    @Override
    public void start(ReplicaRole role) throws Exception {
        logger.info("Starting Source broker. " + (role == ReplicaRole.await_ack ? " Awaiting ack." : ""));

        initQueueProvider();
        initialized.compareAndSet(false, true);
        replicaSequencer.initialize();
        initializeHeartBeatSender();
        ensureDestinationsAreReplicated();
    }

    @Override
    public void brokerServiceStarted(ReplicaRole role) {
        if (role == ReplicaRole.await_ack) {
            stopAllConnections();
        }
    }

    @Override
    public void stop() throws Exception {
        replicaSequencer.deinitialize();
        super.stop();
        initialized.compareAndSet(true, false);
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        logger.info("Stopping Source broker. Forced [{}]", force);
        stopAllConnections();
        if (force) {
            stopBeforeForcedRoleChange();
        } else {
            sendFailOverMessage();
        }
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        logger.info("Starting Source broker after role change");
        startAllConnections();

        initQueueProvider();
        initialized.compareAndSet(false, true);
        replicaSequencer.initialize();
        initializeHeartBeatSender();
        replicaSequencer.updateMainQueueConsumerStatus();
    }

    private void initializeHeartBeatSender() {
        if (replicaPolicy.getHeartBeatPeriod() > 0) {
            heartBeatScheduledFuture = heartBeatPoller.scheduleAtFixedRate(() -> {
                try {
                    enqueueReplicaEvent(
                            getAdminConnectionContext(),
                            new ReplicaEvent()
                                    .setEventType(ReplicaEventType.HEART_BEAT)
                                    .setEventData(eventSerializer.serializeReplicationData(null))
                    );
                } catch (Exception e) {
                    logger.error("Failed to send heart beat message", e);
                }
            }, replicaPolicy.getHeartBeatPeriod(), replicaPolicy.getHeartBeatPeriod(), TimeUnit.MILLISECONDS);
        }
    }


    private void stopBeforeForcedRoleChange() throws Exception {
        updateBrokerState(ReplicaRole.replica);
        completeBeforeRoleChange();
    }

    private void completeBeforeRoleChange() throws Exception {
        replicaSequencer.deinitialize();
        if (heartBeatScheduledFuture != null) {
            heartBeatScheduledFuture.cancel(true);
        }
        removeReplicationQueues();

        onStopSuccess();
    }

    private void initQueueProvider() {
        queueProvider.initialize();
        queueProvider.initializeSequenceQueue();
    }

    private void ensureDestinationsAreReplicated() throws Exception {
        for (ActiveMQDestination d : getDurableDestinations()) {
            if (shouldReplicateDestination(d)) {
                replicateDestinationCreation(getAdminConnectionContext(), d);
            }
        }
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) throws Exception {
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
            throw e;
        }
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = ReplicaSupport.isReplicationDestination(destination);
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

    public void replicateSend(ConnectionContext context, Message message, TransactionId transactionId) throws Exception {
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
            throw e;
        }
    }

    public boolean needToReplicateSend(ConnectionContext connectionContext, Message message) {
        if (isReplicaContext(connectionContext)) {
            return false;
        }
        if (ReplicaSupport.isReplicationDestination(message.getDestination())) {
            return false;
        }
        if (message.getDestination().isTemporary()) {
            return false;
        }
        if (message.isAdvisory()) {
            return false;
        }
        if (!message.isPersistent()) {
            return false;
        }

        return true;
    }

    private void replicateBeginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_BEGIN)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate begin of transaction [{}]", xid);
            throw e;
        }
    }

    private void replicatePrepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_PREPARE)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction prepare [{}]", xid);
            throw e;
        }
    }

    private void replicateForgetTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_FORGET)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction forget [{}]", xid);
            throw e;
        }
    }

    private void replicateRollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.TRANSACTION_ROLLBACK)
                            .setEventData(eventSerializer.serializeReplicationData(xid))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate transaction rollback [{}]", xid);
            throw e;
        }
    }

    private void replicateCommitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
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
            throw e;
        }
    }


    private void replicateDestinationRemoval(ConnectionContext context, ActiveMQDestination destination) throws Exception {
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
            throw e;
        }
    }

    private void sendFailOverMessage() throws Exception {
        ConnectionContext connectionContext = createConnectionContext();

        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        super.beginTransaction(connectionContext, tid);
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.FAIL_OVER)
                            .setTransactionId(tid)
                            .setEventData(eventSerializer.serializeReplicationData(null))
            );

            updateBrokerState(connectionContext, tid, ReplicaRole.await_ack);
            super.commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            super.rollbackTransaction(connectionContext, tid);
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

    private void replicateAddConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
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
            throw e;
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

    private void replicateRemoveConsumer(ConnectionContext context, ConsumerInfo consumerInfo) throws Exception {
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
            throw e;
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) throws Exception {
        super.removeSubscription(context, subscriptionInfo);
        replicateRemoveSubscription(context, subscriptionInfo);
    }

    private void replicateRemoveSubscription(ConnectionContext context, RemoveSubscriptionInfo subscriptionInfo) throws Exception {
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
            throw e;
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
        if (ReplicaSupport.isReplicationDestination(ack.getDestination())) {
            return false;
        }
        if (ack.getDestination().isTemporary()) {
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
        if (ack.isDeliveredAck() || ack.isUnmatchedAck() || ack.isExpiredAck()) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        ConnectionContext connectionContext = consumerExchange.getConnectionContext();

        if (MAIN_REPLICATION_QUEUE_NAME.equals(ack.getDestination().getPhysicalName())) {
            LocalTransactionId transactionId = new LocalTransactionId(
                    new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                    ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

            super.beginTransaction(connectionContext, transactionId);
            ack.setTransactionId(transactionId);

            boolean failover = false;
            try {
                List<MessageReference> ackedMessageList = replicaSequencer.acknowledge(consumerExchange, ack);

                for (MessageReference mr : ackedMessageList) {
                    ActiveMQMessage message = (ActiveMQMessage) mr.getMessage();
                    ReplicaEventType eventType =
                            ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
                    if (eventType == ReplicaEventType.FAIL_OVER) {
                        failover = true;
                        break;
                    }
                }

                if (failover) {
                    updateBrokerState(connectionContext, transactionId, ReplicaRole.replica);
                }

                super.commitTransaction(connectionContext, transactionId, true);
            } catch (Exception e) {
                super.rollbackTransaction(connectionContext, transactionId);
                logger.error("Failed to send broker fail over state", e);
                throw e;
            }
            if (failover) {
                completeBeforeRoleChange();
            }

            return;
        }

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

        if (!ack.isPoisonAck()) {
            if (ack.getTransactionId() != null && !ack.getTransactionId().isXATransaction()) {
                transactionId = ack.getTransactionId();
            } else if (ack.getTransactionId() == null) {
                transactionId = new LocalTransactionId(new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());
                super.beginTransaction(connectionContext, transactionId);
                ack.setTransactionId(transactionId);
                isInternalTransaction = true;
            }
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
            List<String> messageIdsToAck) throws Exception {
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
            throw e;
        }
    }

    @Override
    public void queuePurged(ConnectionContext context, ActiveMQDestination destination) {
        super.queuePurged(context, destination);
        if(!ReplicaSupport.isReplicationDestination(destination)) {
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

    @Override
    public void messageExpired(ConnectionContext context, MessageReference message, Subscription subscription) {
        super.messageExpired(context, message, subscription);
        replicateMessageExpired(context, message);
    }

    private void replicateMessageExpired(ConnectionContext context, MessageReference reference) {
        Message message = reference.getMessage();
        if (!isReplicatedDestination(message.getDestination())) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_EXPIRED)
                            .setEventData(eventSerializer.serializeReplicationData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate discard of {}", reference.getMessageId(), e);
        }
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause) {
        if(ReplicaSupport.isReplicationDestination(messageReference.getMessage().getDestination())) {
            logger.error("A replication event is being sent to DLQ. It shouldn't even happen: " + messageReference.getMessage(), poisonCause);
            return false;
        }

        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
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
