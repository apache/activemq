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
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.storage.ReplicaSequenceStorage;
import org.apache.activemq.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.transaction.xa.XAException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private static final String REPLICATION_CONSUMER_CLIENT_ID = "DUMMY_REPLICATION_CONSUMER";
    private static final String SEQUENCE_NAME = "replicaSeq";
    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final ReplicaBroker replicaBroker;
    private final Broker broker;
    private final ConnectionContext connectionContext;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final PeriodAcknowledge acknowledgeCallback;
    private final ReplicaStatistics replicaStatistics;
    private final AtomicReference<ReplicaEventRetrier> replicaEventRetrier = new AtomicReference<>();
    final ReplicaSequenceStorage sequenceStorage;
    private final TransactionBroker transactionBroker;

    BigInteger sequence;
    MessageId sequenceMessageId;

    ReplicaBrokerEventListener(ReplicaBroker replicaBroker, ReplicaReplicationQueueSupplier queueProvider,
            PeriodAcknowledge acknowledgeCallback, ReplicaStatistics replicaStatistics) {
        this.replicaBroker = requireNonNull(replicaBroker);
        this.broker = requireNonNull(replicaBroker.getNext());
        this.acknowledgeCallback = requireNonNull(acknowledgeCallback);
        this.replicaStatistics = replicaStatistics;
        connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setUserName(ReplicaSupport.REPLICATION_PLUGIN_USER_NAME);
        connectionContext.setClientId(REPLICATION_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());
        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(broker, connectionContext);

        createTransactionMapIfNotExist();

        this.sequenceStorage = new ReplicaSequenceStorage(broker,
                queueProvider, replicaInternalMessageProducer, SEQUENCE_NAME);
        this.transactionBroker = (TransactionBroker) broker.getAdaptor(TransactionBroker.class);
    }

    public void initialize() throws Exception {
        String savedSequence = sequenceStorage.initialize(connectionContext);
        if (savedSequence == null) {
            return;
        }

        String[] split = savedSequence.split("#");
        if (split.length != 2) {
            throw new IllegalStateException("Unknown sequence message format: " + savedSequence);
        }
        sequence = new BigInteger(split[0]);

        sequenceMessageId = new MessageId(split[1]);
    }

    public void deinitialize() throws Exception {
        sequenceStorage.deinitialize(connectionContext);
    }

    @Override
    public void onMessage(Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        ActiveMQMessage message = (ActiveMQMessage) jmsMessage;

        try {
            processMessageWithRetries(message, null);
        } catch (BrokerStoppedException bse) {
            logger.warn("The broker has been stopped");
        } catch (InterruptedException ie) {
            logger.warn("Retrier interrupted: {}", ie.toString());
        }
    }

    public void close() {
        ReplicaEventRetrier retrier = replicaEventRetrier.get();
        if (retrier != null) {
            retrier.stop();
        }
    }

    private synchronized void processMessageWithRetries(ActiveMQMessage message, TransactionId transactionId) throws InterruptedException {
        ReplicaEventRetrier retrier = new ReplicaEventRetrier(() -> {
            boolean commit = false;
            TransactionId tid = transactionId;
            ReplicaEventType eventType = getEventType(message);

            if (tid == null && eventType != ReplicaEventType.FAIL_OVER) {
                tid = new LocalTransactionId(
                        new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

                broker.beginTransaction(connectionContext, tid);

                commit = true;
            }

            try {
                if (eventType == ReplicaEventType.BATCH) {
                    processBatch(message, tid);
                } else {
                    processMessage(message, eventType, tid);
                }

                if (commit) {
                    sequenceStorage.enqueue(connectionContext, tid, sequence.toString() + "#" + sequenceMessageId);

                    broker.commitTransaction(connectionContext, tid, true);
                    acknowledgeCallback.setSafeToAck(true);
                }
            } catch (Exception e) {
                if (commit) {
                    broker.rollbackTransaction(connectionContext, tid);
                }
                acknowledgeCallback.setSafeToAck(false);
                throw e;
            }
            return null;
        });
        replicaEventRetrier.set(retrier);
        try {
            retrier.process();
        } finally {
            replicaEventRetrier.set(null);
        }
    }

    private void processMessage(ActiveMQMessage message, ReplicaEventType eventType, TransactionId transactionId) throws Exception {
        int messageVersion = message.getIntProperty(ReplicaSupport.VERSION_PROPERTY);
        if (messageVersion > ReplicaSupport.CURRENT_VERSION) {
            throw new IllegalStateException("Unsupported version of replication event: " + messageVersion + ".  Maximum supported version: " + ReplicaSupport.CURRENT_VERSION);
        }
        Object deserializedData = eventSerializer.deserializeMessageData(message.getContent());
        BigInteger newSequence = new BigInteger(message.getStringProperty(ReplicaSupport.SEQUENCE_PROPERTY));

        long sequenceDifference = sequence == null ? 0 : newSequence.subtract(sequence).longValue();
        MessageId messageId = message.getMessageId();
        if (sequence == null || sequenceDifference == 1) {
            processMessage(message, eventType, deserializedData, transactionId);

            sequence = newSequence;
            sequenceMessageId = messageId;

        } else if (sequenceDifference > 0) {
            throw new IllegalStateException(String.format(
                    "Replication event is out of order. Current sequence: %s, the sequence of the event: %s",
                    sequence, newSequence));
        } else if (sequenceDifference < 0) {
            logger.info(String.format(
                    "Replication message duplicate. Current sequence: %s, the sequence of the event: %s",
                    sequence, newSequence));
        } else if (!sequenceMessageId.equals(messageId)) {
            throw new IllegalStateException(String.format(
                    "Replication event is out of order. Current sequence %s belongs to message with id %s," +
                            "but the id of the event is %s", sequence, sequenceMessageId, messageId));
        }

        long currentTime = System.currentTimeMillis();
        replicaStatistics.setReplicationLag(currentTime - message.getTimestamp());
        replicaStatistics.setReplicaLastProcessedTime(currentTime);
    }

    private void processMessage(ActiveMQMessage message, ReplicaEventType eventType, Object deserializedData,
            TransactionId transactionId) throws Exception {
        switch (eventType) {
            case DESTINATION_UPSERT:
                logger.trace("Processing replicated destination");
                upsertDestination((ActiveMQDestination) deserializedData);
                return;
            case DESTINATION_DELETE:
                logger.trace("Processing replicated destination deletion");
                deleteDestination((ActiveMQDestination) deserializedData);
                return;
            case MESSAGE_SEND:
                logger.trace("Processing replicated message send");
                sendMessage((ActiveMQMessage) deserializedData, transactionId);
                return;
            case MESSAGE_ACK:
                logger.trace("Processing replicated messages dropped");
                try {
                    messageAck((MessageAck) deserializedData,
                            (List<String>) message.getObjectProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY), transactionId);
                } catch (JMSException e) {
                    logger.error("Failed to extract property to replicate messages dropped [{}]", deserializedData, e);
                    throw new Exception(e);
                }
                return;
            case QUEUE_PURGED:
                logger.trace("Processing queue purge");
                purgeQueue((ActiveMQDestination) deserializedData);
                return;
            case TRANSACTION_BEGIN:
                logger.trace("Processing replicated transaction begin");
                beginTransaction((TransactionId) deserializedData);
                return;
            case TRANSACTION_PREPARE:
                logger.trace("Processing replicated transaction prepare");
                prepareTransaction((TransactionId) deserializedData);
                return;
            case TRANSACTION_FORGET:
                logger.trace("Processing replicated transaction forget");
                forgetTransaction((TransactionId) deserializedData);
                return;
            case TRANSACTION_ROLLBACK:
                logger.trace("Processing replicated transaction rollback");
                rollbackTransaction((TransactionId) deserializedData);
                return;
            case TRANSACTION_COMMIT:
                logger.trace("Processing replicated transaction commit");
                try {
                    commitTransaction(
                            (TransactionId) deserializedData,
                            message.getBooleanProperty(ReplicaSupport.TRANSACTION_ONE_PHASE_PROPERTY));
                } catch (JMSException e) {
                    logger.error("Failed to extract property to replicate transaction commit with id [{}]", deserializedData, e);
                    throw new Exception(e);
                }
                return;
            case ADD_DURABLE_CONSUMER:
                logger.trace("Processing replicated add consumer");
                try {
                    addDurableConsumer((ConsumerInfo) deserializedData,
                            message.getStringProperty(ReplicaSupport.CLIENT_ID_PROPERTY));
                } catch (JMSException e) {
                    logger.error("Failed to extract property to replicate add consumer [{}]", deserializedData, e);
                    throw new Exception(e);
                }
                return;
            case REMOVE_DURABLE_CONSUMER:
                logger.trace("Processing replicated remove consumer");
                removeDurableConsumer((ConsumerInfo) deserializedData);
                return;
            case MESSAGE_EXPIRED:
                logger.trace("Processing replicated message expired");
                messageExpired((ActiveMQMessage) deserializedData);
                return;
            case REMOVE_DURABLE_CONSUMER_SUBSCRIPTION:
                logger.trace("Processing replicated remove durable consumer subscription");
                removeDurableConsumerSubscription((RemoveSubscriptionInfo) deserializedData);
                return;
            case FAIL_OVER:
                failOver();
                return;
            case HEART_BEAT:
                logger.trace("Heart beat message received");
                return;
            default:
                throw new IllegalStateException(
                        String.format("Unhandled event type \"%s\" for replication message id: %s",
                                eventType, message.getJMSMessageID()));
        }
    }

    private boolean isDestinationExisted(ActiveMQDestination destination) throws Exception {
        try {
            return Arrays.stream(broker.getDestinations())
                    .anyMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
            throw e;
        }
    }

    private boolean isTransactionExisted(TransactionId transactionId) throws Exception {
        try {
            Transaction transaction = transactionBroker.getTransaction(connectionContext, transactionId, false);
            return transaction != null;
        }
        catch (XAException e) {
            logger.error("Transaction cannot be found - non-existing transaction [{}]", transactionId, e);
            return false;
        }
    }

    private boolean isExceptionDueToNonExistingMessage(JMSException exception) {
        if (exception.getMessage().contains("Slave broker out of sync with master - Message:")
                && exception.getMessage().contains("does not exist among pending")) {
            return true;
        }

        return false;
    }

    private void processBatch(ActiveMQMessage message, TransactionId tid) throws Exception {
        List<Object> objects = eventSerializer.deserializeListOfObjects(message.getContent().getData());
        for (Object o : objects) {
            processMessageWithRetries((ActiveMQMessage) o, tid);
        }
    }

    private void upsertDestination(ActiveMQDestination destination) throws Exception {
        if (isDestinationExisted(destination)) {
            logger.debug("Destination [{}] already exists, no action to take", destination);
            return;
        }
        try {
            broker.addDestination(connectionContext, destination, true);
        } catch (Exception e) {
            logger.error("Unable to add destination [{}]", destination, e);
            throw e;
        }
    }

    private void deleteDestination(ActiveMQDestination destination) throws Exception {
        if (!isDestinationExisted(destination)) {
            logger.debug("Destination [{}] does not exist, no action to take", destination);
            return;
        }
        try {
            broker.removeDestination(connectionContext, destination, 1000);
        } catch (Exception e) {
            logger.error("Unable to remove destination [{}]", destination, e);
            throw e;
        }
    }

    private ReplicaEventType getEventType(ActiveMQMessage message) throws JMSException {
        return ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
    }

    private void sendMessage(ActiveMQMessage message, TransactionId transactionId) throws Exception {
        try {
            if (message.getTransactionId() == null || !message.getTransactionId().isXATransaction()) {
                message.setTransactionId(transactionId);
            }
            removeScheduledMessageProperties(message);

            if(message.getExpiration() > 0 && System.currentTimeMillis() + 1000 > message.getExpiration()) {
                message.setExpiration(System.currentTimeMillis() + 1000);
            }

            replicaInternalMessageProducer.sendIgnoringFlowControl(message);
        } catch (Exception e) {
            logger.error("Failed to process message {} with JMS message id: {}", message.getMessageId(), message.getJMSMessageID(), e);
            throw e;
        }
    }

    private void messageDispatch(ConsumerId consumerId, ActiveMQDestination destination, String messageId) throws Exception {
        MessageDispatchNotification mdn = new MessageDispatchNotification();
        mdn.setConsumerId(consumerId);
        mdn.setDestination(destination);
        mdn.setMessageId(new MessageId(messageId));
        broker.processDispatchNotification(mdn);
    }

    private void removeScheduledMessageProperties(ActiveMQMessage message) throws IOException {
        message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD);
        message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT);
        message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_CRON);
        message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY);
    }

    private void purgeQueue(ActiveMQDestination destination) throws Exception {
        try {
            Optional<Queue> queue = broker.getDestinations(destination).stream()
                    .findFirst().map(DestinationExtractor::extractQueue);
            if (queue.isPresent()) {
                queue.get().purge(connectionContext);
            }
        } catch (Exception e) {
            logger.error("Unable to replicate queue purge {}", destination, e);
            throw e;
        }
    }

    private void beginTransaction(TransactionId xid) throws Exception {
        try {
            createTransactionMapIfNotExist();
            broker.beginTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate begin transaction [{}]", xid, e);
            throw e;
        }
    }

    private void prepareTransaction(TransactionId xid) throws Exception {
        try {
            if (xid.isXATransaction() && !isTransactionExisted(xid)) {
                logger.warn("Skip processing transaction event - non-existing XA transaction [{}]", xid);
                return;
            }
            createTransactionMapIfNotExist();
            broker.prepareTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate prepare transaction [{}]", xid, e);
            throw e;
        }
    }

    private void forgetTransaction(TransactionId xid) throws Exception {
        try {
            if (xid.isXATransaction() && !isTransactionExisted(xid)) {
                logger.warn("Skip processing transaction event - non-existing XA transaction [{}]", xid);
                return;
            }
            createTransactionMapIfNotExist();
            broker.forgetTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate forget transaction [{}]", xid, e);
            throw e;
        }
    }

    private void rollbackTransaction(TransactionId xid) throws Exception {
        try {
            if (xid.isXATransaction() && !isTransactionExisted(xid)) {
                logger.warn("Skip processing transaction event - non-existing XA transaction [{}]", xid);
                return;
            }
            createTransactionMapIfNotExist();
            broker.rollbackTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate rollback transaction [{}]", xid, e);
            throw e;
        }
    }

    private void commitTransaction(TransactionId xid, boolean onePhase) throws Exception {
        try {
            if (xid.isXATransaction() && !isTransactionExisted(xid)) {
                logger.warn("Skip processing transaction event - non-existing XA transaction [{}]", xid);
                return;
            }
            broker.commitTransaction(connectionContext, xid, onePhase);
        } catch (Exception e) {
            logger.error("Unable to replicate commit transaction [{}]", xid, e);
            throw e;
        }
    }

    private void addDurableConsumer(ConsumerInfo consumerInfo, String clientId) throws Exception {
        try {
            consumerInfo.setPrefetchSize(0);
            ConnectionContext context = connectionContext.copy();
            context.setClientId(clientId);
            context.setConnection(new DummyConnection());
            DurableTopicSubscription durableTopicSubscription = (DurableTopicSubscription) broker.addConsumer(context, consumerInfo);
            // We don't want to keep it active to be able to connect to it on the other side when needed
            // but we want to have keepDurableSubsActive to be able to acknowledge
            durableTopicSubscription.deactivate(true, 0);
        } catch (Exception e) {
            logger.error("Unable to replicate add durable consumer [{}]", consumerInfo, e);
            throw e;
        }
    }

    private void removeDurableConsumer(ConsumerInfo consumerInfo) throws Exception {
        try {
            ConnectionContext context = broker.getDestinations(consumerInfo.getDestination()).stream()
                    .findFirst()
                    .map(Destination::getConsumers)
                    .stream().flatMap(Collection::stream)
                    .filter(v -> v.getConsumerInfo().getClientId().equals(consumerInfo.getClientId()))
                    .findFirst()
                    .map(Subscription::getContext)
                    .orElse(null);
            if (context == null || !ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(context.getUserName())) {
                // a real consumer had stolen the context before we got the message
                return;
            }

            broker.removeConsumer(context, consumerInfo);
        } catch (Exception e) {
            logger.error("Unable to replicate remove durable consumer [{}]", consumerInfo, e);
            throw e;
        }
    }

    private void removeDurableConsumerSubscription(RemoveSubscriptionInfo subscriptionInfo) throws Exception {
        try {
            ConnectionContext context = connectionContext.copy();
            context.setClientId(subscriptionInfo.getClientId());
            broker.removeSubscription(context, subscriptionInfo);
        } catch (Exception e) {
            logger.error("Unable to replicate remove durable consumer subscription [{}]", subscriptionInfo, e);
            throw e;
        }
    }

    private void messageAck(MessageAck ack, List<String> messageIdsToAck, TransactionId transactionId) throws Exception {
        ActiveMQDestination destination = ack.getDestination();
        MessageAck messageAck = new MessageAck();
        try {
            if (!isDestinationExisted(destination)) {
                logger.warn("Skip MESSAGE_ACK processing event due to non-existing destination [{}]", destination.getPhysicalName());
                return;
            }
            ConsumerInfo consumerInfo = null;
            if (destination.isQueue()) {
                consumerInfo = new ConsumerInfo();
                consumerInfo.setConsumerId(ack.getConsumerId());
                consumerInfo.setPrefetchSize(0);
                consumerInfo.setDestination(destination);
                broker.addConsumer(connectionContext, consumerInfo);
            }

            List<String> existingMessageIdsToAck = new ArrayList();
            for (String messageId : messageIdsToAck) {
                try {
                    messageDispatch(ack.getConsumerId(), destination, messageId);
                    existingMessageIdsToAck.add(messageId);
                } catch (JMSException e) {
                    if (isExceptionDueToNonExistingMessage(e)) {
                        logger.warn("Skip MESSAGE_ACK processing event due to non-existing message [{}]", messageId);
                    } else {
                        throw e;
                    }
                }
            }

            if (existingMessageIdsToAck.size() == 0) {
                return;
            }

            ack.copy(messageAck);

            messageAck.setMessageCount(existingMessageIdsToAck.size());
            messageAck.setFirstMessageId(new MessageId(existingMessageIdsToAck.get(0)));
            messageAck.setLastMessageId(new MessageId(existingMessageIdsToAck.get(existingMessageIdsToAck.size() - 1)));

            if (messageAck.getTransactionId() == null || !messageAck.getTransactionId().isXATransaction()) {
                messageAck.setTransactionId(transactionId);
            }

            if (messageAck.isPoisonAck()) {
                messageAck.setAckType(MessageAck.STANDARD_ACK_TYPE);
            }

            ConsumerBrokerExchange consumerBrokerExchange = new ConsumerBrokerExchange();
            consumerBrokerExchange.setConnectionContext(connectionContext);
            broker.acknowledge(consumerBrokerExchange, messageAck);

            if (consumerInfo != null) {
                broker.removeConsumer(connectionContext, consumerInfo);
            }
        } catch (Exception e) {
            logger.error("Unable to ack messages [{} <-> {}] for consumer {}",
                    ack.getFirstMessageId(),
                    ack.getLastMessageId(),
                    ack.getConsumerId(), e);
            throw e;
        }
    }

    private void messageExpired(ActiveMQMessage message) {
        try {
            Destination destination = broker.getDestinations(message.getDestination()).stream()
                    .findFirst().map(DestinationExtractor::extractBaseDestination).orElseThrow();
            message.setRegionDestination(destination);
            destination.messageExpired(connectionContext, null, new IndirectMessageReference(message));
        } catch (Exception e) {
            logger.error("Unable to replicate message expired [{}]", message.getMessageId(), e);
            throw e;
        }
    }

    private void failOver() throws Exception {
        replicaBroker.updateBrokerState(ReplicaRole.ack_processed);
        acknowledgeCallback.acknowledge(true);
        replicaBroker.updateBrokerState(ReplicaRole.source);
        replicaBroker.completeBeforeRoleChange();
    }

    private void createTransactionMapIfNotExist() {
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }
    }
}
