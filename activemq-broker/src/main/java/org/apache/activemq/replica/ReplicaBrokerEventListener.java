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
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.MessageReferenceFilter;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private static final String REPLICATION_CONSUMER_CLIENT_ID = "DUMMY_REPLICATION_CONSUMER";
    private static final String REPLICATION_SEQUENCE_CONSUMER_CLIENT_ID = "DUMMY_REPLICATION_SEQUENCE_CONSUMER";
    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final Broker broker;
    private final ConnectionContext connectionContext;
    private ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;

    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();

    private Queue sequenceQueue;
    private final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();
    private PrefetchSubscription subscription;
    private final PeriodAcknowledge<Void> acknowledgeCallback;

    BigInteger sequence;

    ReplicaBrokerEventListener(Broker broker, ReplicaReplicationQueueSupplier queueProvider, PeriodAcknowledge<Void> acknowledgeCallback) {
        this.broker = requireNonNull(broker);
        this.acknowledgeCallback = requireNonNull(acknowledgeCallback);
        connectionContext = broker.getAdminConnectionContext().copy();
        this.queueProvider = queueProvider;
        connectionContext.setUserName(ReplicaSupport.REPLICATION_PLUGIN_USER_NAME);
        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(broker, connectionContext);

        createTransactionMapIfNotExist();

        replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    public void initialize() throws Exception {
        sequenceQueue = broker.getDestinations(queueProvider.getSequenceQueue()).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();

        ConnectionContext connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setClientId(REPLICATION_SEQUENCE_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());

        ConnectionId connectionId = new ConnectionId(new IdGenerator("ReplicationPlugin.ReplicaSequence").generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(10);
        consumerInfo.setDestination(queueProvider.getSequenceQueue());
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);

        List<MessageId> allMessageIds = sequenceQueue.getAllMessageIds();
        if (allMessageIds.size() == 0) {
            return;
        }

        if (allMessageIds.size() > 1) {
            for (int i = 0; i < allMessageIds.size() - 1; i++) {
                sequenceQueue.removeMessage(allMessageIds.get(i).toString());
            }
        }
        QueueMessageReference message = sequenceQueue.getMessage(allMessageIds.get(0).toString());
        String text = ((ActiveMQTextMessage) message.getMessage()).getText();
        sequence = new BigInteger(text);
    }

    @Override
    public void onMessage(Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        ActiveMQMessage message = (ActiveMQMessage) jmsMessage;

        try {
            processMessageWithRetries(message, null);

            message.acknowledge();
        } catch (JMSException e) {
            acknowledgeCallback.setSafeToAck(false);
            logger.error("Failed to acknowledge replication message (id={})", message.getMessageId());
        }
    }

    private synchronized void processMessageWithRetries(ActiveMQMessage message, TransactionId transactionId) {
        new ReplicaEventRetrier(() -> {
            boolean commit = false;
            TransactionId tid = transactionId;
            if (tid == null) {
                tid = new LocalTransactionId(
                        new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                        localTransactionIdGenerator.getNextSequenceId());

                broker.beginTransaction(connectionContext, tid);

                commit = true;
            }

            try {
                ReplicaEventType eventType = getEventType(message);
                if (eventType == ReplicaEventType.BATCH) {
                    processBatch(message, tid);
                } else {
                    processMessage(message, eventType, tid);
                }

                if (commit) {
                    List<MessageReference> dispatched = subscription.getDispatched();

                    if (dispatched.size() > 0) {
                        ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
                        consumerExchange.setConnectionContext(connectionContext);
                        consumerExchange.setSubscription(subscription);

                        MessageAck ack = new MessageAck();
                        ack.setFirstMessageId(dispatched.get(0).getMessageId());
                        ack.setLastMessageId(dispatched.get(dispatched.size() - 1).getMessageId());
                        ack.setMessageCount(dispatched.size());
                        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
                        ack.setDestination(queueProvider.getSequenceQueue());

                        broker.acknowledge(consumerExchange, ack);
                    }

                    ActiveMQTextMessage seqMessage = new ActiveMQTextMessage();
                    seqMessage.setText(sequence.toString());
                    seqMessage.setTransactionId(tid);
                    seqMessage.setDestination(queueProvider.getSequenceQueue());
                    seqMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
                    seqMessage.setProducerId(replicationProducerId);
                    seqMessage.setPersistent(true);
                    seqMessage.setResponseRequired(false);

                    replicaInternalMessageProducer.sendIgnoringFlowControl(seqMessage);

                    broker.commitTransaction(connectionContext, tid, true);
                }
            } catch (Exception e) {
                if (commit) {
                    broker.rollbackTransaction(connectionContext, tid);
                }
                throw e;
            }
            return null;
        }).process();
    }

    private void processMessage(ActiveMQMessage message, ReplicaEventType eventType, TransactionId transactionId) throws Exception {
        Object deserializedData = eventSerializer.deserializeMessageData(message.getContent());
        BigInteger newSequence = new BigInteger(message.getStringProperty(ReplicaSupport.SEQUENCE_PROPERTY));

        long sequenceDifference = sequence == null ? 0 : newSequence.subtract(sequence).longValue();
        if (sequence == null || sequenceDifference == 1) {
            processMessage(message, eventType, deserializedData, transactionId);

            sequence = newSequence;

        } else if (sequenceDifference > 0) {
            throw new IllegalStateException(String.format(
                    "Replication event is out of order. Current sequence: %s, the sequence of the event: %s",
                    sequence, newSequence));
        }
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
                persistMessage((ActiveMQMessage) deserializedData, transactionId);
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
            default:
                throw new IllegalStateException(
                        String.format("Unhandled event type \"%s\" for replication message id: %s",
                                eventType, message.getJMSMessageID()));
        }
    }

    private void processBatch(ActiveMQMessage message, TransactionId tid) throws Exception {
        List<Object> objects = eventSerializer.deserializeListOfObjects(message.getContent().getData());
        for (Object o : objects) {
            processMessageWithRetries((ActiveMQMessage) o, tid);
        }
    }

    private void upsertDestination(ActiveMQDestination destination) throws Exception {
        try {
            boolean isExistingDestination = Arrays.stream(broker.getDestinations())
                    .anyMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isExistingDestination) {
                logger.debug("Destination [{}] already exists, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
            throw e;
        }
        try {
            broker.addDestination(connectionContext, destination, true);
        } catch (Exception e) {
            logger.error("Unable to add destination [{}]", destination, e);
            throw e;
        }
    }

    private void deleteDestination(ActiveMQDestination destination) throws Exception {
        try {
            boolean isNonExtantDestination = Arrays.stream(broker.getDestinations())
                    .noneMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isNonExtantDestination) {
                logger.debug("Destination [{}] does not exist, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
            throw e;
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

    private void persistMessage(ActiveMQMessage message, TransactionId transactionId) throws Exception {
        try {
            if (message.getTransactionId() == null || !message.getTransactionId().isXATransaction()) {
                message.setTransactionId(transactionId);
            }
            removeScheduledMessageProperties(message);
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
            createTransactionMapIfNotExist();
            broker.prepareTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate prepare transaction [{}]", xid, e);
            throw e;
        }
    }

    private void forgetTransaction(TransactionId xid) throws Exception {
        try {
            createTransactionMapIfNotExist();
            broker.forgetTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate forget transaction [{}]", xid, e);
            throw e;
        }
    }

    private void rollbackTransaction(TransactionId xid) throws Exception {
        try {
            createTransactionMapIfNotExist();
            broker.rollbackTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate rollback transaction [{}]", xid, e);
            throw e;
        }
    }

    private void commitTransaction(TransactionId xid, boolean onePhase) throws Exception {
        try {
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
            DurableTopicSubscription subscription = (DurableTopicSubscription) broker.addConsumer(context, consumerInfo);
            // We don't want to keep it active to be able to connect to it on the other side when needed
            // but we want to have keepDurableSubsActive to be able to acknowledge
            subscription.deactivate(true, 0);
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

    private void messageAck(MessageAck ack, List<String> messageIdsToAck, TransactionId transactionId) throws Exception {
        ActiveMQDestination destination = ack.getDestination();
        MessageAck messageAck = new MessageAck();
        try {

            ConnectionContext context = connectionContext.copy();
            context.setClientId(REPLICATION_CONSUMER_CLIENT_ID);
            context.setConnection(new DummyConnection());

            ConsumerInfo consumerInfo = null;
            if (destination.isQueue()) {
                consumerInfo = new ConsumerInfo();
                consumerInfo.setConsumerId(ack.getConsumerId());
                consumerInfo.setPrefetchSize(0);
                consumerInfo.setDestination(destination);
                broker.addConsumer(context, consumerInfo);
            }

            for (String messageId : messageIdsToAck) {
                messageDispatch(ack.getConsumerId(), destination, messageId);
            }

            ack.copy(messageAck);

            messageAck.setMessageCount(messageIdsToAck.size());
            messageAck.setFirstMessageId(new MessageId(messageIdsToAck.get(0)));
            messageAck.setLastMessageId(new MessageId(messageIdsToAck.get(messageIdsToAck.size() - 1)));

            if (messageAck.getTransactionId() == null || !messageAck.getTransactionId().isXATransaction()) {
                messageAck.setTransactionId(transactionId);
            }

            ConsumerBrokerExchange consumerBrokerExchange = new ConsumerBrokerExchange();
            consumerBrokerExchange.setConnectionContext(connectionContext);
            broker.acknowledge(consumerBrokerExchange, messageAck);

            if (consumerInfo != null) {
                broker.removeConsumer(context, consumerInfo);
            }
        } catch (Exception e) {
            logger.error("Unable to ack messages [{} <-> {}] for consumer {}",
                    ack.getFirstMessageId(),
                    ack.getLastMessageId(),
                    ack.getConsumerId(), e);
            throw e;
        }
    }

    private void createTransactionMapIfNotExist() {
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }
    }

    static class ListMessageReferenceFilter implements MessageReferenceFilter {
        final Set<String> messageIds;

        public ListMessageReferenceFilter(List<String> messageIds) {
            this.messageIds = new HashSet<>(messageIds);
        }

        @Override
        public boolean evaluate(ConnectionContext context, MessageReference messageReference) throws JMSException {
            return messageIds.contains(messageReference.getMessageId().toString());
        }
    }
}
