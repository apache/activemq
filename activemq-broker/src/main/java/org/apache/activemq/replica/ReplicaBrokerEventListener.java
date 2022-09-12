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
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    public static final String REPLICATION_CONSUMER_CLIENT_ID = "DUMMY_REPLICATION_CONSUMER";
    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final Broker broker;
    private final ConnectionContext connectionContext;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;

    ReplicaBrokerEventListener(Broker broker) {
        this.broker = requireNonNull(broker);
        connectionContext = broker.getAdminConnectionContext().copy();
        connectionContext.setUserName(ReplicaSupport.REPLICATION_PLUGIN_USER_NAME);
        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(broker, connectionContext);
    }

    @Override
    public void onMessage(Message jmsMessage) {
        logger.trace("Received replication message from replica source");
        ActiveMQMessage message = (ActiveMQMessage) jmsMessage;
        ByteSequence messageContent = message.getContent();

        try {
            Object deserializedData = eventSerializer.deserializeMessageData(messageContent);
            getEventType(message).ifPresent(eventType -> {
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
                        persistMessage((ActiveMQMessage) deserializedData);
                        return;
                    case MESSAGE_ACK:
                        logger.trace("Processing replicated messages dropped");
                        try {
                            messageAck((MessageAck) deserializedData,
                                    (List<String>) message.getObjectProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate messages dropped [{}]", deserializedData, e);
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
                        }
                        return;
                    case ADD_DURABLE_CONSUMER:
                        logger.trace("Processing replicated add consumer");
                        try {
                            addDurableConsumer((ConsumerInfo) deserializedData,
                                    message.getStringProperty(ReplicaSupport.CLIENT_ID_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate add consumer [{}]", deserializedData, e);
                        }
                        return;
                    case REMOVE_DURABLE_CONSUMER:
                        logger.trace("Processing replicated remove consumer");
                        removeDurableConsumer((ConsumerInfo) deserializedData);
                        return;
                    default:
                        logger.warn("Unhandled event type \"{}\" for replication message id: {}", eventType, message.getJMSMessageID());
                }
            });
            message.acknowledge();
        } catch (IOException | ClassCastException e) {
            logger.error("Failed to deserialize replication message (id={}), {}", message.getMessageId(), new String(messageContent.data), e);
            logger.debug("Deserialization error for replication message (id={})", message.getMessageId(), e);
        } catch (
        JMSException e) {
            logger.error("Failed to acknowledge replication message (id={})", message.getMessageId());
        }
    }

    private Optional<ReplicaEventType> getEventType(ActiveMQMessage message) {
        try {
            String eventTypeProperty = message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY);
            return Arrays.stream(ReplicaEventType.values())
                    .filter(t -> t.name().equals(eventTypeProperty))
                    .findFirst();
        } catch (JMSException e) {
            logger.error("Failed to get {} property {}", ReplicaEventType.class.getSimpleName(), ReplicaEventType.EVENT_TYPE_PROPERTY, e);
            return Optional.empty();
        }
    }

    private void upsertDestination(ActiveMQDestination destination) {
        try {
            boolean isExistingDestination = Arrays.stream(broker.getDestinations())
                    .anyMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isExistingDestination) {
                logger.debug("Destination [{}] already exists, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.addDestination(connectionContext, destination, true);
        } catch (Exception e) {
            logger.error("Unable to add destination [{}]", destination, e);
        }
    }

    private void deleteDestination(ActiveMQDestination destination) {
        try {
            boolean isNonExtantDestination = Arrays.stream(broker.getDestinations())
                    .noneMatch(d -> d.getQualifiedName().equals(destination.getQualifiedName()));
            if (isNonExtantDestination) {
                logger.debug("Destination [{}] does not exist, no action to take", destination);
                return;
            }
        } catch (Exception e) {
            logger.error("Unable to determine if [{}] is an existing destination", destination, e);
        }
        try {
            broker.removeDestination(connectionContext, destination, 1000);
        } catch (Exception e) {
            logger.error("Unable to remove destination [{}]", destination, e);
        }
    }

    private void persistMessage(ActiveMQMessage message) {
        try {
            if (message.getTransactionId() != null && !message.getTransactionId().isXATransaction()) {
                message.setTransactionId(null); // remove transactionId as it has been already handled on source broker
            }
            removeScheduledMessageProperties(message);
            replicaInternalMessageProducer.produceToReplicaQueue(message);
        } catch (Exception e) {
            logger.error("Failed to process message {} with JMS message id: {}", message.getMessageId(), message.getJMSMessageID(), e);
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

    private void purgeQueue(ActiveMQDestination destination) {
        try {
            Optional<Queue> queue = broker.getDestinations(destination).stream()
                    .findFirst().map(DestinationExtractor::extractQueue);
            if (queue.isPresent()) {
                queue.get().purge(connectionContext);
            }
        } catch (Exception e) {
            logger.error("Unable to replicate queue purge {}", destination, e);
        }
    }

    private void beginTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.beginTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate begin transaction [{}]", xid, e);
        }
    }

    private void prepareTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.prepareTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate prepare transaction [{}]", xid, e);
        }
    }

    private void forgetTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.forgetTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate forget transaction [{}]", xid, e);
        }
    }

    private void rollbackTransaction(TransactionId xid) {
        try {
            createTransactionMapIfNotExist();
            broker.rollbackTransaction(connectionContext, xid);
        } catch (Exception e) {
            logger.error("Unable to replicate rollback transaction [{}]", xid, e);
        }
    }

    private void commitTransaction(TransactionId xid, boolean onePhase) {
        try {
            broker.commitTransaction(connectionContext, xid, onePhase);
        } catch (Exception e) {
            logger.error("Unable to replicate commit transaction [{}]", xid, e);
        }
    }

    private void addDurableConsumer(ConsumerInfo consumerInfo, String clientId) {
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
        }
    }

    private void removeDurableConsumer(ConsumerInfo consumerInfo) {
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
        }
    }

    private void messageAck(MessageAck ack, List<String> messageIdsToAck) {
        ActiveMQDestination destination = ack.getDestination();
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

            if (ack.getTransactionId() != null && !ack.getTransactionId().isXATransaction()) {
                ack.setTransactionId(null); // remove transactionId as it has been already handled on source broker
            }

            ConsumerBrokerExchange consumerBrokerExchange = new ConsumerBrokerExchange();
            consumerBrokerExchange.setConnectionContext(connectionContext);
            broker.acknowledge(consumerBrokerExchange, ack);

            if (consumerInfo != null) {
                broker.removeConsumer(context, consumerInfo);
            }
        } catch (Exception e) {
            logger.error("Unable to ack messages ack messages [{} <-> {}] for consumer {}",
                    ack.getFirstMessageId(),
                    ack.getLastMessageId(),
                    ack.getConsumerId(), e);
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
