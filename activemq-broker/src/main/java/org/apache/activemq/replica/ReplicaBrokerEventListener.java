package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.MessageReferenceFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

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
                    case MESSAGES_DROPPED:
                        logger.trace("Processing replicated messages dropped");
                        try {
                            dropMessages(
                                    (ActiveMQDestination) deserializedData,
                                    (List<String>) message.getObjectProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY));
                        } catch (JMSException e) {
                            logger.error("Failed to extract property to replicate messages dropped [{}]", deserializedData, e);
                        }
                        return;
                default:
                    logger.warn("Unhandled event type \"{}\" for replication message id: {}", eventType, message.getJMSMessageID());
                }
            });
            message.acknowledge();
        } catch (IOException | ClassCastException e) {
            logger.error("Failed to deserialize replication message (id={}), {}", message.getMessageId(), new String(messageContent.data));
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
            replicaInternalMessageProducer.produceToReplicaQueue(message);
        } catch (Exception e) {
            logger.error("Failed to process message {} with JMS message id: {}", message.getMessageId(), message.getJMSMessageID(), e);
        }
    }

    private void dropMessages(ActiveMQDestination destination, List<String> messageIds) {
        try {
            Queue queue = broker.getDestinations(destination).stream()
                    .findFirst().map(DestinationExtractor::extractQueue).orElseThrow();
            queue.removeMatchingMessages(connectionContext, new ListMessageReferenceFilter(messageIds), messageIds.size());
        } catch (Exception e) {
            logger.error("Unable to replicate messages dropped [{}]", destination, e);
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
