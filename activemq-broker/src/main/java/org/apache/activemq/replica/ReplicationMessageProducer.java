package org.apache.activemq.replica;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ReplicationMessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationMessageProducer.class);

    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final Object sendingMutex = new Object();
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    ReplicationMessageProducer(ReplicaInternalMessageProducer replicaInternalMessageProducer, ReplicaReplicationQueueSupplier queueProvider) {
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.queueProvider = queueProvider;
        replicationProducerId.setConnectionId(idGenerator.generateId());
    }

    void enqueueReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        synchronized (sendingMutex) {
            logger.debug("Replicating {} event", event.getEventType());
            logger.trace("Replicating {} event: data:\n{}\nproperties:{}", event.getEventType(), new Object() {
                @Override
                public String toString() {
                    try {
                        return eventSerializer.deserializeMessageData(event.getEventData()).toString();
                    } catch (IOException e) {
                        return "<some event data>";
                    }
                }
            }, event.getReplicationProperties()); // FIXME: remove
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
            eventMessage.setTransactionId(event.getTransactionId());
            replicaInternalMessageProducer.produceToReplicaQueue(connectionContext, eventMessage);
        }
    }

}

