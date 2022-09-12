package org.apache.activemq.replica;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaSchedulerSourceBroker extends ReplicaSourceBaseBroker {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSchedulerSourceBroker.class);

    public ReplicaSchedulerSourceBroker(Broker next) {
        super(next);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ActiveMQDestination destination = messageSend.getDestination();
        final String jobId = (String) messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
        if (jobId != null) {
            replicateSend(producerExchange.getConnectionContext(), messageSend, destination);
        }
        super.send(producerExchange, messageSend);
    }


    private void replicateSend(ConnectionContext connectionContext, Message message, ActiveMQDestination destination) {
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate scheduled message {} for destination {}", message.getMessageId(), destination.getPhysicalName());
        }
    }

}
