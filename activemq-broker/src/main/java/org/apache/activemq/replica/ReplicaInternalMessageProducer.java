package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;

import static java.util.Objects.requireNonNull;

public class ReplicaInternalMessageProducer {

    private final Broker broker;
    private final ConnectionContext connectionContext;

    ReplicaInternalMessageProducer(final Broker broker, final ConnectionContext connectionContext) {
        this.broker = requireNonNull(broker);
        this.connectionContext = requireNonNull(connectionContext);
    }

    void produceToReplicaQueue(final ActiveMQMessage eventMessage) throws Exception {
        final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
        producerExchange.setConnectionContext(connectionContext);
        producerExchange.setMutable(true);
        producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
        sendIgnoringFlowControl(eventMessage, producerExchange);
    }

    private void sendIgnoringFlowControl(ActiveMQMessage eventMessage, ProducerBrokerExchange producerExchange) throws Exception {
        boolean originalFlowControl = connectionContext.isProducerFlowControl();
        try {
            connectionContext.setProducerFlowControl(false);
            broker.send(producerExchange, eventMessage);
        } finally {
            connectionContext.setProducerFlowControl(originalFlowControl);
        }
    }

}
