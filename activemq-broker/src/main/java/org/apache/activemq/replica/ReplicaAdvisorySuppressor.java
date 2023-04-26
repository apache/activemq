package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaAdvisorySuppressor implements DestinationInterceptor {

    private final Logger logger = LoggerFactory.getLogger(ReplicaAdvisorySuppressor.class);

    @Override
    public Destination intercept(Destination destination) {
        if (AdvisorySupport.isAdvisoryTopic(destination.getActiveMQDestination())) {
            return new ReplicaAdvisorySuppressionFilter(destination);
        }
        return destination;
    }

    @Override
    public void remove(Destination destination) {
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
    }

    private static class ReplicaAdvisorySuppressionFilter extends DestinationFilter {

        public ReplicaAdvisorySuppressionFilter(Destination next) {
            super(next);
        }

        @Override
        public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
            if (messageSend.isAdvisory()) {
                if (messageSend.getDestination().getPhysicalName().contains(ReplicaSupport.REPLICATION_QUEUE_PREFIX)) {
                    // NoB relies on advisory messages for AddConsumer.
                    // Suppress these messages for replication queues so that the replication queues are ignored by NoB.
                    return;
                }
            }
            super.send(producerExchange, messageSend);
        }
    }
}
