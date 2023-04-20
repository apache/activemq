package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaAdvisorySuppressor extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaAdvisorySuppressor.class);

    public ReplicaAdvisorySuppressor(Broker next) {
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
