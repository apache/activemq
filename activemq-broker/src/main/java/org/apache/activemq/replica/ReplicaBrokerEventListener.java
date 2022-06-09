package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageListener;

import static java.util.Objects.requireNonNull;

public class ReplicaBrokerEventListener implements MessageListener {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBrokerEventListener.class);
    private final Broker broker;

    ReplicaBrokerEventListener(Broker broker) {
        this.broker = requireNonNull(broker);
    }

    @Override
    public void onMessage(Message message) {
        logger.trace("Received replication message from replica source");
    }
}
