package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;

public class ReplicaSourceBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);

    private final URI transportConnectorUri;

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
    }

    @Override
    public void start() throws Exception {
        super.start();
        logger.info("Replica plugin initialized");
    }
}
