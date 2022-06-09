package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;

public class ReplicaSourceBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    static final String REPLICATION_CONNECTOR_NAME = "replication";

    final ReplicaReplicationQueueSupplier queueProvider;
    private final URI transportConnectorUri;

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(transportConnectorUri);
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);

        queueProvider.initialize();
        logger.info("Replica plugin initialized with queue {}", queueProvider.get());

        super.start();
    }
}
