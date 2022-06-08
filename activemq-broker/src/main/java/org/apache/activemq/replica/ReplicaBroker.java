package org.apache.activemq.replica;

import static java.util.Objects.requireNonNull;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBroker.class);
    private final ActiveMQConnectionFactory replicaSourceConnectionFactory;

    public ReplicaBroker(final Broker next, final ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        super(next);
        this.replicaSourceConnectionFactory = requireNonNull(replicaSourceConnectionFactory, "Need connection details of replica source for this broker");
        requireNonNull(replicaSourceConnectionFactory.getBrokerURL(), "Need connection URI of replica source for this broker");
        validateUser(replicaSourceConnectionFactory);
    }

    private void validateUser(ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        if (replicaSourceConnectionFactory.getUserName() != null) {
            requireNonNull(replicaSourceConnectionFactory.getPassword(), "Both userName and password or none of them should be configured for replica broker");
        }
        if (replicaSourceConnectionFactory.getPassword() != null) {
            requireNonNull(replicaSourceConnectionFactory.getUserName(), "Both userName and password or none of them should be configured for replica broker");
        }
    }

    @Override
    public void start() throws Exception {
        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

}
