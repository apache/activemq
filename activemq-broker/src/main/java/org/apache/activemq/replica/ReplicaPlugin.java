package org.apache.activemq.replica;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.broker.scheduler.SchedulerBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * A Broker plugin to replicate core messaging events from one broker to another.
 *
 * @org.apache.xbean.XBean element="replicaPlugin"
 */
public class ReplicaPlugin extends BrokerPluginSupport {

    private final Logger logger = LoggerFactory.getLogger(ReplicaPlugin.class);

    protected ReplicaRole role = ReplicaRole.source;
    protected ActiveMQConnectionFactory otherBrokerConnectionFactory = new ActiveMQConnectionFactory();
    protected URI transportConnectorUri = null;

    public ReplicaPlugin() {
        super();
    }

    @Override
    public Broker installPlugin(final Broker broker) {
        logger.info("{} installed, running as {}", ReplicaPlugin.class.getName(), role);
        Broker replicaBrokerFilter = createReplicaPluginBrokerFilter(broker);
        if (role == ReplicaRole.replica) {
            return replicaBrokerFilter;
        }
        final MutableBrokerFilter scheduledBroker = (MutableBrokerFilter) broker.getAdaptor(SchedulerBroker.class);
        if (scheduledBroker != null) {
            scheduledBroker.setNext(new ReplicaSchedulerSourceBroker(scheduledBroker.getNext()));
        }
        return replicaBrokerFilter;
    }

    private Broker createReplicaPluginBrokerFilter(Broker broker) {
        switch (role) {
            case replica:
                return new ReplicaBroker(broker, otherBrokerConnectionFactory);
            case source:
                return new ReplicaSourceBroker(broker, transportConnectorUri);
            case dual:
                return new ReplicaBroker(new ReplicaSourceBroker(broker, transportConnectorUri), otherBrokerConnectionFactory);
            default:
                throw new IllegalArgumentException("Unknown replica role:" + role);
        }
    }

    public ReplicaPlugin setRole(ReplicaRole role) {
        this.role = requireNonNull(role);
        return this;
    }

    public ReplicaPlugin connectedTo(URI uri) {
        this.setOtherBrokerUri(requireNonNull(uri).toString());
        return this;
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setRole(String role) {
        this.role = Arrays.stream(ReplicaRole.values())
            .filter(roleValue -> roleValue.name().equalsIgnoreCase(role))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(role + " is not a known " + ReplicaRole.class.getSimpleName()));
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setOtherBrokerUri(String uri) {
        otherBrokerConnectionFactory.setBrokerURL(uri); // once to validate
        otherBrokerConnectionFactory.setBrokerURL(
            uri.toLowerCase().startsWith("failover:(")
                ? uri
                : "failover:("+uri+")"
        );
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setTransportConnectorUri(String uri) {
        transportConnectorUri = URI.create(uri);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setUserName(String userName) {
        otherBrokerConnectionFactory.setUserName(userName);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setPassword(String password) {
        otherBrokerConnectionFactory.setPassword(password);
    }

    public ReplicaRole getRole() {
        return role;
    }
}
