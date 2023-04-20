package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;

public class ReplicaDestinationInterceptor implements DestinationInterceptor {

    private final ReplicaSourceBroker sourceBroker;
    private final ReplicaRoleManagementBroker roleManagementBroker;

    public ReplicaDestinationInterceptor(ReplicaSourceBroker sourceBroker, ReplicaRoleManagementBroker roleManagementBroker) {
        this.sourceBroker = sourceBroker;
        this.roleManagementBroker = roleManagementBroker;
    }

    @Override
    public Destination intercept(Destination destination) {
        return new ReplicaDestinationFilter(destination, sourceBroker, roleManagementBroker);
    }

    @Override
    public void remove(Destination destination) {
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
    }
}