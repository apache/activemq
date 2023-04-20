package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaRoleManagementBroker extends MutableBrokerFilter {
    private final Logger logger = LoggerFactory.getLogger(ReplicaRoleManagementBroker.class);
    private final ReplicaSourceAuthorizationBroker sourceBroker;
    private final ReplicaBroker replicaBroker;
    private ReplicaRole role;

    public ReplicaRoleManagementBroker(Broker broker, ReplicaSourceAuthorizationBroker sourceBroker, ReplicaBroker replicaBroker, ReplicaRole role) {
        super(broker);
        this.sourceBroker = sourceBroker;
        this.replicaBroker = replicaBroker;
        this.role = role;
        if (role == ReplicaRole.source) {
            setNext(sourceBroker);
        } else if (role == ReplicaRole.replica) {
            setNext(replicaBroker);
        }
    }

    public void switchRole(ReplicaRole role, boolean force) {
        if (this.role == role) {
            return;
        }

        if (force) {
            switchRoleForce(role);
        } else {
            switchRoleSoft(role);
        }

        this.role = role;
    }

    private void switchRoleSoft(ReplicaRole role) {
        // TODO
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private void switchRoleForce(ReplicaRole role) {
        if (role == ReplicaRole.replica) {
            switchNext(sourceBroker, replicaBroker);
        } else if (role == ReplicaRole.source) {
            switchNext(replicaBroker, sourceBroker);
        }
    }

    private void switchNext(Broker oldNext, Broker newNext) {
        try {
            ((MutativeRoleBroker) oldNext).stopBeforeRoleChange();
            if (newNext.isStopped()) {
                newNext.start();
            } else {
                ((MutativeRoleBroker) newNext).startAfterRoleChange();
            }
            setNext(newNext);
        } catch (Exception e) {
            logger.error("Failed to switch role", e);
            throw new RuntimeException("Failed to switch role", e);
        }
    }
}