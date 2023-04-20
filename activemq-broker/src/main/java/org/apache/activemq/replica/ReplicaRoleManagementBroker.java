/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.replica.storage.ReplicaFailOverStateStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaRoleManagementBroker extends MutableBrokerFilter implements ActionListenerCallback {
    private static final String FAIL_OVER_CONSUMER_CLIENT_ID = "DUMMY_FAIL_OVER_CONSUMER";

    private final Logger logger = LoggerFactory.getLogger(ReplicaRoleManagementBroker.class);
    private final MutativeRoleBroker sourceBroker;
    private final MutativeRoleBroker replicaBroker;
    private final ReplicaFailOverStateStorage replicaFailOverStateStorage;
    private ReplicaRole role;
    private ConnectionContext connectionContext;

    public ReplicaRoleManagementBroker(Broker broker, MutativeRoleBroker sourceBroker, MutativeRoleBroker replicaBroker,  ReplicaFailOverStateStorage replicaFailOverStateStorage, ReplicaRole role) {
        super(broker);
        this.sourceBroker = sourceBroker;
        this.replicaBroker = replicaBroker;
        this.replicaFailOverStateStorage = replicaFailOverStateStorage;
        this.role = role;
    }

    @Override
    public void start() throws Exception {
        super.start();
        initializeFailOverQueue();
        ReplicaRole brokerFailOverState = Optional.ofNullable(replicaFailOverStateStorage.getBrokerState()).orElse(role);

        switch (brokerFailOverState) {
            case await_ack:
                startBroker(sourceBroker, ReplicaRole.source);
                sourceBroker.stopBeforeRoleChange(false);
                break;
            case replica:
                startBroker(replicaBroker, ReplicaRole.replica);
                break;
            case source:
                startBroker(sourceBroker, ReplicaRole.source);
                break;
        }
    }

    private void startBroker(MutativeRoleBroker broker, ReplicaRole role) throws Exception {
        setNext(broker);
        broker.start();
        this.role = role;
    }

    @Override
    public void onDeinitializationSuccess() {
        try {
            if (replicaBroker.isStopped()) {
                replicaBroker.start();
            } else {
                replicaBroker.startAfterRoleChange();
            }

            setNext(replicaBroker);
        } catch (Exception e) {
            logger.error("Failed to switch role", e);
            throw new RuntimeException("Failed to switch role", e);
        }
    }

    @Override
    public void onFailOverAck() throws Exception {
        forceSwitchRole(ReplicaRole.source);
    }

    public void switchRole(ReplicaRole role, boolean force) throws Exception {
        if (this.role == role) {
            return;
        }

        if (force) {
            forceSwitchRole(role);
        } else {
            switchRole(role);
        }

        this.role = role;
    }

    private void initializeFailOverQueue() throws Exception {
        ReplicaInternalMessageProducer replicaInternalMessageProducer = new ReplicaInternalMessageProducer(getNext(), getNext().getAdminConnectionContext());
        connectionContext = getNext().getAdminConnectionContext().copy();
        connectionContext.setClientId(FAIL_OVER_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }
        this.replicaFailOverStateStorage.initialize(getNext(), connectionContext, replicaInternalMessageProducer);

    }

    private void saveBrokerRoleState(ReplicaRole role) throws Exception {
        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        getNext().beginTransaction(connectionContext, tid);
        try {
            replicaFailOverStateStorage.updateBrokerState(connectionContext, tid, role.name());
            this.role = role;

            getNext().commitTransaction(connectionContext, tid, true);
        } catch (Exception e) {
            getNext().rollbackTransaction(connectionContext, tid);
            logger.error("Failed to send broker fail over state", e);
            throw e;
        }
    }

    private void switchRole(ReplicaRole role) {
        if (this.role == ReplicaRole.source && role != ReplicaRole.replica) {
            return;
        }
        switchNext(sourceBroker);
    }

    private void forceSwitchRole(ReplicaRole role) throws Exception {
        if (role == ReplicaRole.replica) {
            switchNext(sourceBroker, replicaBroker);
        } else if (role == ReplicaRole.source) {
            switchNext(replicaBroker, sourceBroker);
        }
        saveBrokerRoleState(role);
    }

    private void switchNext(MutativeRoleBroker oldNext, MutativeRoleBroker newNext) {
        try {
            oldNext.stopBeforeRoleChange(true);
            if (newNext.isStopped()) {
                newNext.start();
            } else {
                newNext.startAfterRoleChange();
            }
            setNext(newNext);
        } catch (Exception e) {
            logger.error("Failed to switch role", e);
            throw new RuntimeException("Failed to switch role", e);
        }
    }

    private void switchNext(MutativeRoleBroker oldNext) {
        try {
            oldNext.stopBeforeRoleChange(false);
        } catch (Exception e) {
            logger.error("Failed to switch role", e);
            throw new RuntimeException("Failed to switch role", e);
        }
    }
}