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
import org.apache.activemq.broker.MutableBrokerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaRoleManagementBroker extends MutableBrokerFilter implements ActionListenerCallback {
    private final Logger logger = LoggerFactory.getLogger(ReplicaRoleManagementBroker.class);
    private final MutativeRoleBroker sourceBroker;
    private final MutativeRoleBroker replicaBroker;
    private ReplicaRole role;

    public ReplicaRoleManagementBroker(Broker broker, MutativeRoleBroker sourceBroker, MutativeRoleBroker replicaBroker,
            ReplicaRole role) {
        super(broker);
        this.sourceBroker = sourceBroker;
        this.replicaBroker = replicaBroker;
        this.role = role;
        if (role == ReplicaRole.source) {
            setNext(sourceBroker);
        } else if (role == ReplicaRole.replica) {
            setNext(replicaBroker);
        }
        logger.info("this is a broker initialization role: {}",this.role);
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
    public void onFailOverAck() {
        switchNext(replicaBroker, sourceBroker);
    }

    public void switchRole(ReplicaRole role, boolean force) {
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

    private void switchRole(ReplicaRole role) {
        if (this.role == ReplicaRole.source && role != ReplicaRole.replica) {
            return;
        }
        switchNext(sourceBroker);
    }

    private void forceSwitchRole(ReplicaRole role) {
        if (role == ReplicaRole.replica) {
            switchNext(sourceBroker, replicaBroker);
        } else if (role == ReplicaRole.source) {
            switchNext(replicaBroker, sourceBroker);
        }
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