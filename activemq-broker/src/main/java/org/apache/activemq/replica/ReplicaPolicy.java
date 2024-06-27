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

import org.apache.activemq.ActiveMQConnectionFactory;

import java.net.URI;
import java.util.Objects;


public class ReplicaPolicy {

    private final ActiveMQConnectionFactory otherBrokerConnectionFactory = new ActiveMQConnectionFactory();
    private URI transportConnectorUri = null;

    private int sourceSendPeriod = 5_000;
    private int compactorAdditionalMessagesLimit = 10_000;
    private int maxBatchLength = 500;
    private int maxBatchSize = 5_000_000;
    private int replicaAckPeriod = 5_000;
    private int replicaMaxAckBatchSize = 100;
    private boolean controlWebConsoleAccess = true;
    private boolean hideReplicationDestination = true;

    private int heartBeatPeriod = 60_000;

    private boolean sourceReplicationFlowControl = true;
    private boolean replicaReplicationFlowControl = true;

    public URI getTransportConnectorUri() {
        return Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
    }

    public void setTransportConnectorUri(URI uri) {
        transportConnectorUri = uri;
    }

    public ActiveMQConnectionFactory getOtherBrokerConnectionFactory() {
        Objects.requireNonNull(otherBrokerConnectionFactory, "Need connection details of replica source for this broker");
        Objects.requireNonNull(otherBrokerConnectionFactory.getBrokerURL(), "Need connection URI of replica source for this broker");
        validateUser(otherBrokerConnectionFactory);
        return otherBrokerConnectionFactory;
    }

    public void setOtherBrokerUri(String uri) {
        otherBrokerConnectionFactory.setBrokerURL(uri); // once to validate
        otherBrokerConnectionFactory.setBrokerURL(
                uri.toLowerCase().startsWith("failover:(")
                        ? uri
                        : "failover:("+ uri +")"
        );
    }

    public void setUserName(String userName) {
        otherBrokerConnectionFactory.setUserName(userName);
    }

    public void setPassword(String password) {
        otherBrokerConnectionFactory.setPassword(password);
    }

    public int getSourceSendPeriod() {
        return sourceSendPeriod;
    }

    public void setSourceSendPeriod(int period) {
        sourceSendPeriod = period;
    }

    public int getCompactorAdditionalMessagesLimit() {
        return compactorAdditionalMessagesLimit;
    }

    public void setCompactorAdditionalMessagesLimit(int limit) {
        compactorAdditionalMessagesLimit = limit;
    }

    public int getMaxBatchLength() {
        return maxBatchLength;
    }

    public void setMaxBatchLength(int maxBatchLength) {
        this.maxBatchLength = maxBatchLength;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getReplicaAckPeriod() {
        return replicaAckPeriod;
    }

    public void setReplicaAckPeriod(int period) {
        replicaAckPeriod = period;
    }

    public int getReplicaMaxAckBatchSize() {
        return replicaMaxAckBatchSize;
    }

    public void setReplicaMaxAckBatchSize(int replicaMaxAckBatchSize) {
        this.replicaMaxAckBatchSize = replicaMaxAckBatchSize;
    }

    public boolean isControlWebConsoleAccess() {
        return controlWebConsoleAccess;
    }

    public void setControlWebConsoleAccess(boolean controlWebConsoleAccess) {
        this.controlWebConsoleAccess = controlWebConsoleAccess;
    }

    public boolean isHideReplicationDestination() {
        return hideReplicationDestination;
    }

    public void setHideReplicationDestination(boolean hideReplicationDestination) {
        this.hideReplicationDestination = hideReplicationDestination;
    }

    public int getHeartBeatPeriod() {
        return heartBeatPeriod;
    }

    public void setHeartBeatPeriod(int heartBeatPeriod) {
        this.heartBeatPeriod = heartBeatPeriod;
    }

    public boolean isSourceReplicationFlowControl() {
        return sourceReplicationFlowControl;
    }

    public void setSourceReplicationFlowControl(boolean enableSourceReplicationFlowControl) {
        this.sourceReplicationFlowControl = enableSourceReplicationFlowControl;
    }

    public boolean isReplicaReplicationFlowControl() {
        return replicaReplicationFlowControl;
    }

    public void setReplicaReplicationFlowControl(boolean enableReplicaReplicationFlowControl) {
        this.replicaReplicationFlowControl = enableReplicaReplicationFlowControl;
    }

    private void validateUser(ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        if (replicaSourceConnectionFactory.getUserName() != null) {
            Objects.requireNonNull(replicaSourceConnectionFactory.getPassword(), "Both userName and password or none of them should be configured for replica broker");
        }
        if (replicaSourceConnectionFactory.getPassword() != null) {
            Objects.requireNonNull(replicaSourceConnectionFactory.getUserName(), "Both userName and password or none of them should be configured for replica broker");
        }
    }
}
