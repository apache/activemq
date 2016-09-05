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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.network.NetworkConnector;

public class NetworkConnectorView implements NetworkConnectorViewMBean {

    private final NetworkConnector connector;

    public NetworkConnectorView(NetworkConnector connector) {
        this.connector = connector;
    }

    @Override
    public void start() throws Exception {
        connector.start();
    }

    @Override
    public void stop() throws Exception {
        connector.stop();
    }

    @Override
    public String getName() {
        return connector.getName();
    }

    @Override
    public int getMessageTTL() {
        return connector.getMessageTTL();
    }

    @Override
    public int getConsumerTTL() {
        return connector.getConsumerTTL();
    }

    @Override
    public int getPrefetchSize() {
        return connector.getPrefetchSize();
    }

    @Override
    public int getAdvisoryPrefetchSize() {
        return connector.getAdvisoryPrefetchSize();
    }

    @Override
    public String getUserName() {
        return connector.getUserName();
    }

    @Override
    public boolean isBridgeTempDestinations() {
        return connector.isBridgeTempDestinations();
    }

    @Override
    public boolean isConduitSubscriptions() {
        return connector.isConduitSubscriptions();
    }

    @Override
    public boolean isDecreaseNetworkConsumerPriority() {
        return connector.isDecreaseNetworkConsumerPriority();
    }

    @Override
    public boolean isDispatchAsync() {
        return connector.isDispatchAsync();
    }

    @Override
    public boolean isDynamicOnly() {
        return connector.isDynamicOnly();
    }

    @Override
    public boolean isDuplex() {
        return connector.isDuplex();
    }

    @Override
    public boolean isSuppressDuplicateQueueSubscriptions() {
        return connector.isSuppressDuplicateQueueSubscriptions();
    }

    @Override
    public boolean isSuppressDuplicateTopicSubscriptions() {
        return connector.isSuppressDuplicateTopicSubscriptions();
    }

    @Override
    public void setBridgeTempDestinations(boolean bridgeTempDestinations) {
        connector.setBridgeTempDestinations(bridgeTempDestinations);
    }

    @Override
    public void setConduitSubscriptions(boolean conduitSubscriptions) {
        connector.setConduitSubscriptions(conduitSubscriptions);
    }

    @Override
    public void setDispatchAsync(boolean dispatchAsync) {
        connector.setDispatchAsync(dispatchAsync);
    }

    @Override
    public void setDynamicOnly(boolean dynamicOnly) {
        connector.setDynamicOnly(dynamicOnly);
    }

    @Override
    public void setMessageTTL(int messageTTL) {
        connector.setMessageTTL(messageTTL);
    }

    @Override
    public void setConsumerTTL(int consumerTTL) {
        connector.setConsumerTTL(consumerTTL);
    }

    @Override
    public void setPassword(String password) {
        connector.setPassword(password);
    }

    @Override
    public void setPrefetchSize(int prefetchSize) {
        connector.setPrefetchSize(prefetchSize);
    }

    @Override
    public void setAdvisoryPrefetchSize(int advisoryPrefetchSize) {
        connector.setAdvisoryPrefetchSize(advisoryPrefetchSize);
    }

    @Override
    public void setUserName(String userName) {
        connector.setUserName(userName);
    }

    @Override
    public String getPassword() {
        String pw = connector.getPassword();
        // Hide the password for security reasons.
        if (pw != null) {
            pw = pw.replaceAll(".", "*");
        }
        return pw;
    }

    @Override
    public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority) {
        connector.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
    }

    @Override
    public void setSuppressDuplicateQueueSubscriptions(boolean val) {
        connector.setSuppressDuplicateQueueSubscriptions(val);
    }

    @Override
    public void setSuppressDuplicateTopicSubscriptions(boolean val) {
        connector.setSuppressDuplicateTopicSubscriptions(val);
    }
}
