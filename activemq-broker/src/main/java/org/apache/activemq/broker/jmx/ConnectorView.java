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

import org.apache.activemq.broker.Connector;
import org.apache.activemq.command.BrokerInfo;

public class ConnectorView implements ConnectorViewMBean {

    private final Connector connector;

    public ConnectorView(Connector connector) {
        this.connector = connector;
    }

    @Override
    public void start() throws Exception {
        connector.start();
    }

    public String getBrokerName() {
        return getBrokerInfo().getBrokerName();
    }

    @Override
    public void stop() throws Exception {
        connector.stop();
    }

    public String getBrokerURL() {
        return getBrokerInfo().getBrokerURL();
    }

    public BrokerInfo getBrokerInfo() {
        return connector.getBrokerInfo();
    }

    /**
     * Resets the statistics
     */
    @Override
    public void resetStatistics() {
        connector.getStatistics().reset();
    }

    /**
     * enable statistics gathering
     */
    @Override
    public void enableStatistics() {
        connector.getStatistics().setEnabled(true);
    }

    /**
     * disable statistics gathering
     */
    @Override
    public void disableStatistics() {
        connector.getStatistics().setEnabled(false);
    }

    /**
     * Returns true if statistics is enabled
     *
     * @return true if statistics is enabled
     */
    @Override
    public boolean isStatisticsEnabled() {
        return connector.getStatistics().isEnabled();
    }

    /**
     * Returns the number of current connections
     */
    @Override
    public int connectionCount() {
        return connector.connectionCount();
    }

    /**
     * Returns true if updating cluster client URL is enabled
     *
     * @return true if update cluster client URL is enabled
     */
    @Override
    public boolean isUpdateClusterClients() {
        return this.connector.isUpdateClusterClients();
    }

    /**
     * Returns true if rebalancing cluster clients is enabled
     *
     * @return true if rebalance cluster clients is enabled
     */
    @Override
    public boolean isRebalanceClusterClients() {
        return this.connector.isRebalanceClusterClients();
    }

    /**
     * Returns true if updating cluster client URL when brokers are removed is
     * enabled
     *
     * @return true if update cluster client URL when brokers are removed is
     *         enabled
     */
    @Override
    public boolean isUpdateClusterClientsOnRemove() {
        return this.connector.isUpdateClusterClientsOnRemove();
    }

    /**
     * @return The comma separated string of regex patterns to match broker
     *         names for cluster client updates
     */
    @Override
    public String getUpdateClusterFilter() {
        return this.connector.getUpdateClusterFilter();
    }

    @Override
    public boolean isAllowLinkStealingEnabled() {
        return this.connector.isAllowLinkStealing();
    }
}
