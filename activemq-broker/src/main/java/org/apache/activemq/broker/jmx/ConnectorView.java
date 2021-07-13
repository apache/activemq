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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.region.ConnectorStatistics;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.PollCountStatisticImpl;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConnectorView implements ConnectorViewMBean {

    private final static ObjectMapper mapper = new ObjectMapper();
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
    
    /**
     * @return A JSON string of the connector statistics
     */
    @Override
    public String getStatistics() {
        return serializeConnectorStatistics();
    }
    
    private String serializeConnectorStatistics() {
    	ConnectorStatistics tmpConnectorStatistics = this.connector.getStatistics();
    	
        if (tmpConnectorStatistics != null) {
            try {
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("consumers", getCountStatisticsAsMap(tmpConnectorStatistics.getConsumers()));
                result.put("dequeues", getCountStatisticsAsMap(tmpConnectorStatistics.getDequeues()));
                result.put("enqueues", getCountStatisticsAsMap(tmpConnectorStatistics.getEnqueues()));
                result.put("messages", getCountStatisticsAsMap(tmpConnectorStatistics.getMessages()));
                result.put("messagesCached", getPollCountStatisticsAsMap(tmpConnectorStatistics.getMessagesCached()));
                return mapper.writeValueAsString(result);
            } catch (IOException e) {
                return e.toString();
            }
        }

        return null;
    }
    
    private Map<String, Object> getPollCountStatisticsAsMap(final PollCountStatisticImpl pollCountStatistic) {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("count", pollCountStatistic.getCount());
        result.put("description", pollCountStatistic.getDescription());
        result.put("frequency", pollCountStatistic.getFrequency());
        result.put("lastSampleTime", pollCountStatistic.getLastSampleTime());
        result.put("period", pollCountStatistic.getPeriod());
        result.put("startTime", pollCountStatistic.getStartTime());
        result.put("unit", pollCountStatistic.getUnit());
        return result;
    }

    private Map<String, Object> getCountStatisticsAsMap(final CountStatisticImpl countStatistic) {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("count", countStatistic.getCount());
        result.put("description", countStatistic.getDescription());
        result.put("frequency", countStatistic.getFrequency());
        result.put("lastSampleTime", countStatistic.getLastSampleTime());
        result.put("period", countStatistic.getPeriod());
        result.put("startTime", countStatistic.getStartTime());
        result.put("unit", countStatistic.getUnit());
        return result;
    }
}
