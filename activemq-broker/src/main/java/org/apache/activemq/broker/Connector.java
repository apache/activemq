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
package org.apache.activemq.broker;

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.ConnectorStatistics;
import org.apache.activemq.command.BrokerInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A connector creates and manages client connections that talk to the Broker.
 * 
 * 
 */
public interface Connector extends Service {

    /**
     * @return brokerInfo
     */
    BrokerInfo getBrokerInfo();

    /**
     * @return the statistics for this connector
     */
    ConnectorStatistics getStatistics();

    /**
     * Reset Connector statistics
     */
    void resetStatistics();

    /**
     * @return true if update client connections when brokers leave/join a cluster
     */
    public boolean isUpdateClusterClients();

    /**
     * @return true if clients should be re-balanced across the cluster
     */
    public boolean isRebalanceClusterClients();

    /**
     * Update all the connections with information
     * about the connected brokers in the cluster
     */
    public void updateClientClusterInfo();

    /**
     * @return true if clients should be updated when
     * a broker is removed from a broker
     */
    public boolean isUpdateClusterClientsOnRemove();

    @Deprecated(forRemoval = true)
    int connectionCount();

    /**
     * If enabled, older connections with the same clientID are stopped
     *
     * @return true/false if link stealing is enabled
     */
    boolean isAllowLinkStealing();

    /**
     * @return The comma separated string of regex patterns to match
     * broker names for cluster client updates
     */
    String getUpdateClusterFilter();

    long getMaxConnectionExceededCount();

    /** @return the configured remote address allow list, comma separated CIDRs or a file: URI, or null */
    String getAllowList();

    /** @return the configured remote address deny list, comma separated CIDRs or a file: URI, or null */
    String getDenyList();

    /** @return true if remote addresses are checked against the allow and deny lists */
    boolean isAllowDenyValidationEnabled();

    /** Turn the remote address check on or off at runtime; the lists stay configured */
    void setAllowDenyValidationEnabled(boolean enabled);

    /** @return connections accepted by the remote address check since the last statistics reset */
    long getAllowedCount();

    /** @return connections refused by the remote address check since the last statistics reset */
    long getDeniedCount();

    /** @return number of valid CIDR entries loaded into the allow list */
    long getAllowListCount();

    /** @return number of valid CIDR entries loaded into the deny list */
    long getDenyListCount();

    /** @return number of allow list entries skipped because they were not valid CIDR blocks */
    long getAllowListInvalidCount();

    /** @return number of deny list entries skipped because they were not valid CIDR blocks */
    long getDenyListInvalidCount();

    /**
     * Runs an IP literal or a CIDR block through the connector's allow/deny
     * decision, ignoring whether enforcement is enabled. An address gets the exact
     * decision a connection would; a block is allowed when an allow entry covers it
     * and no deny entry covers the whole block.
     */
    boolean allowed(String addressOrCidr);

    boolean isAutoStart();

    /**
     * @return true if connector is started
     */
    public boolean isStarted();

    public URI getConnectUri() throws IOException, URISyntaxException;

    public URI getPublishableConnectURI() throws Exception;

    public boolean isEnableStatusMonitor();

    public URI getUri();

    public URI getDiscoveryUri();

    public boolean isAuditNetworkProducers();

    public int getMaximumProducersAllowedPerConnection();

    public int getMaximumConsumersAllowedPerConnection();

    public int getConnectionCount();

    /**
     * @return connector name
     */
    public String getName();
}
