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
    public boolean  isUpdateClusterClientsOnRemove();

    int connectionCount();

    /**
     * If enabled, older connections with the same clientID are stopped
     * @return true/false if link stealing is enabled
     */
    boolean isAllowLinkStealing();
    
    /**
     * @return The comma separated string of regex patterns to match 
     * broker names for cluster client updates
     */
    String getUpdateClusterFilter();
}
