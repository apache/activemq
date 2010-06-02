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
package org.apache.activemq.network;

import java.util.List;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Configuration for a NetworkBridge
 * 
 * @version $Revision: 1.1 $
 */
public class NetworkBridgeConfiguration {

    private boolean conduitSubscriptions = true;
    private boolean dynamicOnly;
    private boolean dispatchAsync = true;
    private boolean decreaseNetworkConsumerPriority;
    private boolean duplex;
    private boolean bridgeTempDestinations = true;
    private int prefetchSize = 1000;
    private int networkTTL = 1;
    private String brokerName = "localhost";
    private String brokerURL = "";
    private String userName;
    private String password;
    private String destinationFilter = ">";
    private String name = null;
    
    private List<ActiveMQDestination> excludedDestinations;
    private List<ActiveMQDestination> dynamicallyIncludedDestinations;
    private List<ActiveMQDestination> staticallyIncludedDestinations;

    private boolean suppressDuplicateQueueSubscriptions = false;
    private boolean suppressDuplicateTopicSubscriptions = true;


    /**
     * @return the conduitSubscriptions
     */
    public boolean isConduitSubscriptions() {
        return this.conduitSubscriptions;
    }

    /**
     * @param conduitSubscriptions the conduitSubscriptions to set
     */
    public void setConduitSubscriptions(boolean conduitSubscriptions) {
        this.conduitSubscriptions = conduitSubscriptions;
    }

    /**
     * @return the dynamicOnly
     */
    public boolean isDynamicOnly() {
        return this.dynamicOnly;
    }

    /**
     * @param dynamicOnly the dynamicOnly to set
     */
    public void setDynamicOnly(boolean dynamicOnly) {
        this.dynamicOnly = dynamicOnly;
    }

    /**
     * @return the bridgeTempDestinations
     */
    public boolean isBridgeTempDestinations() {
        return this.bridgeTempDestinations;
    }

    /**
     * @param bridgeTempDestinations the bridgeTempDestinations to set
     */
    public void setBridgeTempDestinations(boolean bridgeTempDestinations) {
        this.bridgeTempDestinations = bridgeTempDestinations;
    }

    /**
     * @return the decreaseNetworkConsumerPriority
     */
    public boolean isDecreaseNetworkConsumerPriority() {
        return this.decreaseNetworkConsumerPriority;
    }

    /**
     * @param decreaseNetworkConsumerPriority the
     *                decreaseNetworkConsumerPriority to set
     */
    public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority) {
        this.decreaseNetworkConsumerPriority = decreaseNetworkConsumerPriority;
    }

    /**
     * @return the dispatchAsync
     */
    public boolean isDispatchAsync() {
        return this.dispatchAsync;
    }

    /**
     * @param dispatchAsync the dispatchAsync to set
     */
    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    /**
     * @return the duplex
     */
    public boolean isDuplex() {
        return this.duplex;
    }

    /**
     * @param duplex the duplex to set
     */
    public void setDuplex(boolean duplex) {
        this.duplex = duplex;
    }

    /**
     * @return the brokerName
     */
    public String getBrokerName() {
        return this.brokerName;
    }

    /**
     * @param brokerName the localBrokerName to set
     */
    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /**
     * @return the networkTTL
     */
    public int getNetworkTTL() {
        return this.networkTTL;
    }

    /**
     * @param networkTTL the networkTTL to set
     */
    public void setNetworkTTL(int networkTTL) {
        this.networkTTL = networkTTL;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the prefetchSize
     */
    public int getPrefetchSize() {
        return this.prefetchSize;
    }

    /**
     * @param prefetchSize the prefetchSize to set
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryIntPropertyEditor"
     */
    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    /**
     * @return the userName
     */
    public String getUserName() {
        return this.userName;
    }

    /**
     * @param userName the userName to set
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return the destinationFilter
     */
    public String getDestinationFilter() {
        return this.destinationFilter;
    }

    /**
     * @param destinationFilter the destinationFilter to set
     */
    public void setDestinationFilter(String destinationFilter) {
        this.destinationFilter = destinationFilter;
    }

    /**
     * @return the name
     */
    public String getName() {
        if(this.name == null) {
            this.name = "localhost";
        }
        return this.name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

	public List<ActiveMQDestination> getExcludedDestinations() {
		return excludedDestinations;
	}

	public void setExcludedDestinations(
			List<ActiveMQDestination> excludedDestinations) {
		this.excludedDestinations = excludedDestinations;
	}

	public List<ActiveMQDestination> getDynamicallyIncludedDestinations() {
		return dynamicallyIncludedDestinations;
	}

	public void setDynamicallyIncludedDestinations(
			List<ActiveMQDestination> dynamicallyIncludedDestinations) {
		this.dynamicallyIncludedDestinations = dynamicallyIncludedDestinations;
	}

	public List<ActiveMQDestination> getStaticallyIncludedDestinations() {
		return staticallyIncludedDestinations;
	}

	public void setStaticallyIncludedDestinations(
			List<ActiveMQDestination> staticallyIncludedDestinations) {
		this.staticallyIncludedDestinations = staticallyIncludedDestinations;
	}
	
    

    public boolean isSuppressDuplicateQueueSubscriptions() {
        return suppressDuplicateQueueSubscriptions;
    }
    
    /**
     * 
     * @param val if true, duplicate network queue subscriptions (in a cyclic network) will be suppressed
     */
    public void setSuppressDuplicateQueueSubscriptions(boolean val) {
        suppressDuplicateQueueSubscriptions = val;
    }

    public boolean isSuppressDuplicateTopicSubscriptions() {
        return suppressDuplicateTopicSubscriptions;
    }

    /**
     * 
     * @param val if true, duplicate network topic subscriptions (in a cyclic network) will be suppressed
     */
    public void setSuppressDuplicateTopicSubscriptions(boolean val) {
        suppressDuplicateTopicSubscriptions  = val;
    }
    
    /**
     * @return the brokerURL
     */
    public String getBrokerURL() {
        return this.brokerURL;
    }

    /**
     * @param brokerURL the brokerURL to set
     */
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }
}
