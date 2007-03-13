/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.network;

/**
 * Configuration for a NetworkBridge
 * 
 * @version $Revision: 1.1 $
 */
public class NetworkBridgeConfiguration{

    private boolean conduitSubscriptions=true;
    private boolean dynamicOnly=false;
    private boolean dispatchAsync=true;
    private boolean decreaseNetworkConsumerPriority=false;
    private boolean duplex=false;
    private boolean bridgeTempDestinations=true;
    private int prefetchSize=1000;
    private int networkTTL=1;
    private String localBrokerName="Unknow";
    private String userName;
    private String password;
    private String destinationFilter = ">";

    /**
     * @return the conduitSubscriptions
     */
    public boolean isConduitSubscriptions(){
        return this.conduitSubscriptions;
    }

    /**
     * @param conduitSubscriptions the conduitSubscriptions to set
     */
    public void setConduitSubscriptions(boolean conduitSubscriptions){
        this.conduitSubscriptions=conduitSubscriptions;
    }

    /**
     * @return the dynamicOnly
     */
    public boolean isDynamicOnly(){
        return this.dynamicOnly;
    }

    /**
     * @param dynamicOnly the dynamicOnly to set
     */
    public void setDynamicOnly(boolean dynamicOnly){
        this.dynamicOnly=dynamicOnly;
    }

    
    /**
     * @return the bridgeTempDestinations
     */
    public boolean isBridgeTempDestinations(){
        return this.bridgeTempDestinations;
    }

    
    /**
     * @param bridgeTempDestinations the bridgeTempDestinations to set
     */
    public void setBridgeTempDestinations(boolean bridgeTempDestinations){
        this.bridgeTempDestinations=bridgeTempDestinations;
    }

    
    /**
     * @return the decreaseNetworkConsumerPriority
     */
    public boolean isDecreaseNetworkConsumerPriority(){
        return this.decreaseNetworkConsumerPriority;
    }

    
    /**
     * @param decreaseNetworkConsumerPriority the decreaseNetworkConsumerPriority to set
     */
    public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority){
        this.decreaseNetworkConsumerPriority=decreaseNetworkConsumerPriority;
    }

    
    /**
     * @return the dispatchAsync
     */
    public boolean isDispatchAsync(){
        return this.dispatchAsync;
    }

    
    /**
     * @param dispatchAsync the dispatchAsync to set
     */
    public void setDispatchAsync(boolean dispatchAsync){
        this.dispatchAsync=dispatchAsync;
    }

    
    /**
     * @return the duplex
     */
    public boolean isDuplex(){
        return this.duplex;
    }

    
    /**
     * @param duplex the duplex to set
     */
    public void setDuplex(boolean duplex){
        this.duplex=duplex;
    }

    
    /**
     * @return the localBrokerName
     */
    public String getLocalBrokerName(){
        return this.localBrokerName;
    }

    
    /**
     * @param localBrokerName the localBrokerName to set
     */
    public void setLocalBrokerName(String localBrokerName){
        this.localBrokerName=localBrokerName;
    }

    
    /**
     * @return the networkTTL
     */
    public int getNetworkTTL(){
        return this.networkTTL;
    }

    
    /**
     * @param networkTTL the networkTTL to set
     */
    public void setNetworkTTL(int networkTTL){
        this.networkTTL=networkTTL;
    }

    
    /**
     * @return the password
     */
    public String getPassword(){
        return this.password;
    }

    
    /**
     * @param password the password to set
     */
    public void setPassword(String password){
        this.password=password;
    }

    
    /**
     * @return the prefetchSize
     */
    public int getPrefetchSize(){
        return this.prefetchSize;
    }

    
    /**
     * @param prefetchSize the prefetchSize to set
     */
    public void setPrefetchSize(int prefetchSize){
        this.prefetchSize=prefetchSize;
    }

    
    /**
     * @return the userName
     */
    public String getUserName(){
        return this.userName;
    }

    
    /**
     * @param userName the userName to set
     */
    public void setUserName(String userName){
        this.userName=userName;
    }

    
    /**
     * @return the destinationFilter
     */
    public String getDestinationFilter(){
        return this.destinationFilter;
    }

    
    /**
     * @param destinationFilter the destinationFilter to set
     */
    public void setDestinationFilter(String destinationFilter){
        this.destinationFilter=destinationFilter;
    }
}
