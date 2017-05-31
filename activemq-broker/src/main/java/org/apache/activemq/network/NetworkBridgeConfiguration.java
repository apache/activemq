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
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;

/**
 * Configuration for a NetworkBridge
 */
public class NetworkBridgeConfiguration {

    private boolean conduitSubscriptions = true;
    private boolean useVirtualDestSubs;
    private boolean dynamicOnly;
    private boolean syncDurableSubs;
    private boolean dispatchAsync = true;
    private boolean decreaseNetworkConsumerPriority;
    private int consumerPriorityBase = ConsumerInfo.NETWORK_CONSUMER_PRIORITY;
    private boolean duplex;
    private boolean bridgeTempDestinations = true;
    private int prefetchSize = 1000;
    /**
     * By default set to 0, which is disabled and prefetchSize value will be
     * used instead.
     */
    private int advisoryPrefetchSize = 0;
    private int advisoryAckPercentage = 75;
    private int networkTTL = 1;
    private int consumerTTL = networkTTL;
    private int messageTTL = networkTTL;

    private String brokerName = "localhost";
    private String brokerURL = "";
    private String userName;
    private String password;
    private String destinationFilter = null;
    private String name = "NC";

    protected List<ActiveMQDestination> excludedDestinations = new CopyOnWriteArrayList<ActiveMQDestination>();
    protected List<ActiveMQDestination> dynamicallyIncludedDestinations = new CopyOnWriteArrayList<ActiveMQDestination>();
    protected List<ActiveMQDestination> staticallyIncludedDestinations = new CopyOnWriteArrayList<ActiveMQDestination>();

    private boolean suppressDuplicateQueueSubscriptions = false;
    private boolean suppressDuplicateTopicSubscriptions = true;

    private boolean alwaysSyncSend = true;
    private boolean staticBridge = false;
    private boolean useCompression = false;
    private boolean advisoryForFailedForward = false;
    private boolean useBrokerNamesAsIdSeed = true;
    private boolean gcDestinationViews = true;
    private long gcSweepTime = 60 * 1000;
    private boolean checkDuplicateMessagesOnDuplex = false;

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

    public boolean isSyncDurableSubs() {
        return syncDurableSubs;
    }

    public void setSyncDurableSubs(boolean syncDurableSubs) {
        this.syncDurableSubs = syncDurableSubs;
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
        setConsumerTTL(networkTTL);
        setMessageTTL(networkTTL);
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
        if (prefetchSize < 1) {
            throw new IllegalArgumentException("prefetchSize must be > 0"
                    + " because network consumers do not poll for messages.");
        }
        this.prefetchSize = prefetchSize;
    }

    public int getAdvisoryPrefetchSize() {
        return advisoryPrefetchSize;
    }

    /**
     * Prefetch size for advisory consumers.  Just like prefetchSize, if set, this
     * value must be greater than 0 because network consumers do not poll for messages.
     * Setting this to 0 or less means this value is disabled and prefetchSize will be
     * used instead.
     *
     * @param advisoryPrefetchSize
     */
    public void setAdvisoryPrefetchSize(int advisoryPrefetchSize) {
        this.advisoryPrefetchSize = advisoryPrefetchSize;
    }

    public int getAdvisoryAckPercentage() {
        return advisoryAckPercentage;
    }

    /**
     * @param advisoryAckPercentage the percentage of the advisory prefetch size
     * value that can be dispatched before an ack will be sent, defaults to 75
     * which means that when the number of received messages is greater than 75% of
     * the prefetch size an ack will be sent back
     */
    public void setAdvisoryAckPercentage(int advisoryAckPercentage) {
        this.advisoryAckPercentage = advisoryAckPercentage;
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
        if (this.destinationFilter == null) {
            if (dynamicallyIncludedDestinations != null && !dynamicallyIncludedDestinations.isEmpty()) {
                StringBuffer filter = new StringBuffer();
                String delimiter = "";
                for (ActiveMQDestination destination : dynamicallyIncludedDestinations) {
                    if (!destination.isTemporary()) {
                        filter.append(delimiter);
                        filter.append(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX);
                        filter.append(destination.getDestinationTypeAsString());
                        filter.append(".");
                        filter.append(destination.getPhysicalName());
                        delimiter = ",";

                        if (useVirtualDestSubs) {
                            filter.append(delimiter);
                            filter.append(AdvisorySupport.VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX);
                            filter.append(destination.getDestinationTypeAsString());
                            filter.append(".");
                            filter.append(destination.getPhysicalName());
                        }
                    }
                }
                return filter.toString();
            }   else {
                StringBuffer filter = new StringBuffer();
                filter.append(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX);
                filter.append(">");
                if (useVirtualDestSubs) {
                    filter.append(",");
                    filter.append(AdvisorySupport.VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX);
                    filter.append(">");
                }
                return filter.toString();
            }
        } else {
            // prepend consumer advisory prefix
            // to keep backward compatibility
            if (!this.destinationFilter.startsWith(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX)) {
                 return AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX + this.destinationFilter;
            } else {
                return this.destinationFilter;
            }
        }
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

    public boolean isAlwaysSyncSend() {
        return alwaysSyncSend;
    }

    /**
     * @param alwaysSyncSend  when true, both persistent and non persistent
     * messages will be sent using a request. When false, non persistent messages
     * are acked once the oneway send succeeds, which can potentially lead to
     * message loss.
     * Using an async request, allows multiple outstanding requests. This ensures
     * that a bridge need not block all sending when the remote broker needs to
     * flow control a single destination.
     */
    public void setAlwaysSyncSend(boolean alwaysSyncSend) {
        this.alwaysSyncSend = alwaysSyncSend;
    }

    public int getConsumerPriorityBase() {
        return consumerPriorityBase;
    }

    /**
     * @param consumerPriorityBase , default -5. Sets the starting priority
     * for consumers. This base value will be decremented by the length of the
     * broker path when decreaseNetworkConsumerPriority is set.
     */
    public void setConsumerPriorityBase(int consumerPriorityBase) {
        this.consumerPriorityBase = consumerPriorityBase;
    }

    public boolean isStaticBridge() {
        return staticBridge;
    }

    public void setStaticBridge(boolean staticBridge) {
        this.staticBridge = staticBridge;
    }

    /**
     * @param useCompression
     *      True if the Network should enforce compression for messages sent.
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * @return the useCompression setting, true if message will be compressed on send.
     */
    public boolean isUseCompression() {
        return useCompression;
    }

    public boolean isAdvisoryForFailedForward() {
        return advisoryForFailedForward;
    }

    public void setAdvisoryForFailedForward(boolean advisoryForFailedForward) {
        this.advisoryForFailedForward = advisoryForFailedForward;
    }

    public void setConsumerTTL(int consumerTTL) {
        this.consumerTTL = consumerTTL;
    }

    public int getConsumerTTL() {
        return  consumerTTL;
    }

    public void setMessageTTL(int messageTTL) {
        this.messageTTL = messageTTL;
    }

    public int getMessageTTL() {
        return messageTTL;
    }

    public boolean isUseBrokerNamesAsIdSeed() {
        return useBrokerNamesAsIdSeed;
    }

    public void setUseBrokerNameAsIdSees(boolean val) {
        useBrokerNamesAsIdSeed = val;
    }

    public boolean isGcDestinationViews() {
        return gcDestinationViews;
    }

    public void setGcDestinationViews(boolean gcDestinationViews) {
        this.gcDestinationViews = gcDestinationViews;
    }

    public long getGcSweepTime() {
        return gcSweepTime;
    }

    public void setGcSweepTime(long gcSweepTime) {
        this.gcSweepTime = gcSweepTime;
    }

    public boolean isCheckDuplicateMessagesOnDuplex() {
        return checkDuplicateMessagesOnDuplex;
    }

    public void setCheckDuplicateMessagesOnDuplex(boolean checkDuplicateMessagesOnDuplex) {
        this.checkDuplicateMessagesOnDuplex = checkDuplicateMessagesOnDuplex;
    }

    public boolean isUseVirtualDestSubs() {
        return useVirtualDestSubs;
    }

    /**
     * This was a typo, so this is deprecated as of 5.13.1
     */
    @Deprecated
    public boolean isUseVirtualDestSus() {
        return useVirtualDestSubs;
    }

    public void setUseVirtualDestSubs(
            boolean useVirtualDestSubs) {
        this.useVirtualDestSubs = useVirtualDestSubs;
    }

}
