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
package org.apache.activemq.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.state.CommandVisitor;

/**
 * @openwire:marshaller code="5"
 *
 */
public class ConsumerInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.CONSUMER_INFO;

    public static final byte HIGH_PRIORITY = 10;
    public static final byte NORMAL_PRIORITY = 0;
    public static final byte NETWORK_CONSUMER_PRIORITY = -5;
    public static final byte LOW_PRIORITY = -10;

    protected ConsumerId consumerId;
    protected ActiveMQDestination destination;
    protected int prefetchSize;
    protected int maximumPendingMessageLimit;
    protected boolean browser;
    protected boolean dispatchAsync;
    protected String selector;
    protected String clientId;
    protected String subscriptionName;
    protected boolean noLocal;
    protected boolean exclusive;
    protected boolean retroactive;
    protected byte priority;
    protected BrokerId[] brokerPath;
    protected boolean optimizedAcknowledge;
    // used by the broker
    protected transient int currentPrefetchSize;
    // if true, the consumer will not send range
    protected boolean noRangeAcks;
    // acks.

    protected BooleanExpression additionalPredicate;
    protected transient boolean networkSubscription; // this subscription
    protected transient List<ConsumerId> networkConsumerIds; // the original consumerId

    // not marshalled, populated from RemoveInfo, the last message delivered, used
    // to suppress redelivery on prefetched messages after close
    private transient long lastDeliveredSequenceId;
    private transient long assignedGroupCount;
    // originated from a
    // network connection

    public ConsumerInfo() {
    }

    public ConsumerInfo(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public ConsumerInfo(SessionInfo sessionInfo, long consumerId) {
        this.consumerId = new ConsumerId(sessionInfo.getSessionId(), consumerId);
    }

    public ConsumerInfo copy() {
        ConsumerInfo info = new ConsumerInfo();
        copy(info);
        return info;
    }

    public void copy(ConsumerInfo info) {
        super.copy(info);
        info.consumerId = consumerId;
        info.destination = destination;
        info.prefetchSize = prefetchSize;
        info.maximumPendingMessageLimit = maximumPendingMessageLimit;
        info.browser = browser;
        info.dispatchAsync = dispatchAsync;
        info.selector = selector;
        info.clientId = clientId;
        info.subscriptionName = subscriptionName;
        info.noLocal = noLocal;
        info.exclusive = exclusive;
        info.retroactive = retroactive;
        info.priority = priority;
        info.brokerPath = brokerPath;
        info.networkSubscription = networkSubscription;
        if (networkConsumerIds != null) {
            if (info.networkConsumerIds==null){
                info.networkConsumerIds=new ArrayList<ConsumerId>();
            }
            info.networkConsumerIds.addAll(networkConsumerIds);
        }
    }

    public boolean isDurable() {
        return subscriptionName != null;
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * Is used to uniquely identify the consumer to the broker.
     *
     * @openwire:property version=1 cache=true
     */
    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * Is this consumer a queue browser?
     *
     * @openwire:property version=1
     */
    public boolean isBrowser() {
        return browser;
    }

    public void setBrowser(boolean browser) {
        this.browser = browser;
    }

    /**
     * The destination that the consumer is interested in receiving messages
     * from. This destination could be a composite destination.
     *
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * How many messages a broker will send to the client without receiving an
     * ack before he stops dispatching messages to the client.
     *
     * @openwire:property version=1
     */
    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
        this.currentPrefetchSize = prefetchSize;
    }

    /**
     * How many messages a broker will keep around, above the prefetch limit,
     * for non-durable topics before starting to discard older messages.
     *
     * @openwire:property version=1
     */
    public int getMaximumPendingMessageLimit() {
        return maximumPendingMessageLimit;
    }

    public void setMaximumPendingMessageLimit(int maximumPendingMessageLimit) {
        this.maximumPendingMessageLimit = maximumPendingMessageLimit;
    }

    /**
     * Should the broker dispatch a message to the consumer async? If he does it
     * async, then he uses a more SEDA style of processing while if it is not
     * done async, then he broker use a STP style of processing. STP is more
     * appropriate in high bandwidth situations or when being used by and in vm
     * transport.
     *
     * @openwire:property version=1
     */
    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    /**
     * The JMS selector used to filter out messages that this consumer is
     * interested in.
     *
     * @openwire:property version=1
     */
    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    /**
     * Used to identify the id of a client connection.
     *
     * @openwire:property version=10
     */
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Used to identify the name of a durable subscription.
     *
     * @openwire:property version=1
     */
    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String durableSubscriptionId) {
        this.subscriptionName = durableSubscriptionId;
    }

    /**
     * Set noLocal to true to avoid receiving messages that were published
     * locally on the same connection.
     *
     * @openwire:property version=1
     */
    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    /**
     * An exclusive consumer locks out other consumers from being able to
     * receive messages from the destination. If there are multiple exclusive
     * consumers for a destination, the first one created will be the exclusive
     * consumer of the destination.
     *
     * @openwire:property version=1
     */
    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    /**
     * A retroactive consumer only has meaning for Topics. It allows a consumer
     * to retroactively see messages sent prior to the consumer being created.
     * If the consumer is not durable, it will be delivered the last message
     * published to the topic. If the consumer is durable then it will receive
     * all persistent messages that are still stored in persistent storage for
     * that topic.
     *
     * @openwire:property version=1
     */
    public boolean isRetroactive() {
        return retroactive;
    }

    public void setRetroactive(boolean retroactive) {
        this.retroactive = retroactive;
    }

    public RemoveInfo createRemoveCommand() {
        RemoveInfo command = new RemoveInfo(getConsumerId());
        command.setResponseRequired(isResponseRequired());
        return command;
    }

    /**
     * The broker will avoid dispatching to a lower priority consumer if there
     * are other higher priority consumers available to dispatch to. This allows
     * letting the broker to have an affinity to higher priority consumers.
     * Default priority is 0.
     *
     * @openwire:property version=1
     */
    public byte getPriority() {
        return priority;
    }

    public void setPriority(byte priority) {
        this.priority = priority;
    }

    /**
     * The route of brokers the command has moved through.
     *
     * @openwire:property version=1 cache=true
     */
    public BrokerId[] getBrokerPath() {
        return brokerPath;
    }

    public void setBrokerPath(BrokerId[] brokerPath) {
        this.brokerPath = brokerPath;
    }

    /**
     * A transient additional predicate that can be used it inject additional
     * predicates into the selector on the fly. Handy if if say a Security
     * Broker interceptor wants to filter out messages based on security level
     * of the consumer.
     *
     * @openwire:property version=1
     */
    public BooleanExpression getAdditionalPredicate() {
        return additionalPredicate;
    }

    public void setAdditionalPredicate(BooleanExpression additionalPredicate) {
        this.additionalPredicate = additionalPredicate;
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processAddConsumer(this);
    }

    /**
     * @openwire:property version=1
     * @return Returns the networkSubscription.
     */
    public boolean isNetworkSubscription() {
        return networkSubscription;
    }

    /**
     * @param networkSubscription The networkSubscription to set.
     */
    public void setNetworkSubscription(boolean networkSubscription) {
        this.networkSubscription = networkSubscription;
    }

    /**
     * @openwire:property version=1
     * @return Returns the optimizedAcknowledge.
     */
    public boolean isOptimizedAcknowledge() {
        return optimizedAcknowledge;
    }

    /**
     * @param optimizedAcknowledge The optimizedAcknowledge to set.
     */
    public void setOptimizedAcknowledge(boolean optimizedAcknowledge) {
        this.optimizedAcknowledge = optimizedAcknowledge;
    }

    /**
     * @return Returns the currentPrefetchSize.
     */
    public int getCurrentPrefetchSize() {
        return currentPrefetchSize;
    }

    /**
     * @param currentPrefetchSize The currentPrefetchSize to set.
     */
    public void setCurrentPrefetchSize(int currentPrefetchSize) {
        this.currentPrefetchSize = currentPrefetchSize;
    }

    /**
     * The broker may be able to optimize it's processing or provides better QOS
     * if it knows the consumer will not be sending ranged acks.
     *
     * @return true if the consumer will not send range acks.
     * @openwire:property version=1
     */
    public boolean isNoRangeAcks() {
        return noRangeAcks;
    }

    public void setNoRangeAcks(boolean noRangeAcks) {
        this.noRangeAcks = noRangeAcks;
    }

    public synchronized void addNetworkConsumerId(ConsumerId networkConsumerId) {
        if (networkConsumerIds == null) {
            networkConsumerIds = new ArrayList<ConsumerId>();
        }
        networkConsumerIds.add(networkConsumerId);
    }

    public synchronized void removeNetworkConsumerId(ConsumerId networkConsumerId) {
        if (networkConsumerIds != null) {
            networkConsumerIds.remove(networkConsumerId);
            if (networkConsumerIds.isEmpty()) {
                networkConsumerIds=null;
            }
        }
    }

    public synchronized boolean isNetworkConsumersEmpty() {
        return networkConsumerIds == null || networkConsumerIds.isEmpty();
    }

    public synchronized List<ConsumerId> getNetworkConsumerIds(){
        List<ConsumerId> result = new ArrayList<ConsumerId>();
        if (networkConsumerIds != null) {
            result.addAll(networkConsumerIds);
        }
        return result;
    }

    @Override
    public int hashCode() {
        return (consumerId == null) ? 0 : consumerId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        ConsumerInfo other = (ConsumerInfo) obj;

        if (consumerId == null && other.consumerId != null) {
            return false;
        } else if (!consumerId.equals(other.consumerId)) {
            return false;
        }
        return true;
    }

    /**
     * Tracks the original subscription id that causes a subscription to
     * percolate through a network when networkTTL > 1. Tracking the original
     * subscription allows duplicate suppression.
     *
     * @return array of the current subscription path
     * @openwire:property version=4
     */
    public ConsumerId[] getNetworkConsumerPath() {
        ConsumerId[] result = null;
        if (networkConsumerIds != null) {
            result = networkConsumerIds.toArray(new ConsumerId[0]);
        }
        return result;
    }

    public void setNetworkConsumerPath(ConsumerId[] consumerPath) {
        if (consumerPath != null) {
            for (int i=0; i<consumerPath.length; i++) {
                addNetworkConsumerId(consumerPath[i]);
            }
        }
    }

    public void setLastDeliveredSequenceId(long lastDeliveredSequenceId) {
        this.lastDeliveredSequenceId  = lastDeliveredSequenceId;
    }

    public long getLastDeliveredSequenceId() {
        return lastDeliveredSequenceId;
    }

    public void incrementAssignedGroupCount() {
        this.assignedGroupCount++;
    }

    public void clearAssignedGroupCount() {
        this.assignedGroupCount=0;
    }

    public void decrementAssignedGroupCount() {
        this.assignedGroupCount--;
    }

    public long getAssignedGroupCount() {
        return assignedGroupCount;
    }

}
