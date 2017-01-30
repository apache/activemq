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
package org.apache.activemq.broker.region.policy;

import java.util.Set;

import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.QueueSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.group.GroupFactoryFinder;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.network.NetworkBridgeFilterFactory;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an entry in a {@link PolicyMap} for assigning policies to a
 * specific destination or a hierarchical wildcard area of destinations.
 *
 * @org.apache.xbean.XBean
 *
 */
public class PolicyEntry extends DestinationMapEntry {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyEntry.class);
    private DispatchPolicy dispatchPolicy;
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy;
    private boolean sendAdvisoryIfNoConsumers;
    private DeadLetterStrategy deadLetterStrategy = Destination.DEFAULT_DEAD_LETTER_STRATEGY;
    private PendingMessageLimitStrategy pendingMessageLimitStrategy;
    private MessageEvictionStrategy messageEvictionStrategy;
    private long memoryLimit;
    private String messageGroupMapFactoryType = "cached";
    private MessageGroupMapFactory messageGroupMapFactory;
    private PendingQueueMessageStoragePolicy pendingQueuePolicy;
    private PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy;
    private PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy;
    private int maxProducersToAudit=BaseDestination.MAX_PRODUCERS_TO_AUDIT;
    private int maxAuditDepth=BaseDestination.MAX_AUDIT_DEPTH;
    private int maxQueueAuditDepth=BaseDestination.MAX_AUDIT_DEPTH;
    private boolean enableAudit=true;
    private boolean producerFlowControl = true;
    private boolean alwaysRetroactive = false;
    private long blockedProducerWarningInterval = Destination.DEFAULT_BLOCKED_PRODUCER_WARNING_INTERVAL;
    private boolean optimizedDispatch=false;
    private int maxPageSize=BaseDestination.MAX_PAGE_SIZE;
    private int maxBrowsePageSize=BaseDestination.MAX_BROWSE_PAGE_SIZE;
    private boolean useCache=true;
    private long minimumMessageSize=1024;
    private boolean useConsumerPriority=true;
    private boolean strictOrderDispatch=false;
    private boolean lazyDispatch=false;
    private int timeBeforeDispatchStarts = 0;
    private int consumersBeforeDispatchStarts = 0;
    private boolean advisoryForSlowConsumers;
    private boolean advisoryForFastProducers;
    private boolean advisoryForDiscardingMessages;
    private boolean advisoryWhenFull;
    private boolean advisoryForDelivery;
    private boolean advisoryForConsumed;
    private boolean includeBodyForAdvisory;
    private long expireMessagesPeriod = BaseDestination.EXPIRE_MESSAGE_PERIOD;
    private int maxExpirePageSize = BaseDestination.MAX_BROWSE_PAGE_SIZE;
    private int queuePrefetch=ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH;
    private int queueBrowserPrefetch=ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH;
    private int topicPrefetch=ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH;
    private int durableTopicPrefetch=ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH;
    private boolean usePrefetchExtension = true;
    private int cursorMemoryHighWaterMark = 70;
    private int storeUsageHighWaterMark = 100;
    private SlowConsumerStrategy slowConsumerStrategy;
    private boolean prioritizedMessages;
    private boolean allConsumersExclusiveByDefault;
    private boolean gcInactiveDestinations;
    private boolean gcWithNetworkConsumers;
    private long inactiveTimeoutBeforeGC = BaseDestination.DEFAULT_INACTIVE_TIMEOUT_BEFORE_GC;
    private boolean reduceMemoryFootprint;
    private NetworkBridgeFilterFactory networkBridgeFilterFactory;
    private boolean doOptimzeMessageStorage = true;
    private int maxDestinations = -1;

    /*
     * percentage of in-flight messages above which optimize message store is disabled
     */
    private int optimizeMessageStoreInFlightLimit = 10;
    private boolean persistJMSRedelivered = false;


    public void configure(Broker broker,Queue queue) {
        baseConfiguration(broker,queue);
        if (dispatchPolicy != null) {
            queue.setDispatchPolicy(dispatchPolicy);
        }
        queue.setDeadLetterStrategy(getDeadLetterStrategy());
        queue.setMessageGroupMapFactory(getMessageGroupMapFactory());
        if (memoryLimit > 0) {
            queue.getMemoryUsage().setLimit(memoryLimit);
        }
        if (pendingQueuePolicy != null) {
            PendingMessageCursor messages = pendingQueuePolicy.getQueuePendingMessageCursor(broker,queue);
            queue.setMessages(messages);
        }

        queue.setUseConsumerPriority(isUseConsumerPriority());
        queue.setStrictOrderDispatch(isStrictOrderDispatch());
        queue.setOptimizedDispatch(isOptimizedDispatch());
        queue.setLazyDispatch(isLazyDispatch());
        queue.setTimeBeforeDispatchStarts(getTimeBeforeDispatchStarts());
        queue.setConsumersBeforeDispatchStarts(getConsumersBeforeDispatchStarts());
        queue.setAllConsumersExclusiveByDefault(isAllConsumersExclusiveByDefault());
        queue.setPersistJMSRedelivered(isPersistJMSRedelivered());
    }

    public void update(Queue queue) {
        update(queue, null);
    }

    /**
     * Update a queue with this policy.  Only apply properties that
     * match the includedProperties list.  Not all properties are eligible
     * to be updated.
     *
     * If includedProperties is null then all of the properties will be set as
     * isUpdate will return true
     * @param baseDestination
     * @param includedProperties
     */
    public void update(Queue queue, Set<String> includedProperties) {
        baseUpdate(queue, includedProperties);
        if (isUpdate("memoryLimit", includedProperties) && memoryLimit > 0) {
            queue.getMemoryUsage().setLimit(memoryLimit);
        }
        if (isUpdate("useConsumerPriority", includedProperties)) {
            queue.setUseConsumerPriority(isUseConsumerPriority());
        }
        if (isUpdate("strictOrderDispatch", includedProperties)) {
            queue.setStrictOrderDispatch(isStrictOrderDispatch());
        }
        if (isUpdate("optimizedDispatch", includedProperties)) {
            queue.setOptimizedDispatch(isOptimizedDispatch());
        }
        if (isUpdate("lazyDispatch", includedProperties)) {
            queue.setLazyDispatch(isLazyDispatch());
        }
        if (isUpdate("timeBeforeDispatchStarts", includedProperties)) {
            queue.setTimeBeforeDispatchStarts(getTimeBeforeDispatchStarts());
        }
        if (isUpdate("consumersBeforeDispatchStarts", includedProperties)) {
            queue.setConsumersBeforeDispatchStarts(getConsumersBeforeDispatchStarts());
        }
        if (isUpdate("allConsumersExclusiveByDefault", includedProperties)) {
            queue.setAllConsumersExclusiveByDefault(isAllConsumersExclusiveByDefault());
        }
        if (isUpdate("persistJMSRedelivered", includedProperties)) {
            queue.setPersistJMSRedelivered(isPersistJMSRedelivered());
        }
    }

    public void configure(Broker broker,Topic topic) {
        baseConfiguration(broker,topic);
        if (dispatchPolicy != null) {
            topic.setDispatchPolicy(dispatchPolicy);
        }
        topic.setDeadLetterStrategy(getDeadLetterStrategy());
        if (subscriptionRecoveryPolicy != null) {
            SubscriptionRecoveryPolicy srp = subscriptionRecoveryPolicy.copy();
            srp.setBroker(broker);
            topic.setSubscriptionRecoveryPolicy(srp);
        }
        if (memoryLimit > 0) {
            topic.getMemoryUsage().setLimit(memoryLimit);
        }
        topic.setLazyDispatch(isLazyDispatch());
    }

    public void update(Topic topic) {
        update(topic, null);
    }

    //If includedProperties is null then all of the properties will be set as
    //isUpdate will return true
    public void update(Topic topic, Set<String> includedProperties) {
        baseUpdate(topic, includedProperties);
        if (isUpdate("memoryLimit", includedProperties) && memoryLimit > 0) {
            topic.getMemoryUsage().setLimit(memoryLimit);
        }
        if (isUpdate("lazyDispatch", includedProperties)) {
            topic.setLazyDispatch(isLazyDispatch());
        }
    }

    // attributes that can change on the fly
    public void baseUpdate(BaseDestination destination) {
        baseUpdate(destination, null);
    }

    // attributes that can change on the fly
    //If includedProperties is null then all of the properties will be set as
    //isUpdate will return true
    public void baseUpdate(BaseDestination destination, Set<String> includedProperties) {
        if (isUpdate("producerFlowControl", includedProperties)) {
            destination.setProducerFlowControl(isProducerFlowControl());
        }
        if (isUpdate("alwaysRetroactive", includedProperties)) {
            destination.setAlwaysRetroactive(isAlwaysRetroactive());
        }
        if (isUpdate("blockedProducerWarningInterval", includedProperties)) {
            destination.setBlockedProducerWarningInterval(getBlockedProducerWarningInterval());
        }
        if (isUpdate("maxPageSize", includedProperties)) {
            destination.setMaxPageSize(getMaxPageSize());
        }
        if (isUpdate("maxBrowsePageSize", includedProperties)) {
            destination.setMaxBrowsePageSize(getMaxBrowsePageSize());
        }

        if (isUpdate("minimumMessageSize", includedProperties)) {
            destination.setMinimumMessageSize((int) getMinimumMessageSize());
        }
        if (isUpdate("maxExpirePageSize", includedProperties)) {
            destination.setMaxExpirePageSize(getMaxExpirePageSize());
        }
        if (isUpdate("cursorMemoryHighWaterMark", includedProperties)) {
            destination.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        }
        if (isUpdate("storeUsageHighWaterMark", includedProperties)) {
            destination.setStoreUsageHighWaterMark(getStoreUsageHighWaterMark());
        }
        if (isUpdate("gcInactiveDestinations", includedProperties)) {
            destination.setGcIfInactive(isGcInactiveDestinations());
        }
        if (isUpdate("gcWithNetworkConsumers", includedProperties)) {
            destination.setGcWithNetworkConsumers(isGcWithNetworkConsumers());
        }
        if (isUpdate("inactiveTimeoutBeforeGc", includedProperties)) {
            destination.setInactiveTimeoutBeforeGC(getInactiveTimeoutBeforeGC());
        }
        if (isUpdate("reduceMemoryFootprint", includedProperties)) {
            destination.setReduceMemoryFootprint(isReduceMemoryFootprint());
        }
        if (isUpdate("doOptimizeMessageStore", includedProperties)) {
            destination.setDoOptimzeMessageStorage(isDoOptimzeMessageStorage());
        }
        if (isUpdate("optimizeMessageStoreInFlightLimit", includedProperties)) {
            destination.setOptimizeMessageStoreInFlightLimit(getOptimizeMessageStoreInFlightLimit());
        }
        if (isUpdate("advisoryForConsumed", includedProperties)) {
            destination.setAdvisoryForConsumed(isAdvisoryForConsumed());
        }
        if (isUpdate("advisoryForDelivery", includedProperties)) {
            destination.setAdvisoryForDelivery(isAdvisoryForDelivery());
        }
        if (isUpdate("advisoryForDiscardingMessages", includedProperties)) {
            destination.setAdvisoryForDiscardingMessages(isAdvisoryForDiscardingMessages());
        }
        if (isUpdate("advisoryForSlowConsumers", includedProperties)) {
            destination.setAdvisoryForSlowConsumers(isAdvisoryForSlowConsumers());
        }
        if (isUpdate("advisoryForFastProducers", includedProperties)) {
            destination.setAdvisoryForFastProducers(isAdvisoryForFastProducers());
        }
        if (isUpdate("advisoryWhenFull", includedProperties)) {
            destination.setAdvisoryWhenFull(isAdvisoryWhenFull());
        }
        if (isUpdate("includeBodyForAdvisory", includedProperties)) {
            destination.setIncludeBodyForAdvisory(isIncludeBodyForAdvisory());
        }
        if (isUpdate("sendAdvisoryIfNoConsumers", includedProperties)) {
            destination.setSendAdvisoryIfNoConsumers(isSendAdvisoryIfNoConsumers());
        }
    }

    public void baseConfiguration(Broker broker, BaseDestination destination) {
        baseUpdate(destination);
        destination.setEnableAudit(isEnableAudit());
        destination.setMaxAuditDepth(getMaxQueueAuditDepth());
        destination.setMaxProducersToAudit(getMaxProducersToAudit());
        destination.setUseCache(isUseCache());
        destination.setExpireMessagesPeriod(getExpireMessagesPeriod());
        SlowConsumerStrategy scs = getSlowConsumerStrategy();
        if (scs != null) {
            scs.setBrokerService(broker);
            scs.addDestination(destination);
        }
        destination.setSlowConsumerStrategy(scs);
        destination.setPrioritizedMessages(isPrioritizedMessages());
    }

    public void configure(Broker broker, SystemUsage memoryManager, TopicSubscription subscription) {
        configurePrefetch(subscription);
        subscription.setUsePrefetchExtension(isUsePrefetchExtension());
        subscription.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        if (pendingMessageLimitStrategy != null) {
            int value = pendingMessageLimitStrategy.getMaximumPendingMessageLimit(subscription);
            int consumerLimit = subscription.getInfo().getMaximumPendingMessageLimit();
            if (consumerLimit > 0) {
                if (value < 0 || consumerLimit < value) {
                    value = consumerLimit;
                }
            }
            if (value >= 0) {
                LOG.debug("Setting the maximumPendingMessages size to: {} for consumer: {}", value, subscription.getInfo().getConsumerId());
                subscription.setMaximumPendingMessages(value);
            }
        }
        if (messageEvictionStrategy != null) {
            subscription.setMessageEvictionStrategy(messageEvictionStrategy);
        }
        if (pendingSubscriberPolicy != null) {
            String name = subscription.getContext().getClientId() + "_" + subscription.getConsumerInfo().getConsumerId();
            int maxBatchSize = subscription.getConsumerInfo().getPrefetchSize();
            subscription.setMatched(pendingSubscriberPolicy.getSubscriberPendingMessageCursor(broker,name, maxBatchSize,subscription));
        }
        if (enableAudit) {
            subscription.setEnableAudit(enableAudit);
            subscription.setMaxProducersToAudit(maxProducersToAudit);
            subscription.setMaxAuditDepth(maxAuditDepth);
        }
    }

    public void configure(Broker broker, SystemUsage memoryManager, DurableTopicSubscription sub) {
        String clientId = sub.getSubscriptionKey().getClientId();
        String subName = sub.getSubscriptionKey().getSubscriptionName();
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        configurePrefetch(sub);
        if (pendingDurableSubscriberPolicy != null) {
            PendingMessageCursor cursor = pendingDurableSubscriberPolicy.getSubscriberPendingMessageCursor(broker,clientId, subName,sub.getPrefetchSize(),sub);
            cursor.setSystemUsage(memoryManager);
            sub.setPending(cursor);
        }
        int auditDepth = getMaxAuditDepth();
        if (auditDepth == BaseDestination.MAX_AUDIT_DEPTH && this.isPrioritizedMessages()) {
            sub.setMaxAuditDepth(auditDepth * 10);
        } else {
            sub.setMaxAuditDepth(auditDepth);
        }
        sub.setMaxProducersToAudit(getMaxProducersToAudit());
        sub.setUsePrefetchExtension(isUsePrefetchExtension());
    }

    public void configure(Broker broker, SystemUsage memoryManager, QueueBrowserSubscription sub) {
        configurePrefetch(sub);
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        sub.setUsePrefetchExtension(isUsePrefetchExtension());

        // TODO
        // We currently need an infinite audit because of the way that browser dispatch
        // is done.  We should refactor the browsers to better handle message dispatch so
        // we can remove this and perform a more efficient dispatch.
        sub.setMaxProducersToAudit(Integer.MAX_VALUE);
        sub.setMaxAuditDepth(Short.MAX_VALUE);

        // part solution - dispatching to browsers needs to be restricted
        sub.setMaxMessages(getMaxBrowsePageSize());
    }

    public void configure(Broker broker, SystemUsage memoryManager, QueueSubscription sub) {
        configurePrefetch(sub);
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        sub.setUsePrefetchExtension(isUsePrefetchExtension());
        sub.setMaxProducersToAudit(getMaxProducersToAudit());
    }

    public void configurePrefetch(Subscription subscription) {

        final int currentPrefetch = subscription.getConsumerInfo().getPrefetchSize();
        if (subscription instanceof QueueBrowserSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH) {
                ((QueueBrowserSubscription) subscription).setPrefetchSize(getQueueBrowserPrefetch());
            }
        } else if (subscription instanceof QueueSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH) {
                ((QueueSubscription) subscription).setPrefetchSize(getQueuePrefetch());
            }
        } else if (subscription instanceof DurableTopicSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH ||
                    subscription.getConsumerInfo().getPrefetchSize() == ActiveMQPrefetchPolicy.DEFAULT_OPTIMIZE_DURABLE_TOPIC_PREFETCH) {
                ((DurableTopicSubscription)subscription).setPrefetchSize(getDurableTopicPrefetch());
            }
        } else if (subscription instanceof TopicSubscription) {
            if (currentPrefetch == ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH) {
                ((TopicSubscription) subscription).setPrefetchSize(getTopicPrefetch());
            }
        }
        if (currentPrefetch != 0 && subscription.getPrefetchSize() == 0) {
            // tell the sub so that it can issue a pull request
            subscription.updateConsumerPrefetch(0);
        }
    }

    private boolean isUpdate(String property, Set<String> includedProperties) {
        return includedProperties == null || includedProperties.contains(property);
    }
    // Properties
    // -------------------------------------------------------------------------
    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy policy) {
        this.dispatchPolicy = policy;
    }

    public SubscriptionRecoveryPolicy getSubscriptionRecoveryPolicy() {
        return subscriptionRecoveryPolicy;
    }

    public void setSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy subscriptionRecoveryPolicy) {
        this.subscriptionRecoveryPolicy = subscriptionRecoveryPolicy;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    /**
     * Sends an advisory message if a non-persistent message is sent and there
     * are no active consumers
     */
    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }

    public DeadLetterStrategy getDeadLetterStrategy() {
        return deadLetterStrategy;
    }

    /**
     * Sets the policy used to determine which dead letter queue destination
     * should be used
     */
    public void setDeadLetterStrategy(DeadLetterStrategy deadLetterStrategy) {
        this.deadLetterStrategy = deadLetterStrategy;
    }

    public PendingMessageLimitStrategy getPendingMessageLimitStrategy() {
        return pendingMessageLimitStrategy;
    }

    /**
     * Sets the strategy to calculate the maximum number of messages that are
     * allowed to be pending on consumers (in addition to their prefetch sizes).
     * Once the limit is reached, non-durable topics can then start discarding
     * old messages. This allows us to keep dispatching messages to slow
     * consumers while not blocking fast consumers and discarding the messages
     * oldest first.
     */
    public void setPendingMessageLimitStrategy(PendingMessageLimitStrategy pendingMessageLimitStrategy) {
        this.pendingMessageLimitStrategy = pendingMessageLimitStrategy;
    }

    public MessageEvictionStrategy getMessageEvictionStrategy() {
        return messageEvictionStrategy;
    }

    /**
     * Sets the eviction strategy used to decide which message to evict when the
     * slow consumer needs to discard messages
     */
    public void setMessageEvictionStrategy(MessageEvictionStrategy messageEvictionStrategy) {
        this.messageEvictionStrategy = messageEvictionStrategy;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    /**
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setMemoryLimit(long memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    public MessageGroupMapFactory getMessageGroupMapFactory() {
        if (messageGroupMapFactory == null) {
            try {
            messageGroupMapFactory = GroupFactoryFinder.createMessageGroupMapFactory(getMessageGroupMapFactoryType());
            }catch(Exception e){
                LOG.error("Failed to create message group Factory ",e);
            }
        }
        return messageGroupMapFactory;
    }

    /**
     * Sets the factory used to create new instances of {MessageGroupMap} used
     * to implement the <a
     * href="http://activemq.apache.org/message-groups.html">Message Groups</a>
     * functionality.
     */
    public void setMessageGroupMapFactory(MessageGroupMapFactory messageGroupMapFactory) {
        this.messageGroupMapFactory = messageGroupMapFactory;
    }


    public String getMessageGroupMapFactoryType() {
        return messageGroupMapFactoryType;
    }

    public void setMessageGroupMapFactoryType(String messageGroupMapFactoryType) {
        this.messageGroupMapFactoryType = messageGroupMapFactoryType;
    }


    /**
     * @return the pendingDurableSubscriberPolicy
     */
    public PendingDurableSubscriberMessageStoragePolicy getPendingDurableSubscriberPolicy() {
        return this.pendingDurableSubscriberPolicy;
    }

    /**
     * @param pendingDurableSubscriberPolicy the pendingDurableSubscriberPolicy
     *                to set
     */
    public void setPendingDurableSubscriberPolicy(PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy) {
        this.pendingDurableSubscriberPolicy = pendingDurableSubscriberPolicy;
    }

    /**
     * @return the pendingQueuePolicy
     */
    public PendingQueueMessageStoragePolicy getPendingQueuePolicy() {
        return this.pendingQueuePolicy;
    }

    /**
     * @param pendingQueuePolicy the pendingQueuePolicy to set
     */
    public void setPendingQueuePolicy(PendingQueueMessageStoragePolicy pendingQueuePolicy) {
        this.pendingQueuePolicy = pendingQueuePolicy;
    }

    /**
     * @return the pendingSubscriberPolicy
     */
    public PendingSubscriberMessageStoragePolicy getPendingSubscriberPolicy() {
        return this.pendingSubscriberPolicy;
    }

    /**
     * @param pendingSubscriberPolicy the pendingSubscriberPolicy to set
     */
    public void setPendingSubscriberPolicy(PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy) {
        this.pendingSubscriberPolicy = pendingSubscriberPolicy;
    }

    /**
     * @return true if producer flow control enabled
     */
    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }

    /**
     * @param producerFlowControl
     */
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }

    /**
     * @return true if topic is always retroactive
     */
    public boolean isAlwaysRetroactive() {
        return alwaysRetroactive;
    }

    /**
     * @param alwaysRetroactive
     */
    public void setAlwaysRetroactive(boolean alwaysRetroactive) {
        this.alwaysRetroactive = alwaysRetroactive;
    }


    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     *
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    public void setBlockedProducerWarningInterval(long blockedProducerWarningInterval) {
        this.blockedProducerWarningInterval = blockedProducerWarningInterval;
    }

    /**
     *
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    public long getBlockedProducerWarningInterval() {
        return blockedProducerWarningInterval;
    }

    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }

    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }

    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return enableAudit;
    }

    /**
     * @param enableAudit the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    public int getMaxQueueAuditDepth() {
        return maxQueueAuditDepth;
    }

    public void setMaxQueueAuditDepth(int maxQueueAuditDepth) {
        this.maxQueueAuditDepth = maxQueueAuditDepth;
    }

    public boolean isOptimizedDispatch() {
        return optimizedDispatch;
    }

    public void setOptimizedDispatch(boolean optimizedDispatch) {
        this.optimizedDispatch = optimizedDispatch;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public int getMaxBrowsePageSize() {
        return maxBrowsePageSize;
    }

    public void setMaxBrowsePageSize(int maxPageSize) {
        this.maxBrowsePageSize = maxPageSize;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public long getMinimumMessageSize() {
        return minimumMessageSize;
    }

    public void setMinimumMessageSize(long minimumMessageSize) {
        this.minimumMessageSize = minimumMessageSize;
    }

    public boolean isUseConsumerPriority() {
        return useConsumerPriority;
    }

    public void setUseConsumerPriority(boolean useConsumerPriority) {
        this.useConsumerPriority = useConsumerPriority;
    }

    public boolean isStrictOrderDispatch() {
        return strictOrderDispatch;
    }

    public void setStrictOrderDispatch(boolean strictOrderDispatch) {
        this.strictOrderDispatch = strictOrderDispatch;
    }

    public boolean isLazyDispatch() {
        return lazyDispatch;
    }

    public void setLazyDispatch(boolean lazyDispatch) {
        this.lazyDispatch = lazyDispatch;
    }

    public int getTimeBeforeDispatchStarts() {
        return timeBeforeDispatchStarts;
    }

    public void setTimeBeforeDispatchStarts(int timeBeforeDispatchStarts) {
        this.timeBeforeDispatchStarts = timeBeforeDispatchStarts;
    }

    public int getConsumersBeforeDispatchStarts() {
        return consumersBeforeDispatchStarts;
    }

    public void setConsumersBeforeDispatchStarts(int consumersBeforeDispatchStarts) {
        this.consumersBeforeDispatchStarts = consumersBeforeDispatchStarts;
    }

    /**
     * @return the advisoryForSlowConsumers
     */
    public boolean isAdvisoryForSlowConsumers() {
        return advisoryForSlowConsumers;
    }

    /**
     * @param advisoryForSlowConsumers the advisoryForSlowConsumers to set
     */
    public void setAdvisoryForSlowConsumers(boolean advisoryForSlowConsumers) {
        this.advisoryForSlowConsumers = advisoryForSlowConsumers;
    }

    /**
     * @return the advisoryForDiscardingMessages
     */
    public boolean isAdvisoryForDiscardingMessages() {
        return advisoryForDiscardingMessages;
    }

    /**
     * @param advisoryForDiscardingMessages the advisoryForDiscardingMessages to set
     */
    public void setAdvisoryForDiscardingMessages(
            boolean advisoryForDiscardingMessages) {
        this.advisoryForDiscardingMessages = advisoryForDiscardingMessages;
    }

    /**
     * @return the advisoryWhenFull
     */
    public boolean isAdvisoryWhenFull() {
        return advisoryWhenFull;
    }

    /**
     * @param advisoryWhenFull the advisoryWhenFull to set
     */
    public void setAdvisoryWhenFull(boolean advisoryWhenFull) {
        this.advisoryWhenFull = advisoryWhenFull;
    }

    /**
     * @return the advisoryForDelivery
     */
    public boolean isAdvisoryForDelivery() {
        return advisoryForDelivery;
    }

    /**
     * @param advisoryForDelivery the advisoryForDelivery to set
     */
    public void setAdvisoryForDelivery(boolean advisoryForDelivery) {
        this.advisoryForDelivery = advisoryForDelivery;
    }

    /**
     * @return the advisoryForConsumed
     */
    public boolean isAdvisoryForConsumed() {
        return advisoryForConsumed;
    }

    /**
     * @param advisoryForConsumed the advisoryForConsumed to set
     */
    public void setAdvisoryForConsumed(boolean advisoryForConsumed) {
        this.advisoryForConsumed = advisoryForConsumed;
    }

    /**
     * @return the advisdoryForFastProducers
     */
    public boolean isAdvisoryForFastProducers() {
        return advisoryForFastProducers;
    }

    /**
     * @param advisoryForFastProducers the advisdoryForFastProducers to set
     */
    public void setAdvisoryForFastProducers(boolean advisoryForFastProducers) {
        this.advisoryForFastProducers = advisoryForFastProducers;
    }

    /**
     * Returns true if the original message body should be included when applicable
     * for advisory messages
     *
     * @return
     */
    public boolean isIncludeBodyForAdvisory() {
        return includeBodyForAdvisory;
    }

    /**
     * Sets if the original message body should be included when applicable
     * for advisory messages
     *
     * @param includeBodyForAdvisory
     */
    public void setIncludeBodyForAdvisory(boolean includeBodyForAdvisory) {
        this.includeBodyForAdvisory = includeBodyForAdvisory;
    }

    public void setMaxExpirePageSize(int maxExpirePageSize) {
        this.maxExpirePageSize = maxExpirePageSize;
    }

    public int getMaxExpirePageSize() {
        return maxExpirePageSize;
    }

    public void setExpireMessagesPeriod(long expireMessagesPeriod) {
        this.expireMessagesPeriod = expireMessagesPeriod;
    }

    public long getExpireMessagesPeriod() {
        return expireMessagesPeriod;
    }

    /**
     * Get the queuePrefetch
     * @return the queuePrefetch
     */
    public int getQueuePrefetch() {
        return this.queuePrefetch;
    }

    /**
     * Set the queuePrefetch
     * @param queuePrefetch the queuePrefetch to set
     */
    public void setQueuePrefetch(int queuePrefetch) {
        this.queuePrefetch = queuePrefetch;
    }

    /**
     * Get the queueBrowserPrefetch
     * @return the queueBrowserPrefetch
     */
    public int getQueueBrowserPrefetch() {
        return this.queueBrowserPrefetch;
    }

    /**
     * Set the queueBrowserPrefetch
     * @param queueBrowserPrefetch the queueBrowserPrefetch to set
     */
    public void setQueueBrowserPrefetch(int queueBrowserPrefetch) {
        this.queueBrowserPrefetch = queueBrowserPrefetch;
    }

    /**
     * Get the topicPrefetch
     * @return the topicPrefetch
     */
    public int getTopicPrefetch() {
        return this.topicPrefetch;
    }

    /**
     * Set the topicPrefetch
     * @param topicPrefetch the topicPrefetch to set
     */
    public void setTopicPrefetch(int topicPrefetch) {
        this.topicPrefetch = topicPrefetch;
    }

    /**
     * Get the durableTopicPrefetch
     * @return the durableTopicPrefetch
     */
    public int getDurableTopicPrefetch() {
        return this.durableTopicPrefetch;
    }

    /**
     * Set the durableTopicPrefetch
     * @param durableTopicPrefetch the durableTopicPrefetch to set
     */
    public void setDurableTopicPrefetch(int durableTopicPrefetch) {
        this.durableTopicPrefetch = durableTopicPrefetch;
    }

    public boolean isUsePrefetchExtension() {
        return this.usePrefetchExtension;
    }

    public void setUsePrefetchExtension(boolean usePrefetchExtension) {
        this.usePrefetchExtension = usePrefetchExtension;
    }

    public int getCursorMemoryHighWaterMark() {
        return this.cursorMemoryHighWaterMark;
    }

    public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
        this.cursorMemoryHighWaterMark = cursorMemoryHighWaterMark;
    }

    public void setStoreUsageHighWaterMark(int storeUsageHighWaterMark) {
        this.storeUsageHighWaterMark = storeUsageHighWaterMark;
    }

    public int getStoreUsageHighWaterMark() {
        return storeUsageHighWaterMark;
    }

    public void setSlowConsumerStrategy(SlowConsumerStrategy slowConsumerStrategy) {
        this.slowConsumerStrategy = slowConsumerStrategy;
    }

    public SlowConsumerStrategy getSlowConsumerStrategy() {
        return this.slowConsumerStrategy;
    }


    public boolean isPrioritizedMessages() {
        return this.prioritizedMessages;
    }

    public void setPrioritizedMessages(boolean prioritizedMessages) {
        this.prioritizedMessages = prioritizedMessages;
    }

    public void setAllConsumersExclusiveByDefault(boolean allConsumersExclusiveByDefault) {
        this.allConsumersExclusiveByDefault = allConsumersExclusiveByDefault;
    }

    public boolean isAllConsumersExclusiveByDefault() {
        return allConsumersExclusiveByDefault;
    }

    public boolean isGcInactiveDestinations() {
        return this.gcInactiveDestinations;
    }

    public void setGcInactiveDestinations(boolean gcInactiveDestinations) {
        this.gcInactiveDestinations = gcInactiveDestinations;
    }

    /**
     * @return the amount of time spent inactive before GC of the destination kicks in.
     *
     * @deprecated use getInactiveTimeoutBeforeGC instead.
     */
    @Deprecated
    public long getInactiveTimoutBeforeGC() {
        return getInactiveTimeoutBeforeGC();
    }

    /**
     * Sets the amount of time a destination is inactive before it is marked for GC
     *
     * @param inactiveTimoutBeforeGC
     *        time in milliseconds to configure as the inactive timeout.
     *
     * @deprecated use getInactiveTimeoutBeforeGC instead.
     */
    @Deprecated
    public void setInactiveTimoutBeforeGC(long inactiveTimoutBeforeGC) {
        setInactiveTimeoutBeforeGC(inactiveTimoutBeforeGC);
    }

    /**
     * @return the amount of time spent inactive before GC of the destination kicks in.
     */
    public long getInactiveTimeoutBeforeGC() {
        return this.inactiveTimeoutBeforeGC;
    }

    /**
     * Sets the amount of time a destination is inactive before it is marked for GC
     *
     * @param inactiveTimoutBeforeGC
     *        time in milliseconds to configure as the inactive timeout.
     */
    public void setInactiveTimeoutBeforeGC(long inactiveTimeoutBeforeGC) {
        this.inactiveTimeoutBeforeGC = inactiveTimeoutBeforeGC;
    }

    public void setGcWithNetworkConsumers(boolean gcWithNetworkConsumers) {
        this.gcWithNetworkConsumers = gcWithNetworkConsumers;
    }

    public boolean isGcWithNetworkConsumers() {
        return gcWithNetworkConsumers;
    }

    public boolean isReduceMemoryFootprint() {
        return reduceMemoryFootprint;
    }

    public void setReduceMemoryFootprint(boolean reduceMemoryFootprint) {
        this.reduceMemoryFootprint = reduceMemoryFootprint;
    }

    public void setNetworkBridgeFilterFactory(NetworkBridgeFilterFactory networkBridgeFilterFactory) {
        this.networkBridgeFilterFactory = networkBridgeFilterFactory;
    }

    public NetworkBridgeFilterFactory getNetworkBridgeFilterFactory() {
        return networkBridgeFilterFactory;
    }

    public boolean isDoOptimzeMessageStorage() {
        return doOptimzeMessageStorage;
    }

    public void setDoOptimzeMessageStorage(boolean doOptimzeMessageStorage) {
        this.doOptimzeMessageStorage = doOptimzeMessageStorage;
    }

    public int getOptimizeMessageStoreInFlightLimit() {
        return optimizeMessageStoreInFlightLimit;
    }

    public void setOptimizeMessageStoreInFlightLimit(int optimizeMessageStoreInFlightLimit) {
        this.optimizeMessageStoreInFlightLimit = optimizeMessageStoreInFlightLimit;
    }

    public void setPersistJMSRedelivered(boolean val) {
        this.persistJMSRedelivered = val;
    }

    public boolean isPersistJMSRedelivered() {
        return persistJMSRedelivered;
    }

    public int getMaxDestinations() {
        return maxDestinations;
    }

    /**
     * Sets the maximum number of destinations that can be created
     *
     * @param maxDestinations
     *            maximum number of destinations
     */
    public void setMaxDestinations(int maxDestinations) {
        this.maxDestinations = maxDestinations;
    }

    @Override
    public String toString() {
        return "PolicyEntry [" + destination + "]";
    }
}
