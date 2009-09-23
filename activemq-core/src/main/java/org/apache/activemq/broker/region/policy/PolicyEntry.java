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

import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueBrowserSubscription;
import org.apache.activemq.broker.region.QueueSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.group.MessageGroupHashBucketFactory;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.impl.sql.compile.GetCurrentConnectionNode;

/**
 * Represents an entry in a {@link PolicyMap} for assigning policies to a
 * specific destination or a hierarchical wildcard area of destinations.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision: 1.1 $
 */
public class PolicyEntry extends DestinationMapEntry {

    private static final Log LOG = LogFactory.getLog(PolicyEntry.class);
    private DispatchPolicy dispatchPolicy;
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy;
    private boolean sendAdvisoryIfNoConsumers;
    private DeadLetterStrategy deadLetterStrategy = Destination.DEFAULT_DEAD_LETTER_STRATEGY;
    private PendingMessageLimitStrategy pendingMessageLimitStrategy;
    private MessageEvictionStrategy messageEvictionStrategy;
    private long memoryLimit;
    private MessageGroupMapFactory messageGroupMapFactory;
    private PendingQueueMessageStoragePolicy pendingQueuePolicy;
    private PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy;
    private PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy;
    private int maxProducersToAudit=32;
    private int maxAuditDepth=2048;
    private int maxQueueAuditDepth=2048;
    private boolean enableAudit=true;
    private boolean producerFlowControl = true;
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
    private boolean advisdoryForFastProducers;
    private boolean advisoryForDiscardingMessages;
    private boolean advisoryWhenFull;
    private boolean advisoryForDelivery;
    private boolean advisoryForConsumed;
    private long expireMessagesPeriod = BaseDestination.EXPIRE_MESSAGE_PERIOD;
    private int maxExpirePageSize = BaseDestination.MAX_BROWSE_PAGE_SIZE;
    private int queuePrefetch=ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH;
    private int queueBrowserPrefetch=ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH;
    private int topicPrefetch=ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH;
    private int durableTopicPrefetch=ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH;
    private int cursorMemoryHighWaterMark=70;
    
   
    public void configure(Broker broker,Queue queue) {
        baseConfiguration(queue);
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
    }

    public void configure(Topic topic) {
        baseConfiguration(topic);
        if (dispatchPolicy != null) {
            topic.setDispatchPolicy(dispatchPolicy);
        }
        topic.setDeadLetterStrategy(getDeadLetterStrategy());
        if (subscriptionRecoveryPolicy != null) {
            topic.setSubscriptionRecoveryPolicy(subscriptionRecoveryPolicy.copy());
        }
        if (memoryLimit > 0) {
            topic.getMemoryUsage().setLimit(memoryLimit);
        }
        topic.setLazyDispatch(isLazyDispatch());
    }
    
    public void baseConfiguration(BaseDestination destination) {
        destination.setProducerFlowControl(isProducerFlowControl());
        destination.setEnableAudit(isEnableAudit());
        destination.setMaxAuditDepth(getMaxQueueAuditDepth());
        destination.setMaxProducersToAudit(getMaxProducersToAudit());
        destination.setMaxPageSize(getMaxPageSize());
        destination.setMaxBrowsePageSize(getMaxBrowsePageSize());
        destination.setUseCache(isUseCache());
        destination.setMinimumMessageSize((int) getMinimumMessageSize());
        destination.setAdvisoryForConsumed(isAdvisoryForConsumed());
        destination.setAdvisoryForDelivery(isAdvisoryForDelivery());
        destination.setAdvisoryForDiscardingMessages(isAdvisoryForDiscardingMessages());
        destination.setAdvisoryForSlowConsumers(isAdvisoryForSlowConsumers());
        destination.setAdvisdoryForFastProducers(isAdvisdoryForFastProducers());
        destination.setAdvisoryWhenFull(isAdvisoryWhenFull());
        destination.setSendAdvisoryIfNoConsumers(sendAdvisoryIfNoConsumers);
        destination.setExpireMessagesPeriod(getExpireMessagesPeriod());
        destination.setMaxExpirePageSize(getMaxExpirePageSize());
        destination.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
    }

    public void configure(Broker broker, SystemUsage memoryManager, TopicSubscription subscription) {
        if (pendingMessageLimitStrategy != null) {
            int value = pendingMessageLimitStrategy.getMaximumPendingMessageLimit(subscription);
            int consumerLimit = subscription.getInfo().getMaximumPendingMessageLimit();
            if (consumerLimit > 0) {
                if (value < 0 || consumerLimit < value) {
                    value = consumerLimit;
                }
            }
            if (value >= 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting the maximumPendingMessages size to: " + value + " for consumer: " + subscription.getInfo().getConsumerId());
                }
                subscription.setMaximumPendingMessages(value);
            }
        }
        if (messageEvictionStrategy != null) {
            subscription.setMessageEvictionStrategy(messageEvictionStrategy);
        }
        if (pendingSubscriberPolicy != null) {
            String name = subscription.getContext().getClientId() + "_" + subscription.getConsumerInfo().getConsumerId();
            //override prefetch size if not set by the Consumer
            int prefetch=subscription.getConsumerInfo().getPrefetchSize();
            if (prefetch == ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH){
                subscription.getConsumerInfo().setPrefetchSize(getTopicPrefetch());
            }
            int maxBatchSize = subscription.getConsumerInfo().getPrefetchSize();
            subscription.setMatched(pendingSubscriberPolicy.getSubscriberPendingMessageCursor(broker,name, maxBatchSize));
        }
    }

    public void configure(Broker broker, SystemUsage memoryManager, DurableTopicSubscription sub) {
        String clientId = sub.getSubscriptionKey().getClientId();
        String subName = sub.getSubscriptionKey().getSubscriptionName();
        int prefetch = sub.getPrefetchSize();
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
        //override prefetch size if not set by the Consumer
        if (prefetch == ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH){
            sub.setPrefetchSize(getDurableTopicPrefetch());
        }
        if (pendingDurableSubscriberPolicy != null) {
            PendingMessageCursor cursor = pendingDurableSubscriberPolicy.getSubscriberPendingMessageCursor(broker,clientId, subName,prefetch,sub);
            cursor.setSystemUsage(memoryManager);
            sub.setPending(cursor);
        }
        sub.setMaxAuditDepth(getMaxAuditDepth());
        sub.setMaxProducersToAudit(getMaxProducersToAudit());
        
    }
    
    public void configure(Broker broker, SystemUsage memoryManager, QueueBrowserSubscription sub) {
       
        int prefetch = sub.getPrefetchSize();
        //override prefetch size if not set by the Consumer
        
        if (prefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_BROWSER_PREFETCH){
            sub.setPrefetchSize(getQueueBrowserPrefetch());
        }
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
    }
    
    public void configure(Broker broker, SystemUsage memoryManager, QueueSubscription sub) {
        
        int prefetch = sub.getPrefetchSize();
        //override prefetch size if not set by the Consumer
        
        if (prefetch == ActiveMQPrefetchPolicy.DEFAULT_QUEUE_PREFETCH){
            sub.setPrefetchSize(getQueuePrefetch());
        }
        sub.setCursorMemoryHighWaterMark(getCursorMemoryHighWaterMark());
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
            messageGroupMapFactory = new MessageGroupHashBucketFactory();
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
    public boolean isAdvisdoryForFastProducers() {
        return advisdoryForFastProducers;
    }

    /**
     * @param advisdoryForFastProducers the advisdoryForFastProducers to set
     */
    public void setAdvisdoryForFastProducers(boolean advisdoryForFastProducers) {
        this.advisdoryForFastProducers = advisdoryForFastProducers;
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
    
    public int getCursorMemoryHighWaterMark() {
		return this.cursorMemoryHighWaterMark;
	}

	public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark) {
		this.cursorMemoryHighWaterMark = cursorMemoryHighWaterMark;
	}



}
