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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.group.MessageGroupHashBucketFactory;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.memory.UsageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private DeadLetterStrategy deadLetterStrategy;
    private PendingMessageLimitStrategy pendingMessageLimitStrategy;
    private MessageEvictionStrategy messageEvictionStrategy;
    private long memoryLimit;
    private MessageGroupMapFactory messageGroupMapFactory;
    private PendingQueueMessageStoragePolicy pendingQueuePolicy;
    private PendingDurableSubscriberMessageStoragePolicy pendingDurableSubscriberPolicy;
    private PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy;

    public void configure(Queue queue, Store tmpStore) {
        if (dispatchPolicy != null) {
            queue.setDispatchPolicy(dispatchPolicy);
        }
        if (deadLetterStrategy != null) {
            queue.setDeadLetterStrategy(deadLetterStrategy);
        }
        queue.setMessageGroupMapFactory(getMessageGroupMapFactory());
        if (memoryLimit > 0) {
            queue.getUsageManager().setLimit(memoryLimit);
        }
        if (pendingQueuePolicy != null) {
            PendingMessageCursor messages = pendingQueuePolicy.getQueuePendingMessageCursor(queue, tmpStore);
            queue.setMessages(messages);
        }
    }

    public void configure(Topic topic) {
        if (dispatchPolicy != null) {
            topic.setDispatchPolicy(dispatchPolicy);
        }
        if (deadLetterStrategy != null) {
            topic.setDeadLetterStrategy(deadLetterStrategy);
        }
        if (subscriptionRecoveryPolicy != null) {
            topic.setSubscriptionRecoveryPolicy(subscriptionRecoveryPolicy.copy());
        }
        topic.setSendAdvisoryIfNoConsumers(sendAdvisoryIfNoConsumers);
        if (memoryLimit > 0) {
            topic.getUsageManager().setLimit(memoryLimit);
        }
    }

    public void configure(Broker broker, UsageManager memoryManager, TopicSubscription subscription) {
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
            int maxBatchSize = subscription.getConsumerInfo().getPrefetchSize();
            subscription.setMatched(pendingSubscriberPolicy.getSubscriberPendingMessageCursor(name, broker.getTempDataStore(), maxBatchSize));
        }
    }

    public void configure(Broker broker, UsageManager memoryManager, DurableTopicSubscription sub) {
        String clientId = sub.getClientId();
        String subName = sub.getSubscriptionName();
        int prefetch = sub.getPrefetchSize();
        if (pendingDurableSubscriberPolicy != null) {
            PendingMessageCursor cursor = pendingDurableSubscriberPolicy.getSubscriberPendingMessageCursor(clientId, subName, broker.getTempDataStore(), prefetch);
            cursor.setUsageManager(memoryManager);
            sub.setPending(cursor);
        }
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

}
