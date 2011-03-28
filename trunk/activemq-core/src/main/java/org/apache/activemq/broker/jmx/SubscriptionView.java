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

import javax.jms.InvalidSelectorException;

import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationFilter;

/**
 * 
 */
public class SubscriptionView implements SubscriptionViewMBean {

    protected final Subscription subscription;
    protected final String clientId;

    /**
     * Constructor
     * 
     * @param subs
     */
    public SubscriptionView(String clientId, Subscription subs) {
        this.clientId = clientId;
        this.subscription = subs;
    }

    /**
     * @return the clientId
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * @return the id of the Connection the Subscription is on
     */
    public String getConnectionId() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            return info.getConsumerId().getConnectionId();
        }
        return "NOTSET";
    }

    /**
     * @return the id of the Session the subscription is on
     */
    public long getSessionId() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            return info.getConsumerId().getSessionId();
        }
        return 0;
    }

    /**
     * @return the id of the Subscription
     */
    public long getSubcriptionId() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            return info.getConsumerId().getValue();
        }
        return 0;
    }

    /**
     * @return the destination name
     */
    public String getDestinationName() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.getPhysicalName();
        }
        return "NOTSET";
    }

    public String getSelector() {
        if (subscription != null) {
            return subscription.getSelector();
        }
        return null;
    }

    public void setSelector(String selector) throws InvalidSelectorException, UnsupportedOperationException {
        if (subscription != null) {
            subscription.setSelector(selector);
        } else {
            throw new UnsupportedOperationException("No subscription object");
        }
    }

    /**
     * @return true if the destination is a Queue
     */
    public boolean isDestinationQueue() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.isQueue();
        }
        return false;
    }

    /**
     * @return true of the destination is a Topic
     */
    public boolean isDestinationTopic() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.isTopic();
        }
        return false;
    }

    /**
     * @return true if the destination is temporary
     */
    public boolean isDestinationTemporary() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.isTemporary();
        }
        return false;
    }

    /**
     * @return true if the subscriber is active
     */
    public boolean isActive() {
        return true;
    }

    /**
     * The subscription should release as may references as it can to help the
     * garbage collector reclaim memory.
     */
    public void gc() {
        if (subscription != null) {
            subscription.gc();
        }
    }

    /**
     * @return whether or not the subscriber is retroactive or not
     */
    public boolean isRetroactive() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isRetroactive() : false;
    }

    /**
     * @return whether or not the subscriber is an exclusive consumer
     */
    public boolean isExclusive() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isExclusive() : false;
    }

    /**
     * @return whether or not the subscriber is durable (persistent)
     */
    public boolean isDurable() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isDurable() : false;
    }

    /**
     * @return whether or not the subscriber ignores local messages
     */
    public boolean isNoLocal() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isNoLocal() : false;
    }

    /**
     * @return the maximum number of pending messages allowed in addition to the
     *         prefetch size. If enabled to a non-zero value then this will
     *         perform eviction of messages for slow consumers on non-durable
     *         topics.
     */
    public int getMaximumPendingMessageLimit() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getMaximumPendingMessageLimit() : 0;
    }

    /**
     * @return the consumer priority
     */
    public byte getPriority() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getPriority() : 0;
    }

    /**
     * @return the name of the consumer which is only used for durable
     *         consumers.
     */
    public String getSubcriptionName() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getSubscriptionName() : null;
    }

    /**
     * @return number of messages pending delivery
     */
    public int getPendingQueueSize() {
        return subscription != null ? subscription.getPendingQueueSize() : 0;
    }

    /**
     * @return number of messages dispatched
     */
    public int getDispatchedQueueSize() {
        return subscription != null ? subscription.getDispatchedQueueSize() : 0;
    }
    
    public int getMessageCountAwaitingAcknowledge() {
        return getDispatchedQueueSize();
    }

    /**
     * @return number of messages that matched the subscription
     */
    public long getDispatchedCounter() {
        return subscription != null ? subscription.getDispatchedCounter() : 0;
    }

    /**
     * @return number of messages that matched the subscription
     */
    public long getEnqueueCounter() {
        return subscription != null ? subscription.getEnqueueCounter() : 0;
    }

    /**
     * @return number of messages queued by the client
     */
    public long getDequeueCounter() {
        return subscription != null ? subscription.getDequeueCounter() : 0;
    }

    protected ConsumerInfo getConsumerInfo() {
        return subscription != null ? subscription.getConsumerInfo() : null;
    }

    /**
     * @return pretty print
     */
    public String toString() {
        return "SubscriptionView: " + getClientId() + ":" + getConnectionId();
    }

    /**
     */
    public int getPrefetchSize() {
        return subscription != null ? subscription.getPrefetchSize() : 0;
    }

    public boolean isMatchingQueue(String queueName) {
        if (isDestinationQueue()) {
            return matchesDestination(new ActiveMQQueue(queueName));
        }
        return false;
    }

    public boolean isMatchingTopic(String topicName) {
        if (isDestinationTopic()) {
            return matchesDestination(new ActiveMQTopic(topicName));
        }
        return false;
    }

    /**
     * Return true if this subscription matches the given destination
     *
     * @param destination the destination to compare against
     * @return true if this subscription matches the given destination
     */
    public boolean matchesDestination(ActiveMQDestination destination) {
        ActiveMQDestination subscriptionDestination = subscription.getActiveMQDestination();
        DestinationFilter filter = DestinationFilter.parseFilter(subscriptionDestination);
        return filter.matches(destination);
    }

}
