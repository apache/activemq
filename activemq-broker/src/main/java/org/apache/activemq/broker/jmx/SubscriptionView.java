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

import java.io.IOException;
import java.util.Set;

import javax.jms.InvalidSelectorException;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.util.IOExceptionSupport;

/**
 *
 */
public class SubscriptionView implements SubscriptionViewMBean {

    protected final Subscription subscription;
    protected final String clientId;
    protected final String userName;

    /**
     * Constructor
     *
     * @param subs
     */
    public SubscriptionView(String clientId, String userName, Subscription subs) {
        this.clientId = clientId;
        this.subscription = subs;
        this.userName = userName;
    }

    /**
     * @return the clientId
     */
    @Override
    public String getClientId() {
        return clientId;
    }

    /**
     * @returns the ObjectName of the Connection that created this subscription
     */
    @Override
    public ObjectName getConnection() {
        ObjectName result = null;

        if (clientId != null && subscription != null) {
            ConnectionContext ctx = subscription.getContext();
            if (ctx != null && ctx.getBroker() != null && ctx.getBroker().getBrokerService() != null) {
                BrokerService service = ctx.getBroker().getBrokerService();
                ManagementContext managementCtx = service.getManagementContext();
                if (managementCtx != null) {

                    try {
                        ObjectName query = createConnectionQuery(managementCtx, service.getBrokerName());
                        Set<ObjectName> names = managementCtx.queryNames(query, null);
                        if (names.size() == 1) {
                            result = names.iterator().next();
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }
        return result;
    }



    private ObjectName createConnectionQuery(ManagementContext ctx, String brokerName) throws IOException {
        try {
            return BrokerMBeanSupport.createConnectionQuery(ctx.getJmxDomainName(), brokerName, clientId);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * @return the id of the Connection the Subscription is on
     */
    @Override
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
    @Override
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
    @Override
    public long getSubscriptionId() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            return info.getConsumerId().getValue();
        }
        return 0;
    }

    /**
     * @return the destination name
     */
    @Override
    public String getDestinationName() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            ActiveMQDestination dest = info.getDestination();
            return dest.getPhysicalName();
        }
        return "NOTSET";
    }

    @Override
    public String getSelector() {
        if (subscription != null) {
            return subscription.getSelector();
        }
        return null;
    }

    @Override
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
    @Override
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
    @Override
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
    @Override
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
    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public boolean isNetwork() {
        ConsumerInfo info = getConsumerInfo();
        if (info != null) {
            return info.isNetworkSubscription();
        }
        return false;
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
    @Override
    public boolean isRetroactive() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isRetroactive() : false;
    }

    /**
     * @return whether or not the subscriber is an exclusive consumer
     */
    @Override
    public boolean isExclusive() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isExclusive() : false;
    }

    /**
     * @return whether or not the subscriber is durable (persistent)
     */
    @Override
    public boolean isDurable() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isDurable() : false;
    }

    /**
     * @return whether or not the subscriber ignores local messages
     */
    @Override
    public boolean isNoLocal() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isNoLocal() : false;
    }

    /**
     * @return whether or not the subscriber is configured for async dispatch
     */
    @Override
    public boolean isDispatchAsync() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.isDispatchAsync() : false;
    }

    /**
     * @return the maximum number of pending messages allowed in addition to the
     *         prefetch size. If enabled to a non-zero value then this will
     *         perform eviction of messages for slow consumers on non-durable
     *         topics.
     */
    @Override
    public int getMaximumPendingMessageLimit() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getMaximumPendingMessageLimit() : 0;
    }

    /**
     * @return the consumer priority
     */
    @Override
    public byte getPriority() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getPriority() : 0;
    }

    /**
     * @return the name of the consumer which is only used for durable
     *         consumers.
     */
    @Override
    public String getSubscriptionName() {
        ConsumerInfo info = getConsumerInfo();
        return info != null ? info.getSubscriptionName() : null;
    }

    /**
     * @return number of messages pending delivery
     */
    @Override
    public int getPendingQueueSize() {
        return subscription != null ? subscription.getPendingQueueSize() : 0;
    }

    /**
     * @return number of messages dispatched
     */
    @Override
    public int getDispatchedQueueSize() {
        return subscription != null ? subscription.getDispatchedQueueSize() : 0;
    }

    @Override
    public int getMessageCountAwaitingAcknowledge() {
        return getDispatchedQueueSize();
    }

    /**
     * @return number of messages that matched the subscription
     */
    @Override
    public long getDispatchedCounter() {
        return subscription != null ? subscription.getDispatchedCounter() : 0;
    }

    /**
     * @return number of messages that matched the subscription
     */
    @Override
    public long getEnqueueCounter() {
        return subscription != null ? subscription.getEnqueueCounter() : 0;
    }

    /**
     * @return number of messages queued by the client
     */
    @Override
    public long getDequeueCounter() {
        return subscription != null ? subscription.getDequeueCounter() : 0;
    }

    protected ConsumerInfo getConsumerInfo() {
        return subscription != null ? subscription.getConsumerInfo() : null;
    }

    /**
     * @return pretty print
     */
    @Override
    public String toString() {
        return "SubscriptionView: " + getClientId() + ":" + getConnectionId();
    }

    /**
     */
    @Override
    public int getPrefetchSize() {
        return subscription != null ? subscription.getPrefetchSize() : 0;
    }

    @Override
    public boolean isMatchingQueue(String queueName) {
        if (isDestinationQueue()) {
            return matchesDestination(new ActiveMQQueue(queueName));
        }
        return false;
    }

    @Override
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

    @Override
    public boolean isSlowConsumer() {
        return subscription.isSlowConsumer();
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public void resetStatistics() {
        if (subscription != null && subscription.getSubscriptionStatistics() != null){
            subscription.getSubscriptionStatistics().reset();
        }
    }

    @Override
    public long getConsumedCount() {
        return subscription != null ? subscription.getConsumedCount() : 0;
    }
}
