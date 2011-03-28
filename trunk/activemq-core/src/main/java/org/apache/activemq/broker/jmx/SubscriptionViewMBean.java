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

/**
 * 
 */
public interface SubscriptionViewMBean {

    /**
     * @return the clientId of the Connection the Subscription is on
     */
    @MBeanInfo("JMS Client id of the Connection the Subscription is on.")
    String getClientId();

    /**
     * @return the id of the Connection the Subscription is on
     */
    @MBeanInfo("ID of the Connection the Subscription is on.")
    String getConnectionId();

    /**
     * @return the id of the Session the subscription is on
     */
    @MBeanInfo("ID of the Session the Subscription is on.")
    long getSessionId();

    /**
     * @return the id of the Subscription
     */
    @MBeanInfo("ID of the Subscription.")
    long getSubcriptionId();

    /**
     * @return the destination name
     */
    @MBeanInfo("The name of the destionation the subscription is on.")
    String getDestinationName();

    /**
     * @return the JMS selector on the current subscription
     */
    @MBeanInfo("The SQL-92 message header selector or XPATH body selector of the subscription.")
    String getSelector();

    /**
     * Attempts to change the current active selector on the subscription. This
     * operation is not supported for persistent topics.
     */
    void setSelector(@MBeanInfo("selector") String selector) throws InvalidSelectorException, UnsupportedOperationException;

    /**
     * @return true if the destination is a Queue
     */
    @MBeanInfo("Subscription is on a Queue")
    boolean isDestinationQueue();

    /**
     * @return true of the destination is a Topic
     */
    @MBeanInfo("Subscription is on a Topic")
    boolean isDestinationTopic();

    /**
     * @return true if the destination is temporary
     */
    @MBeanInfo("Subscription is on a temporary Queue/Topic")
    boolean isDestinationTemporary();

    /**
     * @return true if the subscriber is active
     */
    @MBeanInfo("Subscription is active (connected and receiving messages).")
    boolean isActive();

    /**
     * @return number of messages pending delivery
     */
    @MBeanInfo("Number of messages pending delivery.")
    int getPendingQueueSize();

    /**
     * @return number of messages dispatched
     */
    @MBeanInfo("Number of messages dispatched awaiting acknowledgement.")
    int getDispatchedQueueSize();
    
    /**
     * The same as the number of messages dispatched - 
     * making it explicit
     * @return
     */
    @MBeanInfo("Number of messages dispatched awaiting acknowledgement.")
    int getMessageCountAwaitingAcknowledge();

    /**
     * @return number of messages that matched the subscription
     */
    @MBeanInfo("Number of messages that sent to the client.")
    long getDispatchedCounter();

    /**
     * @return number of messages that matched the subscription
     */
    @MBeanInfo("Number of messages that matched the subscription.")
    long getEnqueueCounter();

    /**
     * @return number of messages queued by the client
     */
    @MBeanInfo("Number of messages were sent to and acknowledge by the client.")
    long getDequeueCounter();

    /**
     * @return the prefetch that has been configured for this subscriber
     */
    @MBeanInfo("Number of messages to pre-fetch and dispatch to the client.")
    int getPrefetchSize();

    /**
     * @return whether or not the subscriber is retroactive or not
     */
    @MBeanInfo("The subscriber is retroactive (tries to receive broadcasted topic messages sent prior to connecting)")
    boolean isRetroactive();

    /**
     * @return whether or not the subscriber is an exclusive consumer
     */
    @MBeanInfo("The subscriber is exclusive (no other subscribers may receive messages from the destination as long as this one is)")
    boolean isExclusive();

    /**
     * @return whether or not the subscriber is durable (persistent)
     */
    @MBeanInfo("The subsription is persistent.")
    boolean isDurable();

    /**
     * @return whether or not the subscriber ignores local messages
     */
    @MBeanInfo("The subsription ignores local messages.")
    boolean isNoLocal();

    /**
     * @return the maximum number of pending messages allowed in addition to the
     *         prefetch size. If enabled to a non-zero value then this will
     *         perform eviction of messages for slow consumers on non-durable
     *         topics.
     */
    @MBeanInfo("The maximum number of pending messages allowed (in addition to the prefetch size).")
    int getMaximumPendingMessageLimit();

    /**
     * @return the consumer priority
     */
    @MBeanInfo("The subscription priority")
    byte getPriority();

    /**
     * @return the name of the consumer which is only used for durable
     *         consumers.
     */
    @MBeanInfo("The name of the subscription (durable subscriptions only).")
    String getSubcriptionName();

    /**
     * Returns true if this subscription (which may be using wildcards) matches the given queue name
     *
     * @param queueName the JMS queue name to match against
     * @return true if this subscription matches the given queue or false if not
     */
    @MBeanInfo("Returns true if the subscription (which may be using wildcards) matches the given queue name")
    boolean isMatchingQueue(String queueName);

    /**
     * Returns true if this subscription (which may be using wildcards) matches the given topic name
     *
     * @param topicName the JMS topic name to match against
     * @return true if this subscription matches the given topic or false if not
     */
    @MBeanInfo("Returns true if the subscription (which may be using wildcards) matches the given topic name")
    boolean isMatchingTopic(String topicName);
}
