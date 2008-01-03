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
 * @version $Revision: 1.5 $
 */
public interface SubscriptionViewMBean {

    /**
     * @return the clientId of the Connection the Subscription is on
     */
    String getClientId();

    /**
     * @return the id of the Connection the Subscription is on
     */
    String getConnectionId();

    /**
     * @return the id of the Session the subscription is on
     */
    long getSessionId();

    /**
     * @return the id of the Subscription
     */
    long getSubcriptionId();

    /**
     * @return the destination name
     */
    String getDestinationName();

    /**
     * @return the JMS selector on the current subscription
     */
    String getSelector();

    /**
     * Attempts to change the current active selector on the subscription. This
     * operation is not supported for persistent topics.
     */
    void setSelector(String selector) throws InvalidSelectorException, UnsupportedOperationException;

    /**
     * @return true if the destination is a Queue
     */
    boolean isDestinationQueue();

    /**
     * @return true of the destination is a Topic
     */
    boolean isDestinationTopic();

    /**
     * @return true if the destination is temporary
     */
    boolean isDestinationTemporary();

    /**
     * @return true if the subscriber is active
     */
    boolean isActive();

    /**
     * @return number of messages pending delivery
     */
    int getPendingQueueSize();

    /**
     * @return number of messages dispatched
     */
    int getDispatchedQueueSize();
    
    /**
     * The same as the number of messages dispatched - 
     * making it explicit
     * @return
     */
    int getMessageCountAwaitingAcknowledge();

    /**
     * @return number of messages that matched the subscription
     */
    long getDispachedCounter();

    /**
     * @return number of messages that matched the subscription
     */
    long getEnqueueCounter();

    /**
     * @return number of messages queued by the client
     */
    long getDequeueCounter();

    /**
     * @return the prefetch that has been configured for this subscriber
     */
    int getPrefetchSize();

    /**
     * @return whether or not the subscriber is retroactive or not
     */
    boolean isRetroactive();

    /**
     * @return whether or not the subscriber is an exclusive consumer
     */
    boolean isExclusive();

    /**
     * @return whether or not the subscriber is durable (persistent)
     */
    boolean isDurable();

    /**
     * @return whether or not the subscriber ignores local messages
     */
    boolean isNoLocal();

    /**
     * @return the maximum number of pending messages allowed in addition to the
     *         prefetch size. If enabled to a non-zero value then this will
     *         perform eviction of messages for slow consumers on non-durable
     *         topics.
     */
    int getMaximumPendingMessageLimit();

    /**
     * @return the consumer priority
     */
    byte getPriority();

    /**
     * @return the name of the consumer which is only used for durable
     *         consumers.
     */
    String getSubcriptionName();
}
