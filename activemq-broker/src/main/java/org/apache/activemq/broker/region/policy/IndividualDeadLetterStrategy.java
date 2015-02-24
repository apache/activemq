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

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;

/**
 * A {@link DeadLetterStrategy} where each destination has its own individual
 * DLQ using the subject naming hierarchy.
 *
 * @org.apache.xbean.XBean
 *
 */
public class IndividualDeadLetterStrategy extends AbstractDeadLetterStrategy {

    private String topicPrefix = "ActiveMQ.DLQ.Topic.";
    private String queuePrefix = "ActiveMQ.DLQ.Queue.";
    private String topicSuffix;
    private String queueSuffix;
    private boolean useQueueForQueueMessages = true;
    private boolean useQueueForTopicMessages = true;
    private boolean destinationPerDurableSubscriber;

    public ActiveMQDestination getDeadLetterQueueFor(Message message, Subscription subscription) {
        if (message.getDestination().isQueue()) {
            return createDestination(message, queuePrefix, queueSuffix, useQueueForQueueMessages, subscription);
        } else {
            return createDestination(message, topicPrefix, topicSuffix, useQueueForTopicMessages, subscription);
        }
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getQueuePrefix() {
        return queuePrefix;
    }

    /**
     * Sets the prefix to use for all dead letter queues for queue messages
     */
    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    /**
     * Sets the prefix to use for all dead letter queues for topic messages
     */
    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getQueueSuffix() {
        return queueSuffix;
    }

    /**
     * Sets the suffix to use for all dead letter queues for queue messages
     */
    public void setQueueSuffix(String queueSuffix) {
        this.queueSuffix = queueSuffix;
    }

    public String getTopicSuffix() {
        return topicSuffix;
    }

    /**
     * Sets the suffix to use for all dead letter queues for topic messages
     */
    public void setTopicSuffix(String topicSuffix) {
        this.topicSuffix = topicSuffix;
    }

    public boolean isUseQueueForQueueMessages() {
        return useQueueForQueueMessages;
    }

    /**
     * Sets whether a queue or topic should be used for queue messages sent to a
     * DLQ. The default is to use a Queue
     */
    public void setUseQueueForQueueMessages(boolean useQueueForQueueMessages) {
        this.useQueueForQueueMessages = useQueueForQueueMessages;
    }

    public boolean isUseQueueForTopicMessages() {
        return useQueueForTopicMessages;
    }

    /**
     * Sets whether a queue or topic should be used for topic messages sent to a
     * DLQ. The default is to use a Queue
     */
    public void setUseQueueForTopicMessages(boolean useQueueForTopicMessages) {
        this.useQueueForTopicMessages = useQueueForTopicMessages;
    }

    public boolean isDestinationPerDurableSubscriber() {
        return destinationPerDurableSubscriber;
    }

    /**
     * sets whether durable topic subscriptions are to get individual dead letter destinations.
     * When true, the DLQ is of the form 'topicPrefix.clientId:subscriptionName'
     * The default is false.
     * @param destinationPerDurableSubscriber
     */
    public void setDestinationPerDurableSubscriber(boolean destinationPerDurableSubscriber) {
        this.destinationPerDurableSubscriber = destinationPerDurableSubscriber;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected ActiveMQDestination createDestination(Message message,
                                                    String prefix,
                                                    String suffix,
                                                    boolean useQueue,
                                                    Subscription subscription ) {
        String name = null;

        Destination regionDestination = (Destination) message.getRegionDestination();
        if (regionDestination != null
                && regionDestination.getActiveMQDestination() != null
                && regionDestination.getActiveMQDestination().getPhysicalName() != null
                && !regionDestination.getActiveMQDestination().getPhysicalName().isEmpty()){
            name = prefix + regionDestination.getActiveMQDestination().getPhysicalName();
        } else {
            name = prefix + message.getDestination().getPhysicalName();
        }

        if (destinationPerDurableSubscriber && subscription instanceof DurableTopicSubscription) {
            name += "." + ((DurableTopicSubscription)subscription).getSubscriptionKey();
        }

        if (suffix != null && !suffix.isEmpty()) {
            name += suffix;
        }

        if (useQueue) {
            return new ActiveMQQueue(name);
        } else {
            return new ActiveMQTopic(name);
        }
    }

}