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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

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
    private boolean useQueueForQueueMessages = true;
    private boolean useQueueForTopicMessages = true;

    public ActiveMQDestination getDeadLetterQueueFor(ActiveMQDestination originalDestination) {
        if (originalDestination.isQueue()) {
            return createDestination(originalDestination, queuePrefix, useQueueForQueueMessages);
        } else {
            return createDestination(originalDestination, topicPrefix, useQueueForTopicMessages);
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

    // Implementation methods
    // -------------------------------------------------------------------------
    protected ActiveMQDestination createDestination(ActiveMQDestination originalDestination, String prefix, boolean useQueue) {
        String name = prefix + originalDestination.getPhysicalName();
        if (useQueue) {
            return new ActiveMQQueue(name);
        } else {
            return new ActiveMQTopic(name);
        }
    }

}
