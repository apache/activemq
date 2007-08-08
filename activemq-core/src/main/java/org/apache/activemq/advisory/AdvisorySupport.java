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
package org.apache.activemq.advisory;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.Destination;

public class AdvisorySupport {

    public static final String ADVISORY_TOPIC_PREFIX = "ActiveMQ.Advisory.";
    public static final ActiveMQTopic CONNECTION_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "Connection");
    public static final ActiveMQTopic QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "Queue");
    public static final ActiveMQTopic TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "Topic");
    public static final ActiveMQTopic TEMP_QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "TempQueue");
    public static final ActiveMQTopic TEMP_TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "TempTopic");
    public static final String PRODUCER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Producer.";
    public static final String QUEUE_PRODUCER_ADVISORY_TOPIC_PREFIX = PRODUCER_ADVISORY_TOPIC_PREFIX + "Queue.";
    public static final String TOPIC_PRODUCER_ADVISORY_TOPIC_PREFIX = PRODUCER_ADVISORY_TOPIC_PREFIX + "Topic.";
    public static final String CONSUMER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Consumer.";
    public static final String QUEUE_CONSUMER_ADVISORY_TOPIC_PREFIX = CONSUMER_ADVISORY_TOPIC_PREFIX + "Queue.";
    public static final String TOPIC_CONSUMER_ADVISORY_TOPIC_PREFIX = CONSUMER_ADVISORY_TOPIC_PREFIX + "Topic.";
    public static final String EXPIRED_TOPIC_MESSAGES_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Expired.Topic.";
    public static final String EXPIRED_QUEUE_MESSAGES_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Expired.Queue.";
    public static final String NO_TOPIC_CONSUMERS_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "NoConsumer.Topic.";
    public static final String NO_QUEUE_CONSUMERS_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "NoConsumer.Queue.";
    public static final String AGENT_TOPIC = "ActiveMQ.Agent";

    public static final String ADIVSORY_MESSAGE_TYPE = "Advisory";
    public static final ActiveMQTopic TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC = new ActiveMQTopic(TEMP_QUEUE_ADVISORY_TOPIC + "," + TEMP_TOPIC_ADVISORY_TOPIC);
    private static final ActiveMQTopic AGENT_TOPIC_DESTINATION = new ActiveMQTopic(AGENT_TOPIC);

    public static ActiveMQTopic getConnectionAdvisoryTopic() {
        return CONNECTION_ADVISORY_TOPIC;
    }

    public static ActiveMQTopic getConsumerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isQueue())
            return new ActiveMQTopic(QUEUE_CONSUMER_ADVISORY_TOPIC_PREFIX + destination.getPhysicalName());
        else
            return new ActiveMQTopic(TOPIC_CONSUMER_ADVISORY_TOPIC_PREFIX + destination.getPhysicalName());
    }

    public static ActiveMQTopic getProducerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isQueue())
            return new ActiveMQTopic(QUEUE_PRODUCER_ADVISORY_TOPIC_PREFIX + destination.getPhysicalName());
        else
            return new ActiveMQTopic(TOPIC_PRODUCER_ADVISORY_TOPIC_PREFIX + destination.getPhysicalName());
    }

    public static ActiveMQTopic getExpiredMessageTopic(ActiveMQDestination destination) {
        if (destination.isQueue()) {
            return getExpiredQueueMessageAdvisoryTopic(destination);
        }
        return getExpiredTopicMessageAdvisoryTopic(destination);
    }

    public static ActiveMQTopic getExpiredTopicMessageAdvisoryTopic(ActiveMQDestination destination) {
        String name = EXPIRED_TOPIC_MESSAGES_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getExpiredQueueMessageAdvisoryTopic(ActiveMQDestination destination) {
        String name = EXPIRED_QUEUE_MESSAGES_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getNoTopicConsumersAdvisoryTopic(ActiveMQDestination destination) {
        String name = NO_TOPIC_CONSUMERS_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getNoQueueConsumersAdvisoryTopic(ActiveMQDestination destination) {
        String name = NO_QUEUE_CONSUMERS_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getDestinationAdvisoryTopic(ActiveMQDestination destination) {
        switch (destination.getDestinationType()) {
        case ActiveMQDestination.QUEUE_TYPE:
            return QUEUE_ADVISORY_TOPIC;
        case ActiveMQDestination.TOPIC_TYPE:
            return TOPIC_ADVISORY_TOPIC;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return TEMP_QUEUE_ADVISORY_TOPIC;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return TEMP_TOPIC_ADVISORY_TOPIC;
        }
        throw new RuntimeException("Unknown destination type: " + destination.getDestinationType());
    }

    public static boolean isDestinationAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isDestinationAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.equals(TEMP_QUEUE_ADVISORY_TOPIC) || destination.equals(TEMP_TOPIC_ADVISORY_TOPIC) || destination.equals(QUEUE_ADVISORY_TOPIC)
                   || destination.equals(TOPIC_ADVISORY_TOPIC);
        }
    }

    public static boolean isAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(ADVISORY_TOPIC_PREFIX);
        }
    }

    public static boolean isConnectionAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isConnectionAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.equals(CONNECTION_ADVISORY_TOPIC);
        }
    }

    public static boolean isProducerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isProducerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(PRODUCER_ADVISORY_TOPIC_PREFIX);
        }
    }

    public static boolean isConsumerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isConsumerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(CONSUMER_ADVISORY_TOPIC_PREFIX);
        }
    }

    /**
     * Returns the agent topic which is used to send commands to the broker
     */
    public static Destination getAgentDestination() {
        return AGENT_TOPIC_DESTINATION;
    }
}
