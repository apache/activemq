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

import java.util.ArrayList;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;

public final class AdvisorySupport {
    public static final String ADVISORY_TOPIC_PREFIX = "ActiveMQ.Advisory.";
    public static final ActiveMQTopic CONNECTION_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX
            + "Connection");
    public static final ActiveMQTopic QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "Queue");
    public static final ActiveMQTopic TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "Topic");
    public static final ActiveMQTopic TEMP_QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "TempQueue");
    public static final ActiveMQTopic TEMP_TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX + "TempTopic");
    public static final String PRODUCER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Producer.";
    public static final String QUEUE_PRODUCER_ADVISORY_TOPIC_PREFIX = PRODUCER_ADVISORY_TOPIC_PREFIX + "Queue.";
    public static final String TOPIC_PRODUCER_ADVISORY_TOPIC_PREFIX = PRODUCER_ADVISORY_TOPIC_PREFIX + "Topic.";
    public static final String CONSUMER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Consumer.";
    public static final String VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "VirtualDestination.Consumer.";
    public static final String QUEUE_CONSUMER_ADVISORY_TOPIC_PREFIX = CONSUMER_ADVISORY_TOPIC_PREFIX + "Queue.";
    public static final String TOPIC_CONSUMER_ADVISORY_TOPIC_PREFIX = CONSUMER_ADVISORY_TOPIC_PREFIX + "Topic.";
    public static final String QUEUE_VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX = VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX + "Queue.";
    public static final String TOPIC_VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX = VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX + "Topic.";
    public static final String EXPIRED_TOPIC_MESSAGES_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Expired.Topic.";
    public static final String EXPIRED_QUEUE_MESSAGES_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "Expired.Queue.";
    public static final String NO_TOPIC_CONSUMERS_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "NoConsumer.Topic.";
    public static final String NO_QUEUE_CONSUMERS_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "NoConsumer.Queue.";
    public static final String SLOW_CONSUMER_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "SlowConsumer.";
    public static final String FAST_PRODUCER_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "FastProducer.";
    public static final String MESSAGE_DISCAREDED_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "MessageDiscarded.";
    public static final String FULL_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "FULL.";
    public static final String MESSAGE_DELIVERED_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "MessageDelivered.";
    public static final String MESSAGE_CONSUMED_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "MessageConsumed.";
    public static final String MESSAGE_DLQ_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "MessageDLQd.";
    public static final String MASTER_BROKER_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "MasterBroker";
    public static final String NETWORK_BRIDGE_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX + "NetworkBridge";
    public static final String NETWORK_BRIDGE_FORWARD_FAILURE_TOPIC_PREFIX = NETWORK_BRIDGE_TOPIC_PREFIX + ".ForwardFailure";
    public static final String AGENT_TOPIC = "ActiveMQ.Agent";
    public static final String ADIVSORY_MESSAGE_TYPE = "Advisory";
    public static final String MSG_PROPERTY_ORIGIN_BROKER_ID = "originBrokerId";
    public static final String MSG_PROPERTY_ORIGIN_BROKER_NAME = "originBrokerName";
    public static final String MSG_PROPERTY_ORIGIN_BROKER_URL = "originBrokerURL";
    public static final String MSG_PROPERTY_USAGE_NAME = "usageName";
    public static final String MSG_PROPERTY_USAGE_COUNT = "usageCount";

    public static final String MSG_PROPERTY_CONSUMER_ID = "consumerId";
    public static final String MSG_PROPERTY_PRODUCER_ID = "producerId";
    public static final String MSG_PROPERTY_MESSAGE_ID = "orignalMessageId";
    public static final String MSG_PROPERTY_DESTINATION = "orignalDestination";
    public static final String MSG_PROPERTY_CONSUMER_COUNT = "consumerCount";
    public static final String MSG_PROPERTY_DISCARDED_COUNT = "discardedCount";

    public static final ActiveMQTopic ALL_DESTINATIONS_COMPOSITE_ADVISORY_TOPIC = new ActiveMQTopic(
            TOPIC_ADVISORY_TOPIC.getPhysicalName() + "," + QUEUE_ADVISORY_TOPIC.getPhysicalName() + "," +
                    TEMP_QUEUE_ADVISORY_TOPIC.getPhysicalName() + "," + TEMP_TOPIC_ADVISORY_TOPIC.getPhysicalName());
    public static final ActiveMQTopic TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC = new ActiveMQTopic(
            TEMP_QUEUE_ADVISORY_TOPIC.getPhysicalName() + "," + TEMP_TOPIC_ADVISORY_TOPIC.getPhysicalName());
    private static final ActiveMQTopic AGENT_TOPIC_DESTINATION = new ActiveMQTopic(AGENT_TOPIC);

    private AdvisorySupport() {
    }

    public static ActiveMQTopic getConnectionAdvisoryTopic() {
        return CONNECTION_ADVISORY_TOPIC;
    }

    public static ActiveMQTopic[] getAllDestinationAdvisoryTopics(Destination destination) throws JMSException {
        return getAllDestinationAdvisoryTopics(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic[] getAllDestinationAdvisoryTopics(ActiveMQDestination destination) throws JMSException {
        ArrayList<ActiveMQTopic> result = new ArrayList<ActiveMQTopic>();

        result.add(getConsumerAdvisoryTopic(destination));
        result.add(getProducerAdvisoryTopic(destination));
        result.add(getExpiredMessageTopic(destination));
        result.add(getNoConsumersAdvisoryTopic(destination));
        result.add(getSlowConsumerAdvisoryTopic(destination));
        result.add(getFastProducerAdvisoryTopic(destination));
        result.add(getMessageDiscardedAdvisoryTopic(destination));
        result.add(getMessageDeliveredAdvisoryTopic(destination));
        result.add(getMessageConsumedAdvisoryTopic(destination));
        result.add(getMessageDLQdAdvisoryTopic(destination));
        result.add(getFullAdvisoryTopic(destination));

        return result.toArray(new ActiveMQTopic[0]);
    }

    public static ActiveMQTopic getConsumerAdvisoryTopic(Destination destination) throws JMSException {
        return getConsumerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getConsumerAdvisoryTopic(ActiveMQDestination destination) {
        String prefix;
        if (destination.isQueue()) {
            prefix = QUEUE_CONSUMER_ADVISORY_TOPIC_PREFIX;
        } else {
            prefix = TOPIC_CONSUMER_ADVISORY_TOPIC_PREFIX;
        }
        return getAdvisoryTopic(destination, prefix, true);
    }

    public static ActiveMQTopic getVirtualDestinationConsumerAdvisoryTopic(ActiveMQDestination destination) {
        String prefix;
        if (destination.isQueue()) {
            prefix = QUEUE_VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX;
        } else {
            prefix = TOPIC_VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX;
        }
        return getAdvisoryTopic(destination, prefix, true);
    }

    public static ActiveMQTopic getProducerAdvisoryTopic(Destination destination) throws JMSException {
        return getProducerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getProducerAdvisoryTopic(ActiveMQDestination destination) {
        String prefix;
        if (destination.isQueue()) {
            prefix = QUEUE_PRODUCER_ADVISORY_TOPIC_PREFIX;
        } else {
            prefix = TOPIC_PRODUCER_ADVISORY_TOPIC_PREFIX;
        }
        return getAdvisoryTopic(destination, prefix, false);
    }

    private static ActiveMQTopic getAdvisoryTopic(ActiveMQDestination destination, String prefix, boolean consumerTopics) {
        return new ActiveMQTopic(prefix + destination.getPhysicalName().replaceAll(",", "&sbquo;"));
    }

    public static ActiveMQTopic getExpiredMessageTopic(Destination destination) throws JMSException {
        return getExpiredMessageTopic(ActiveMQMessageTransformation.transformDestination(destination));
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

    public static ActiveMQTopic getExpiredQueueMessageAdvisoryTopic(Destination destination) throws JMSException {
        return getExpiredQueueMessageAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getExpiredQueueMessageAdvisoryTopic(ActiveMQDestination destination) {
        String name = EXPIRED_QUEUE_MESSAGES_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getNoConsumersAdvisoryTopic(Destination destination) throws JMSException {
        return getExpiredMessageTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getNoConsumersAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isQueue()) {
            return getNoQueueConsumersAdvisoryTopic(destination);
        }
        return getNoTopicConsumersAdvisoryTopic(destination);
    }

    public static ActiveMQTopic getNoTopicConsumersAdvisoryTopic(Destination destination) throws JMSException {
        return getNoTopicConsumersAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getNoTopicConsumersAdvisoryTopic(ActiveMQDestination destination) {
        String name = NO_TOPIC_CONSUMERS_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getNoQueueConsumersAdvisoryTopic(Destination destination) throws JMSException {
        return getNoQueueConsumersAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getNoQueueConsumersAdvisoryTopic(ActiveMQDestination destination) {
        String name = NO_QUEUE_CONSUMERS_TOPIC_PREFIX + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getSlowConsumerAdvisoryTopic(Destination destination) throws JMSException {
        return getSlowConsumerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getSlowConsumerAdvisoryTopic(ActiveMQDestination destination) {
        String name = SLOW_CONSUMER_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getFastProducerAdvisoryTopic(Destination destination) throws JMSException {
        return getFastProducerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getFastProducerAdvisoryTopic(ActiveMQDestination destination) {
        String name = FAST_PRODUCER_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getMessageDiscardedAdvisoryTopic(Destination destination) throws JMSException {
        return getMessageDiscardedAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getMessageDiscardedAdvisoryTopic(ActiveMQDestination destination) {
        String name = MESSAGE_DISCAREDED_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getMessageDeliveredAdvisoryTopic(Destination destination) throws JMSException {
        return getMessageDeliveredAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getMessageDeliveredAdvisoryTopic(ActiveMQDestination destination) {
        String name = MESSAGE_DELIVERED_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getMessageConsumedAdvisoryTopic(Destination destination) throws JMSException {
        return getMessageConsumedAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getMessageConsumedAdvisoryTopic(ActiveMQDestination destination) {
        String name = MESSAGE_CONSUMED_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getMessageDLQdAdvisoryTopic(ActiveMQDestination destination) {
        String name = MESSAGE_DLQ_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getMasterBrokerAdvisoryTopic() {
        return new ActiveMQTopic(MASTER_BROKER_TOPIC_PREFIX);
    }

    public static ActiveMQTopic getNetworkBridgeAdvisoryTopic() {
        return new ActiveMQTopic(NETWORK_BRIDGE_TOPIC_PREFIX);
    }

    public static ActiveMQTopic getFullAdvisoryTopic(Destination destination) throws JMSException {
        return getFullAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static ActiveMQTopic getFullAdvisoryTopic(ActiveMQDestination destination) {
        String name = FULL_TOPIC_PREFIX + destination.getDestinationTypeAsString() + "."
                + destination.getPhysicalName();
        return new ActiveMQTopic(name);
    }

    public static ActiveMQTopic getDestinationAdvisoryTopic(Destination destination) throws JMSException {
        return getDestinationAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
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
            default:
                throw new RuntimeException("Unknown destination type: " + destination.getDestinationType());
        }
    }

    public static boolean isDestinationAdvisoryTopic(Destination destination) throws JMSException {
        return isDestinationAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isTempDestinationAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (!isTempDestinationAdvisoryTopic(compositeDestinations[i])) {
                    return false;
                }
            }
            return true;
        } else {
            return destination.equals(TEMP_QUEUE_ADVISORY_TOPIC) || destination.equals(TEMP_TOPIC_ADVISORY_TOPIC);
        }
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
            return destination.equals(TEMP_QUEUE_ADVISORY_TOPIC) || destination.equals(TEMP_TOPIC_ADVISORY_TOPIC)
                    || destination.equals(QUEUE_ADVISORY_TOPIC) || destination.equals(TOPIC_ADVISORY_TOPIC);
        }
    }

    public static boolean isAdvisoryTopic(Destination destination) throws JMSException {
        return isAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isAdvisoryTopic(ActiveMQDestination destination) {
        if (destination != null) {
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
        return false;
    }

    public static boolean isConnectionAdvisoryTopic(Destination destination) throws JMSException {
        return isConnectionAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
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

    public static boolean isProducerAdvisoryTopic(Destination destination) throws JMSException {
        return isProducerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
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

    public static boolean isConsumerAdvisoryTopic(Destination destination) throws JMSException {
        return isConsumerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
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

    public static boolean isVirtualDestinationConsumerAdvisoryTopic(Destination destination) throws JMSException {
        return isVirtualDestinationConsumerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isVirtualDestinationConsumerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isVirtualDestinationConsumerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(VIRTUAL_DESTINATION_CONSUMER_ADVISORY_TOPIC_PREFIX);
        }
    }

    public static boolean isSlowConsumerAdvisoryTopic(Destination destination) throws JMSException {
        return isSlowConsumerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isSlowConsumerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isSlowConsumerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(SLOW_CONSUMER_TOPIC_PREFIX);
        }
    }

    public static boolean isFastProducerAdvisoryTopic(Destination destination) throws JMSException {
        return isFastProducerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isFastProducerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isFastProducerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(FAST_PRODUCER_TOPIC_PREFIX);
        }
    }

    public static boolean isMessageConsumedAdvisoryTopic(Destination destination) throws JMSException {
        return isMessageConsumedAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isMessageConsumedAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isMessageConsumedAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(MESSAGE_CONSUMED_TOPIC_PREFIX);
        }
    }

    public static boolean isMasterBrokerAdvisoryTopic(Destination destination) throws JMSException {
        return isMasterBrokerAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isMasterBrokerAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isMasterBrokerAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(MASTER_BROKER_TOPIC_PREFIX);
        }
    }

    public static boolean isMessageDeliveredAdvisoryTopic(Destination destination) throws JMSException {
        return isMessageDeliveredAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isMessageDeliveredAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isMessageDeliveredAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(MESSAGE_DELIVERED_TOPIC_PREFIX);
        }
    }

    public static boolean isMessageDiscardedAdvisoryTopic(Destination destination) throws JMSException {
        return isMessageDiscardedAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isMessageDiscardedAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isMessageDiscardedAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(MESSAGE_DISCAREDED_TOPIC_PREFIX);
        }
    }

    public static boolean isFullAdvisoryTopic(Destination destination) throws JMSException {
        return isFullAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isFullAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isFullAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(FULL_TOPIC_PREFIX);
        }
    }

    public static boolean isNetworkBridgeAdvisoryTopic(Destination destination) throws JMSException {
        return isNetworkBridgeAdvisoryTopic(ActiveMQMessageTransformation.transformDestination(destination));
    }

    public static boolean isNetworkBridgeAdvisoryTopic(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if (isNetworkBridgeAdvisoryTopic(compositeDestinations[i])) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(NETWORK_BRIDGE_TOPIC_PREFIX);
        }
    }

    /**
     * Returns the agent topic which is used to send commands to the broker
     */
    public static Destination getAgentDestination() {
        return AGENT_TOPIC_DESTINATION;
    }

    public static ActiveMQTopic getNetworkBridgeForwardFailureAdvisoryTopic() {
        return new ActiveMQTopic(NETWORK_BRIDGE_FORWARD_FAILURE_TOPIC_PREFIX);
    }
}
