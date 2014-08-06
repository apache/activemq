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
package org.apache.activemq.transport.mqtt.strategy;

import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.broker.region.virtual.VirtualTopicInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTProtocolException;
import org.apache.activemq.transport.mqtt.MQTTSubscription;

/**
 * Abstract implementation of the {@link MQTTSubscriptionStrategy} interface providing
 * the base functionality that is common to most implementations.
 */
public abstract class AbstractMQTTSubscriptionStrategy implements MQTTSubscriptionStrategy, BrokerServiceAware {

    protected MQTTProtocolConverter protocol;
    protected BrokerService brokerService;

    @Override
    public void initialize(MQTTProtocolConverter protocol) throws MQTTProtocolException {
        setProtocolConverter(protocol);
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public void setProtocolConverter(MQTTProtocolConverter parent) {
        this.protocol = parent;
    }

    @Override
    public MQTTProtocolConverter getProtocolConverter() {
        return protocol;
    }

    @Override
    public void onReSubscribe(MQTTSubscription mqttSubscription) throws MQTTProtocolException {
        String topicName = mqttSubscription.getTopicName();

        // get TopicRegion
        RegionBroker regionBroker;
        try {
            regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        } catch (Exception e) {
            throw new MQTTProtocolException("Error subscribing to " + topicName + ": " + e.getMessage(), false, e);
        }
        final TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();

        final ConsumerInfo consumerInfo = mqttSubscription.getConsumerInfo();
        final ConsumerId consumerId = consumerInfo.getConsumerId();

        // use actual client id used to create connection to lookup connection
        // context
        final String connectionInfoClientId = protocol.getClientId();
        final ConnectionContext connectionContext = regionBroker.getConnectionContext(connectionInfoClientId);

        // get all matching Topics
        final Set<org.apache.activemq.broker.region.Destination> matchingDestinations =
            topicRegion.getDestinations(mqttSubscription.getDestination());
        for (org.apache.activemq.broker.region.Destination dest : matchingDestinations) {

            // recover retroactive messages for matching subscription
            for (Subscription subscription : dest.getConsumers()) {
                if (subscription.getConsumerInfo().getConsumerId().equals(consumerId)) {
                    try {
                        if (dest instanceof org.apache.activemq.broker.region.Topic) {
                            ((org.apache.activemq.broker.region.Topic) dest).recoverRetroactiveMessages(connectionContext, subscription);
                        } else if (dest instanceof VirtualTopicInterceptor) {
                            ((VirtualTopicInterceptor) dest).getTopic().recoverRetroactiveMessages(connectionContext, subscription);
                        }
                        if (subscription instanceof PrefetchSubscription) {
                            // request dispatch for prefetch subs
                            PrefetchSubscription prefetchSubscription = (PrefetchSubscription) subscription;
                            prefetchSubscription.dispatchPending();
                        }
                    } catch (Exception e) {
                        throw new MQTTProtocolException("Error recovering retained messages for " + dest.getName() + ": " + e.getMessage(), false, e);
                    }
                    break;
                }
            }
        }
    }

    @Override
    public ActiveMQDestination onSend(String topicName) {
        return new ActiveMQTopic(topicName);
    }

    @Override
    public String onSend(ActiveMQDestination destination) {
        return destination.getPhysicalName();
    }

    @Override
    public boolean isControlTopic(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith("$");
    }
}
