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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTProtocolException;
import org.apache.activemq.transport.mqtt.MQTTProtocolSupport;
import org.apache.activemq.transport.mqtt.MQTTSubscription;
import org.apache.activemq.transport.mqtt.ResponseHandler;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNECT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation that uses unmapped topic subscriptions.
 */
public class MQTTDefaultSubscriptionStrategy extends AbstractMQTTSubscriptionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTDefaultSubscriptionStrategy.class);

    private final Set<String> restoredSubs = Collections.synchronizedSet(new HashSet<String>());

    @Override
    public void onConnect(CONNECT connect) throws MQTTProtocolException {
        List<SubscriptionInfo> subs = lookupSubscription(protocol.getClientId());

        if (connect.cleanSession()) {
            deleteDurableSubs(subs);
        } else {
            restoreDurableSubs(subs);
        }
    }

    @Override
    public byte onSubscribe(String topicName, QoS requestedQoS) throws MQTTProtocolException {
        ActiveMQDestination destination = new ActiveMQTopic(MQTTProtocolSupport.convertMQTTToActiveMQ(topicName));

        ConsumerInfo consumerInfo = new ConsumerInfo(getNextConsumerId());
        consumerInfo.setDestination(destination);
        consumerInfo.setPrefetchSize(ActiveMQPrefetchPolicy.DEFAULT_TOPIC_PREFETCH);
        consumerInfo.setRetroactive(true);
        consumerInfo.setDispatchAsync(true);
        // create durable subscriptions only when clean session is false
        if (!protocol.isCleanSession() && protocol.getClientId() != null && requestedQoS.ordinal() >= QoS.AT_LEAST_ONCE.ordinal()) {
            consumerInfo.setSubscriptionName(requestedQoS + ":" + topicName);
            consumerInfo.setPrefetchSize(ActiveMQPrefetchPolicy.DEFAULT_DURABLE_TOPIC_PREFETCH);
        }

        if (protocol.getActiveMQSubscriptionPrefetch() > 0) {
            consumerInfo.setPrefetchSize(protocol.getActiveMQSubscriptionPrefetch());
        }

        return doSubscribe(consumerInfo, topicName, requestedQoS);
    }

    @Override
    public void onReSubscribe(MQTTSubscription mqttSubscription) throws MQTTProtocolException {

        ActiveMQDestination destination = mqttSubscription.getDestination();

        // check whether the Topic has been recovered in restoreDurableSubs
        // mark subscription available for recovery for duplicate subscription
        if (restoredSubs.remove(destination.getPhysicalName())) {
            return;
        }

        super.onReSubscribe(mqttSubscription);
    }

    @Override
    public void onUnSubscribe(String topicName) throws MQTTProtocolException {
        MQTTSubscription subscription = mqttSubscriptionByTopic.remove(topicName);
        if (subscription != null) {
            doUnSubscribe(subscription);

            // check if the durable sub also needs to be removed
            if (subscription.getConsumerInfo().getSubscriptionName() != null) {
                // also remove it from restored durable subscriptions set
                restoredSubs.remove(MQTTProtocolSupport.convertMQTTToActiveMQ(subscription.getTopicName()));

                RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(protocol.getConnectionId());
                rsi.setSubscriptionName(subscription.getConsumerInfo().getSubscriptionName());
                rsi.setClientId(protocol.getClientId());
                protocol.sendToActiveMQ(rsi, new ResponseHandler() {
                    @Override
                    public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                        // ignore failures..
                    }
                });
            }
        }
    }

    private void deleteDurableSubs(List<SubscriptionInfo> subs) {
        try {
            for (SubscriptionInfo sub : subs) {
                RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(protocol.getConnectionId());
                rsi.setSubscriptionName(sub.getSubcriptionName());
                rsi.setClientId(sub.getClientId());
                protocol.sendToActiveMQ(rsi, new ResponseHandler() {
                    @Override
                    public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                        // ignore failures..
                    }
                });
            }
        } catch (Throwable e) {
            LOG.warn("Could not delete the MQTT durable subs.", e);
        }
    }

    private void restoreDurableSubs(List<SubscriptionInfo> subs) {
        try {
            for (SubscriptionInfo sub : subs) {
                String name = sub.getSubcriptionName();
                String[] split = name.split(":", 2);
                QoS qoS = QoS.valueOf(split[0]);
                onSubscribe(new Topic(split[1], qoS));
                // mark this durable subscription as restored by Broker
                restoredSubs.add(split[1]);
            }
        } catch (IOException e) {
            LOG.warn("Could not restore the MQTT durable subs.", e);
        }
    }

    List<SubscriptionInfo> lookupSubscription(String clientId) throws MQTTProtocolException {
        List<SubscriptionInfo> result = new ArrayList<SubscriptionInfo>();
        RegionBroker regionBroker;

        try {
            regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        } catch (Exception e) {
            throw new MQTTProtocolException("Error recovering durable subscriptions: " + e.getMessage(), false, e);
        }

        final TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        List<DurableTopicSubscription> subscriptions = topicRegion.lookupSubscriptions(clientId);
        if (subscriptions != null) {
            for (DurableTopicSubscription subscription : subscriptions) {
                LOG.debug("Recovered durable sub:{} on connect", subscription);

                SubscriptionInfo info = new SubscriptionInfo();

                info.setDestination(subscription.getActiveMQDestination());
                info.setSubcriptionName(subscription.getSubscriptionKey().getSubscriptionName());
                info.setClientId(clientId);

                result.add(info);
            }
        }

        return result;
    }
}
