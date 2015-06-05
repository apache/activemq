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
import java.util.List;

import org.apache.activemq.ActiveMQPrefetchPolicy;
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
import org.fusesource.mqtt.codec.CONNECT;

/**
 * Default implementation that uses unmapped topic subscriptions.
 */
public class MQTTDefaultSubscriptionStrategy extends AbstractMQTTSubscriptionStrategy {

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
        if (restoredDurableSubs.remove(destination.getPhysicalName())) {
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
                restoredDurableSubs.remove(MQTTProtocolSupport.convertMQTTToActiveMQ(subscription.getTopicName()));

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
}
