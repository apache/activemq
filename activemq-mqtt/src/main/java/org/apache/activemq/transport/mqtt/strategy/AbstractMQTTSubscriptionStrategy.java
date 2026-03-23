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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.broker.region.virtual.VirtualTopicInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTProtocolException;
import org.apache.activemq.transport.mqtt.MQTTProtocolSupport;
import org.apache.activemq.transport.mqtt.MQTTSubscription;
import org.apache.activemq.transport.mqtt.ResponseHandler;
import org.apache.activemq.util.LongSequenceGenerator;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of the {@link MQTTSubscriptionStrategy} interface providing
 * the base functionality that is common to most implementations.
 */
public abstract class AbstractMQTTSubscriptionStrategy implements MQTTSubscriptionStrategy, BrokerServiceAware {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMQTTSubscriptionStrategy.class);

    private static final byte SUBSCRIBE_ERROR = (byte) 0x80;

    protected MQTTProtocolConverter protocol;
    protected BrokerService brokerService;

    protected final ConcurrentMap<ConsumerId, MQTTSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, MQTTSubscription>();
    protected final ConcurrentMap<String, MQTTSubscription> mqttSubscriptionByTopic = new ConcurrentHashMap<String, MQTTSubscription>();
    protected final Set<String> restoredDurableSubs = Collections.synchronizedSet(new HashSet<String>());

    protected final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();

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
    public byte onSubscribe(final Topic topic) throws MQTTProtocolException {

        final String destinationName = topic.name().toString();
        final QoS requestedQoS = topic.qos();

        final MQTTSubscription mqttSubscription = mqttSubscriptionByTopic.get(destinationName);
        if (mqttSubscription != null) {
            if (requestedQoS != mqttSubscription.getQoS()) {
                // remove old subscription as the QoS has changed
                onUnSubscribe(destinationName);
            } else {
                try {
                    onReSubscribe(mqttSubscription);
                } catch (IOException e) {
                    throw new MQTTProtocolException("Failed to find subscription strategy", true, e);
                }
                return (byte) requestedQoS.ordinal();
            }
        }

        try {
            return onSubscribe(destinationName, requestedQoS);
        } catch (IOException e) {
            throw new MQTTProtocolException("Failed while intercepting subscribe", true, e);
        }
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
        String connectionInfoClientId = protocol.getClientId();
        // for zero-byte client ids we used connection id
        if (connectionInfoClientId == null || connectionInfoClientId.isEmpty()) {
            connectionInfoClientId = protocol.getConnectionId().toString();
        }
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

    @Override
    public MQTTSubscription getSubscription(ConsumerId consumerId) {
        return subscriptionsByConsumerId.get(consumerId);
    }

    protected ConsumerId getNextConsumerId() {
        return new ConsumerId(protocol.getSessionId(), consumerIdGenerator.getNextSequenceId());
    }

    protected byte doSubscribe(ConsumerInfo consumerInfo, final String topicName, final QoS qoS) throws MQTTProtocolException {

        MQTTSubscription mqttSubscription = new MQTTSubscription(protocol, topicName, qoS, consumerInfo);

        // optimistic add to local maps first to be able to handle commands in onActiveMQCommand
        subscriptionsByConsumerId.put(consumerInfo.getConsumerId(), mqttSubscription);
        mqttSubscriptionByTopic.put(topicName, mqttSubscription);

        final byte[] qos = {-1};
        protocol.sendToActiveMQ(consumerInfo, new ResponseHandler() {
            @Override
            public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                // validate subscription request
                if (response.isException()) {
                    final Throwable throwable = ((ExceptionResponse) response).getException();
                    LOG.warn("Error subscribing to {}", topicName, throwable);
                    // version 3.1 don't supports silent fail
                    // version 3.1.1 send "error" qos
                    if (protocol.version == MQTTProtocolConverter.V3_1_1) {
                        qos[0] = SUBSCRIBE_ERROR;
                    } else {
                        qos[0] = (byte) qoS.ordinal();
                    }
                } else {
                    qos[0] = (byte) qoS.ordinal();
                }
            }
        });

        if (qos[0] == SUBSCRIBE_ERROR) {
            // remove from local maps if subscribe failed
            subscriptionsByConsumerId.remove(consumerInfo.getConsumerId());
            mqttSubscriptionByTopic.remove(topicName);
        }

        return qos[0];
    }

    public void doUnSubscribe(MQTTSubscription subscription) {
        mqttSubscriptionByTopic.remove(subscription.getTopicName());
        ConsumerInfo info = subscription.getConsumerInfo();
        if (info != null) {
            subscriptionsByConsumerId.remove(info.getConsumerId());

            RemoveInfo removeInfo = info.createRemoveCommand();
            protocol.sendToActiveMQ(removeInfo, new ResponseHandler() {
                @Override
                public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                    // ignore failures..
                }
            });
        }
    }

    //----- Durable Subscription management methods --------------------------//

    protected void deleteDurableSubs(List<SubscriptionInfo> subs) {
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

    protected void restoreDurableSubs(List<SubscriptionInfo> subs) {
        try {
            for (SubscriptionInfo sub : subs) {
                String name = sub.getSubcriptionName();
                String[] split = name.split(":", 2);
                QoS qoS = QoS.valueOf(split[0]);
                onSubscribe(new Topic(split[1], qoS));
                // mark this durable subscription as restored by Broker
                restoredDurableSubs.add(MQTTProtocolSupport.convertMQTTToActiveMQ(split[1]));
            }
        } catch (IOException e) {
            LOG.warn("Could not restore the MQTT durable subs.", e);
        }
    }

    protected List<SubscriptionInfo> lookupSubscription(String clientId) throws MQTTProtocolException {
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
