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
package org.apache.activemq.transport.mqtt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.fusesource.mqtt.codec.PUBLISH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages PUBLISH packet ids for clients.
 *
 * @author Dhiraj Bokde
 */
public class MQTTPacketIdGenerator extends ServiceSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTPacketIdGenerator.class);
    private static final Object LOCK = new Object();

    Map<String, PacketIdMaps> clientIdMap = new ConcurrentHashMap<String, PacketIdMaps>();

    private final NonZeroSequenceGenerator messageIdGenerator = new NonZeroSequenceGenerator();

    private MQTTPacketIdGenerator() {
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        synchronized (this) {
            clientIdMap = new ConcurrentHashMap<String, PacketIdMaps>();
        }
    }

    @Override
    protected void doStart() throws Exception {
    }

    public void startClientSession(String clientId) {
        if (!clientIdMap.containsKey(clientId)) {
            clientIdMap.put(clientId, new PacketIdMaps());
        }
    }

    public boolean stopClientSession(String clientId) {
        return clientIdMap.remove(clientId) != null;
    }

    public short setPacketId(String clientId, MQTTSubscription subscription, ActiveMQMessage message, PUBLISH publish) {
        final PacketIdMaps idMaps = clientIdMap.get(clientId);
        if (idMaps == null) {
            // maybe its a cleansession=true client id, use session less message id
            final short id = messageIdGenerator.getNextSequenceId();
            publish.messageId(id);
            return id;
        } else {
            return idMaps.setPacketId(subscription, message, publish);
        }
    }

    public void ackPacketId(String clientId, short packetId) {
        final PacketIdMaps idMaps = clientIdMap.get(clientId);
        if (idMaps != null) {
            idMaps.ackPacketId(packetId);
        }
    }

    public short getNextSequenceId(String clientId) {
        final PacketIdMaps idMaps = clientIdMap.get(clientId);
        return idMaps != null ? idMaps.getNextSequenceId(): messageIdGenerator.getNextSequenceId();
    }

    public static MQTTPacketIdGenerator getMQTTPacketIdGenerator(BrokerService broker) {
        MQTTPacketIdGenerator result = null;
        if (broker != null) {
            synchronized (LOCK) {
                Service[] services = broker.getServices();
                if (services != null) {
                    for (Service service : services) {
                        if (service instanceof MQTTPacketIdGenerator) {
                            return (MQTTPacketIdGenerator) service;
                        }
                    }
                }
                result = new MQTTPacketIdGenerator();
                broker.addService(result);
                if (broker.isStarted()) {
                    try {
                        result.start();
                    } catch (Exception e) {
                        LOG.warn("Couldn't start MQTTPacketIdGenerator");
                    }
                }
            }
        }


        return result;
    }

    private class PacketIdMaps {

        private final NonZeroSequenceGenerator messageIdGenerator = new NonZeroSequenceGenerator();
        final Map<String, Short> activemqToPacketIds = new LRUCache<String, Short>(MQTTProtocolConverter.DEFAULT_CACHE_SIZE);
        final Map<Short, String> packetIdsToActivemq = new LRUCache<Short, String>(MQTTProtocolConverter.DEFAULT_CACHE_SIZE);

        short setPacketId(MQTTSubscription subscription, ActiveMQMessage message, PUBLISH publish) {
            // subscription key
            final StringBuilder subscriptionKey = new StringBuilder();
            subscriptionKey.append(subscription.getConsumerInfo().getDestination().getPhysicalName())
                .append(':').append(message.getJMSMessageID());
            final String keyStr = subscriptionKey.toString();
            Short packetId;
            synchronized (activemqToPacketIds) {
                packetId = activemqToPacketIds.get(keyStr);
                if (packetId == null) {
                    packetId = getNextSequenceId();
                    activemqToPacketIds.put(keyStr, packetId);
                    packetIdsToActivemq.put(packetId, keyStr);
                } else {
                    // mark publish as duplicate!
                    publish.dup(true);
                }
            }
            publish.messageId(packetId);
            return packetId;
        }

        void ackPacketId(short packetId) {
            synchronized (activemqToPacketIds) {
                final String subscriptionKey = packetIdsToActivemq.remove(packetId);
                if (subscriptionKey != null) {
                    activemqToPacketIds.remove(subscriptionKey);
                }
            }
        }

        short getNextSequenceId() {
            return messageIdGenerator.getNextSequenceId();
        }

    }

    private class NonZeroSequenceGenerator {

        private short lastSequenceId;

        public synchronized short getNextSequenceId() {
            final short val = ++lastSequenceId;
            return val != 0 ? val : ++lastSequenceId;
        }

    }

}
