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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkBridgeListener;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Run the basic tests with the NIO Transport.
 */
@Category(ParallelTest.class)
public class MQTTVirtualTopicSubscriptionsTest extends MQTTTest {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTVirtualTopicSubscriptionsTest.class);

    @Override
    @Before
    public void setUp() throws Exception {
        protocolConfig = "transport.subscriptionStrategy=mqtt-virtual-topic-subscriptions";
        super.setUp();
    }

    @Override
    @Test(timeout = 60 * 1000)
    public void testSendMQTTReceiveJMS() throws Exception {
        doTestSendMQTTReceiveJMS("VirtualTopic.foo.*");
    }

    @Override
    @Test(timeout = 2 * 60 * 1000)
    public void testSendJMSReceiveMQTT() throws Exception {
        doTestSendJMSReceiveMQTT("VirtualTopic.foo.far");
    }

    @Override
    @Test(timeout = 30 * 10000)
    public void testJmsMapping() throws Exception {
        doTestJmsMapping("VirtualTopic.test.foo");
    }

    @Test(timeout = 60 * 1000)
    public void testSubscribeOnVirtualTopicAsDurable() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("VirtualTopicSubscriber");
        mqtt.setKeepAlive((short) 2);
        mqtt.setCleanSession(false);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String topicName = "VirtualTopic/foo/bah";

        connection.subscribe(new Topic[] { new Topic(topicName, QoS.EXACTLY_ONCE)});

        assertTrue("Should create a durable subscription", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 1;
            }
        }));

        connection.unsubscribe(new String[] { topicName });

        assertTrue("Should remove a durable subscription", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getDurableTopicSubscribers().length == 0;
            }
        }));

        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testDurableVirtaulTopicSubIsRecovered() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("VirtualTopicSubscriber");
        mqtt.setKeepAlive((short) 2);
        mqtt.setCleanSession(false);

        final String topicName = "VirtualTopic/foo/bah";

        {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            connection.subscribe(new Topic[] { new Topic(topicName, QoS.EXACTLY_ONCE)});

            assertTrue("Should create a durable subscription", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return brokerService.getAdminView().getDurableTopicSubscribers().length == 1;
                }
            }));

            connection.disconnect();
        }

        assertTrue("Should be one inactive subscription", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            assertTrue("Should recover a durable subscription", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return brokerService.getAdminView().getDurableTopicSubscribers().length == 1;
                }
            }));

            connection.subscribe(new Topic[] { new Topic(topicName, QoS.EXACTLY_ONCE)});

            assertTrue("Should still be just one durable subscription", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return brokerService.getAdminView().getDurableTopicSubscribers().length == 1;
                }
            }));

            connection.disconnect();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testRetainMessageDurability() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("sub");

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String topicName = "foo/bah";

        connection.subscribe(new Topic[] { new Topic(topicName, QoS.EXACTLY_ONCE)});


        // jms client
        ActiveMQConnection activeMQConnection = (ActiveMQConnection) cf.createConnection();
        // MUST set to true to receive retained messages
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        jakarta.jms.Queue consumerQ = s.createQueue("Consumer.RegularSub.VirtualTopic.foo.bah");
        MessageConsumer consumer = s.createConsumer(consumerQ);


        // publisher
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        // send retained message
        final String RETAINED = "RETAINED_MESSAGE_TEXT";
        provider.publish(topicName, RETAINED.getBytes(), EXACTLY_ONCE, true);

        Message message = connection.receive(5, TimeUnit.SECONDS);
        assertNotNull("got message", message);

        String response = new String(message.getPayload());
        LOG.info("Got message:" + response);


        // jms - verify retained message is persistent
        ActiveMQMessage activeMQMessage = (ActiveMQMessage) consumer.receive(5000);
        assertNotNull("Should get retained message", activeMQMessage);
        ByteSequence bs = activeMQMessage.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        LOG.info("Got message with deliverMode:" + activeMQMessage.getJMSDeliveryMode());
        assertEquals(DeliveryMode.PERSISTENT, activeMQMessage.getJMSDeliveryMode());

        activeMQConnection.close();
        connection.unsubscribe(new String[] { topicName });

        connection.disconnect();
    }

    @Test(timeout = 6000 * 1000)
    public void testCleanSessionWithNetworkOfBrokersRemoveAddRace() throws Exception {

        stopBroker();
        advisorySupport = true;
        startBroker();

        BrokerService brokerTwo = createBroker(false);
        brokerTwo.setBrokerName("BrokerTwo");
        final NetworkConnector networkConnector = brokerTwo.addNetworkConnector("static:" + jmsUri);
        networkConnector.setDestinationFilter("ActiveMQ.Advisory.Consumer.Queue.>,ActiveMQ.Advisory.Queue");

        // let remove Ops backup on the executor
        final CountDownLatch removeOp = new CountDownLatch(1);

        brokerTwo.setPlugins(new BrokerPlugin[] {new BrokerPluginSupport() {
            @Override
            public void removeDestinationInfo(ConnectionContext context, DestinationInfo destInfo) throws Exception {
                // delay remove ops till subscription is renewed such that the single thread executor backs up
                removeOp.await(50, TimeUnit.SECONDS);
                super.removeDestinationInfo(context, destInfo);
            }
        }});
        brokerTwo.start();


        assertTrue("Bridge created", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !networkConnector.activeBridges().isEmpty();
            }
        }));

        // track an error on the network bridge
        final AtomicBoolean failed = new AtomicBoolean();
        NetworkBridgeListener listener = new NetworkBridgeListener() {
            @Override
            public void bridgeFailed() {
                failed.set(true);
            }

            @Override
            public void onStart(NetworkBridge bridge) {

            }

            @Override
            public void onStop(NetworkBridge bridge) {

            }

            @Override
            public void onOutboundMessage(NetworkBridge bridge, org.apache.activemq.command.Message message) {

            }

            @Override
            public void onInboundMessage(NetworkBridge bridge, org.apache.activemq.command.Message message) {

            }
        };
        for (NetworkBridge bridge : networkConnector.activeBridges()) {
            bridge.setNetworkBridgeListener(listener);
        }

        final int numDests = 100;

        // subscribe with durability
        final String CLIENTID = "clean-session";
        final MQTT mqttNotClean = createMQTTConnection(CLIENTID, false);
        BlockingConnection notClean = mqttNotClean.blockingConnection();
        notClean.connect();

        for (int i=0; i<numDests; i++) {
            final String TOPIC = "TopicA-" + i;
            notClean.subscribe(new Topic[]{new Topic(TOPIC, QoS.EXACTLY_ONCE)});
        }
        notClean.disconnect();


        // whack any old state with reconnect clean
        final MQTT mqttClean = createMQTTConnection(CLIENTID, true);
        final BlockingConnection clean = mqttClean.blockingConnection();
        clean.connect();
        for (int i=0; i<numDests; i++) {
            final String TOPIC = "TopicA-" + i;
            clean.subscribe(new Topic[]{new Topic(TOPIC, QoS.EXACTLY_ONCE)});
        }
        clean.disconnect();

        // subscribe again with durability
        notClean = mqttNotClean.blockingConnection();
        notClean.connect();
        for (int i=0; i<numDests; i++) {
            final String TOPIC = "TopicA-" + i;
            notClean.subscribe(new Topic[]{new Topic(TOPIC, QoS.EXACTLY_ONCE)});
        }

        // release bridge remove ops *after* new/re subscription
        removeOp.countDown();

        assertTrue("All destinations and subs recreated and consumers connected on brokerTwo via network", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                BrokerView brokerView = brokerTwo.getAdminView();
                int numQueues = brokerView.getQueues().length;
                int numSubscriptions = brokerView.getQueueSubscribers().length;

                LOG.info("#Queues: " + numQueues + ", #Subs: " + numSubscriptions);
                return numQueues == numDests && numSubscriptions == numDests;
            }
        }));
        Message msg = notClean.receive(500, TimeUnit.MILLISECONDS);
        assertNull(msg);
        notClean.disconnect();

        assertFalse("bridge did not fail", failed.get());
        brokerTwo.stop();
    }
}
