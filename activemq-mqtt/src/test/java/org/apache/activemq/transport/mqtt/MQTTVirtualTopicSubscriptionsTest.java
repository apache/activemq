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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.concurrent.TimeUnit;

/**
 * Run the basic tests with the NIO Transport.
 */
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
        javax.jms.Queue consumerQ = s.createQueue("Consumer.RegularSub.VirtualTopic.foo.bah");
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

}
