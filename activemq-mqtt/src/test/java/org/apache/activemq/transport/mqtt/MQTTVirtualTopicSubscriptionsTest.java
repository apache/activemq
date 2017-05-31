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

import static org.junit.Assert.assertTrue;

import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Before;
import org.junit.Test;

/**
 * Run the basic tests with the NIO Transport.
 */
public class MQTTVirtualTopicSubscriptionsTest extends MQTTTest {

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
}
