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

import java.net.ProtocolException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RetainedMessageSubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.SimpleAuthorizationMap;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PUBLISH;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTTest extends AbstractMQTTTest {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTTest.class);

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveMQTT() throws Exception {
        addMQTTConnector();
        brokerService.start();
        final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
        initializeConnection(subscriptionProvider);

        subscriptionProvider.subscribe("foo/bah", AT_MOST_ONCE);

        final CountDownLatch latch = new CountDownLatch(numberOfMessages);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfMessages; i++) {
                    try {
                        byte[] payload = subscriptionProvider.receive(10000);
                        assertNotNull("Should get a message", payload);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }

                }
            }
        });
        thread.start();

        final MQTTClientProvider publishProvider = getMQTTClientProvider();
        initializeConnection(publishProvider);

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Message " + i;
            publishProvider.publish("foo/bah", payload.getBytes(), AT_LEAST_ONCE);
        }

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        subscriptionProvider.disconnect();
        publishProvider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testUnsubscribeMQTT() throws Exception {
        addMQTTConnector();
        brokerService.start();
        final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
        initializeConnection(subscriptionProvider);

        String topic = "foo/bah";

        subscriptionProvider.subscribe(topic, AT_MOST_ONCE);

        final CountDownLatch latch = new CountDownLatch(numberOfMessages / 2);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfMessages; i++) {
                    try {
                        byte[] payload = subscriptionProvider.receive(10000);
                        assertNotNull("Should get a message", payload);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }

                }
            }
        });
        thread.start();

        final MQTTClientProvider publishProvider = getMQTTClientProvider();
        initializeConnection(publishProvider);

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Message " + i;
            if (i == numberOfMessages / 2) {
                subscriptionProvider.unsubscribe(topic);
            }
            publishProvider.publish(topic, payload.getBytes(), AT_LEAST_ONCE);
        }

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        subscriptionProvider.disconnect();
        publishProvider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAtMostOnceReceiveExactlyOnce() throws Exception {
        /**
         * Although subscribing with EXACTLY ONCE, the message gets published
         * with AT_MOST_ONCE - in MQTT the QoS is always determined by the
         * message as published - not the wish of the subscriber
         */
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAtLeastOnceReceiveExactlyOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAtLeastOnceReceiveAtMostOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_MOST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveAtMostOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_MOST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveAtLeastOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_LEAST_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveExactlyOnce() throws Exception {
        addMQTTConnector();
        brokerService.start();
        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);

        subscriber.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            publisher.publish("foo", payload.getBytes(), EXACTLY_ONCE);
            byte[] message = subscriber.receive(5000);
            assertNotNull("Should get a message + [" + i + "]", message);
            assertEquals(payload, new String(message));
        }
        subscriber.disconnect();
        publisher.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveLargeMessages() throws Exception {
        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }
        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);

        subscriber.subscribe("foo", AT_LEAST_ONCE);
        for (int i = 0; i < 10; i++) {
            publisher.publish("foo", payload, AT_LEAST_ONCE);
            byte[] message = subscriber.receive(5000);
            assertNotNull("Should get a message", message);

            assertArrayEquals(payload, message);
        }
        subscriber.disconnect();
        publisher.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveRetainedMessages() throws Exception {

        addMQTTConnector();
        brokerService.start();

        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);

        String RETAINED = "retained";
        publisher.publish("foo", RETAINED.getBytes(), AT_LEAST_ONCE, true);

        List<String> messages = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            messages.add("TEST MESSAGE:" + i);
        }

        subscriber.subscribe("foo", AT_LEAST_ONCE);

        for (int i = 0; i < 10; i++) {
            publisher.publish("foo", messages.get(i).getBytes(), AT_LEAST_ONCE);
        }
        byte[] msg = subscriber.receive(5000);
        assertNotNull(msg);
        assertEquals(RETAINED, new String(msg));

        for (int i = 0; i < 10; i++) {
            msg = subscriber.receive(5000);
            assertNotNull(msg);
            assertEquals(messages.get(i), new String(msg));
        }
        subscriber.disconnect();
        publisher.disconnect();
    }

    @Test(timeout = 30 * 1000)
    public void testValidZeroLengthClientId() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("");
        mqtt.setCleanSession(true);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testMQTTPathPatterns() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("");
        mqtt.setCleanSession(true);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String RETAINED = "RETAINED";
        String[] topics = { "TopicA", "/TopicA", "/", "TopicA/", "//" };
        for (String topic : topics) {
            // test retained message
            connection.publish(topic, (RETAINED + topic).getBytes(), QoS.AT_LEAST_ONCE, true);

            connection.subscribe(new Topic[] { new Topic(topic, QoS.AT_LEAST_ONCE) });
            Message msg = connection.receive(5, TimeUnit.SECONDS);
            assertNotNull("No message for " + topic, msg);
            assertEquals(RETAINED + topic, new String(msg.getPayload()));
            msg.ack();

            // test non-retained message
            connection.publish(topic, topic.getBytes(), QoS.AT_LEAST_ONCE, false);
            msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertEquals(topic, new String(msg.getPayload()));
            msg.ack();

            connection.unsubscribe(new String[] { topic });
        }
        connection.disconnect();

        // test wildcard patterns with above topics
        String[] wildcards = { "#", "+", "+/#", "/+", "+/", "+/+", "+/+/", "+/+/+" };
        for (String wildcard : wildcards) {
            final Pattern pattern = Pattern.compile(wildcard.replaceAll("/?#", "(/?.*)*").replaceAll("\\+", "[^/]*"));

            connection = mqtt.blockingConnection();
            connection.connect();
            final byte[] qos = connection.subscribe(new Topic[]{new Topic(wildcard, QoS.AT_LEAST_ONCE)});
            assertNotEquals("Subscribe failed " + wildcard, (byte)0x80, qos[0]);

            // test retained messages
            Message msg = connection.receive(5, TimeUnit.SECONDS);
            do {
                assertNotNull("RETAINED null " + wildcard, msg);
                assertTrue("RETAINED prefix " + wildcard, new String(msg.getPayload()).startsWith(RETAINED));
                assertTrue("RETAINED matching " + wildcard + " " + msg.getTopic(), pattern.matcher(msg.getTopic()).matches());
                msg.ack();
                msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            } while (msg != null);

            // test non-retained message
            for (String topic : topics) {
                connection.publish(topic, topic.getBytes(), QoS.AT_LEAST_ONCE, false);
            }
            msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            do {
                assertNotNull("Non-retained Null " + wildcard, msg);
                assertTrue("Non-retained matching " + wildcard + " " + msg.getTopic(), pattern.matcher(msg.getTopic()).matches());
                msg.ack();
                msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            } while (msg != null);

            connection.unsubscribe(new String[] { wildcard });
            connection.disconnect();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testMQTTRetainQoS() throws Exception {
        addMQTTConnector();
        brokerService.start();

        String[] topics = { "AT_MOST_ONCE", "AT_LEAST_ONCE", "EXACTLY_ONCE" };
        for (int i = 0; i < topics.length; i++) {
            final String topic = topics[i];

            MQTT mqtt = createMQTTConnection();
            mqtt.setClientId("foo");
            mqtt.setKeepAlive((short) 2);

            final int[] actualQoS = { -1 };
            mqtt.setTracer(new Tracer() {
                @Override
                public void onReceive(MQTTFrame frame) {
                    // validate the QoS
                    if (frame.messageType() == PUBLISH.TYPE) {
                        actualQoS[0] = frame.qos().ordinal();
                    }
                }
            });

            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.publish(topic, topic.getBytes(), QoS.EXACTLY_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(topic, QoS.valueOf(topic))});

            final Message msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertEquals(topic, new String(msg.getPayload()));
            int waitCount = 0;
            while (actualQoS[0] == -1 && waitCount < 10) {
                Thread.sleep(1000);
                waitCount++;
            }
            assertEquals(i, actualQoS[0]);
            msg.ack();

            connection.unsubscribe(new String[]{topic});
            connection.disconnect();
        }

    }

    @Test(timeout = 60 * 1000)
    public void testDuplicateSubscriptions() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 2);

        final int[] actualQoS = { -1 };
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                // validate the QoS
                if (frame.messageType() == PUBLISH.TYPE) {
                    actualQoS[0] = frame.qos().ordinal();
                }
            }
        });

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String RETAIN = "RETAIN";
        connection.publish("TopicA", RETAIN.getBytes(), QoS.EXACTLY_ONCE, true);

        QoS[] qoss = { QoS.AT_MOST_ONCE, QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE };
        for (QoS qos : qoss) {
            connection.subscribe(new Topic[]{new Topic("TopicA", qos)});

            final Message msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No message for " + qos, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            int waitCount = 0;
            while (actualQoS[0] == -1 && waitCount < 10) {
                Thread.sleep(1000);
                waitCount++;
            }
            assertEquals(qos.ordinal(), actualQoS[0]);
            actualQoS[0] = -1;
        }

        connection.unsubscribe(new String[] { "TopicA" });
        connection.disconnect();

    }

    @Test(timeout = 120 * 1000)
    public void testRetainedMessage() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short) 2);

        final String RETAIN = "RETAIN";
        final String TOPICA = "TopicA";

        final String[] clientIds = { null, "foo", "durable" };
        for (String clientId : clientIds) {

            mqtt.setClientId(clientId);
            mqtt.setCleanSession(!"durable".equals(clientId));

            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            // set retained message and check
            connection.publish(TOPICA, RETAIN.getBytes(), QoS.EXACTLY_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            Message msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(5000, TimeUnit.MILLISECONDS));

            // test duplicate subscription
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(15000, TimeUnit.MILLISECONDS);
            assertNotNull("No retained message on duplicate subscription for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(5000, TimeUnit.MILLISECONDS));
            connection.unsubscribe(new String[]{"TopicA"});

            // clear retained message and check that we don't receive it
            connection.publish(TOPICA, "".getBytes(), QoS.AT_MOST_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNull("Retained message not cleared for " + clientId, msg);
            connection.unsubscribe(new String[]{"TopicA"});

            // set retained message again and check
            connection.publish(TOPICA, RETAIN.getBytes(), QoS.EXACTLY_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No reset retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(5000, TimeUnit.MILLISECONDS));

            // re-connect and check
            connection.disconnect();
            connection = mqtt.blockingConnection();
            connection.connect();
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No reset retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(5000, TimeUnit.MILLISECONDS));

            connection.unsubscribe(new String[]{"TopicA"});

            connection.disconnect();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testFailedSubscription() throws Exception {
        addMQTTConnector();

        final SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setAnonymousAccessAllowed(true);

        final String ANONYMOUS = "anonymous";
        authenticationPlugin.setAnonymousGroup(ANONYMOUS);
        final DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        // only one authorized destination, anonymous for anonymous group!
        map.put(new ActiveMQTopic(ANONYMOUS), new GroupPrincipal(ANONYMOUS));
        final AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(new SimpleAuthorizationMap(map, map, map));

        brokerService.setPlugins(new BrokerPlugin[] { authorizationPlugin, authenticationPlugin });
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 2);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String NAMED = "named";
        byte[] qos = connection.subscribe(new Topic[] { new Topic(NAMED, QoS.AT_MOST_ONCE), new Topic(ANONYMOUS, QoS.EXACTLY_ONCE) });
        assertEquals((byte) 0x80, qos[0]);
        assertEquals((byte) QoS.EXACTLY_ONCE.ordinal(), qos[1]);

        // validate the subscription by sending a retained message
        connection.publish(ANONYMOUS, ANONYMOUS.getBytes(), QoS.AT_MOST_ONCE, true);
        Message msg = connection.receive(1000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(ANONYMOUS, new String(msg.getPayload()));
        msg.ack();

        connection.unsubscribe(new String[] { ANONYMOUS });
        qos = connection.subscribe(new Topic[] { new Topic(ANONYMOUS, QoS.AT_LEAST_ONCE) });
        assertEquals((byte) QoS.AT_LEAST_ONCE.ordinal(), qos[0]);

        msg = connection.receive(1000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(ANONYMOUS, new String(msg.getPayload()));
        msg.ack();

        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testUniqueMessageIds() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 2);
        mqtt.setCleanSession(true);

        final List<PUBLISH> publishList = new ArrayList<PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client received:\n" + frame);
                if (frame.messageType() == PUBLISH.TYPE) {
                    PUBLISH publish = new PUBLISH();
                    try {
                        publish.decode(frame);
                    } catch (ProtocolException e) {
                        fail("Error decoding publish " + e.getMessage());
                    }
                    publishList.add(publish);
                }
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client sent:\n" + frame);
            }
        });

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        // create overlapping subscriptions with different QoSs
        QoS[] qoss = { QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE };
        final String TOPIC = "TopicA/";

        // publish retained message
        connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, true);

        String[] subs = { TOPIC, "TopicA/#", "TopicA/+" };
        for (int i = 0; i < qoss.length; i++) {
            connection.subscribe(new Topic[] { new Topic(subs[i], qoss[i]) });
        }

        // publish non-retained message
        connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        int received = 0;

        Message msg = connection.receive(5000, TimeUnit.MILLISECONDS);
        do {
            assertNotNull(msg);
            assertEquals(TOPIC, new String(msg.getPayload()));
            msg.ack();
            int waitCount = 0;
            while (publishList.size() <= received && waitCount < 10) {
                Thread.sleep(1000);
                waitCount++;
            }
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
        } while (msg != null && received++ < subs.length * 2);
        assertEquals("Unexpected number of messages", subs.length * 2, received + 1);

        // make sure we received distinct ids for QoS != AT_MOST_ONCE, and 0 for
        // AT_MOST_ONCE
        for (int i = 0; i < publishList.size(); i++) {
            for (int j = i + 1; j < publishList.size(); j++) {
                final PUBLISH publish1 = publishList.get(i);
                final PUBLISH publish2 = publishList.get(j);
                boolean qos0 = false;
                if (publish1.qos() == QoS.AT_MOST_ONCE) {
                    qos0 = true;
                    assertEquals(0, publish1.messageId());
                }
                if (publish2.qos() == QoS.AT_MOST_ONCE) {
                    qos0 = true;
                    assertEquals(0, publish2.messageId());
                }
                if (!qos0) {
                    assertNotEquals(publish1.messageId(), publish2.messageId());
                }
            }
        }

        connection.unsubscribe(subs);
        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testResendMessageId() throws Exception {
        addMQTTConnector("trace=true");
        brokerService.start();

        final MQTT mqtt = createMQTTConnection("resend", false);
        mqtt.setKeepAlive((short) 5);

        final List<PUBLISH> publishList = new ArrayList<PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client received:\n" + frame);
                if (frame.messageType() == PUBLISH.TYPE) {
                    PUBLISH publish = new PUBLISH();
                    try {
                        publish.decode(frame);
                    } catch (ProtocolException e) {
                        fail("Error decoding publish " + e.getMessage());
                    }
                    publishList.add(publish);
                }
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client sent:\n" + frame);
            }
        });

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        final String TOPIC = "TopicA/";
        final String[] topics = new String[] { TOPIC, "TopicA/+" };
        connection.subscribe(new Topic[] { new Topic(topics[0], QoS.AT_LEAST_ONCE), new Topic(topics[1], QoS.EXACTLY_ONCE) });

        // publish non-retained message
        connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return publishList.size() == 2;
            }
        }, 5000);
        assertEquals(2, publishList.size());

        connection.disconnect();

        connection = mqtt.blockingConnection();
        connection.connect();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return publishList.size() == 4;
            }
        }, 5000);
        assertEquals(4, publishList.size());

        // make sure we received duplicate message ids
        assertTrue(publishList.get(0).messageId() == publishList.get(2).messageId() || publishList.get(0).messageId() == publishList.get(3).messageId());
        assertTrue(publishList.get(1).messageId() == publishList.get(3).messageId() || publishList.get(1).messageId() == publishList.get(2).messageId());
        assertTrue(publishList.get(2).dup() && publishList.get(3).dup());

        connection.unsubscribe(topics);
        connection.disconnect();
    }

    @Test(timeout = 90 * 1000)
    public void testPacketIdGeneratorNonCleanSession() throws Exception {
        addMQTTConnector("trace=true");
        brokerService.start();

        final MQTT mqtt = createMQTTConnection("nonclean-packetid", false);
        mqtt.setKeepAlive((short) 15);

        final Map<Short, PUBLISH> publishMap = new ConcurrentHashMap<Short, PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client received:\n" + frame);
                if (frame.messageType() == PUBLISH.TYPE) {
                    PUBLISH publish = new PUBLISH();
                    try {
                        publish.decode(frame);
                        LOG.info("PUBLISH " + publish);
                    } catch (ProtocolException e) {
                        fail("Error decoding publish " + e.getMessage());
                    }
                    if (publishMap.get(publish.messageId()) != null) {
                        assertTrue(publish.dup());
                    }
                    publishMap.put(publish.messageId(), publish);
                }
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client sent:\n" + frame);
            }
        });

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        final String TOPIC = "TopicA/";
        connection.subscribe(new Topic[] { new Topic(TOPIC, QoS.EXACTLY_ONCE) });

        // publish non-retained messages
        final int TOTAL_MESSAGES = 10;
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        }

        // receive half the messages in this session
        for (int i = 0; i < TOTAL_MESSAGES / 2; i++) {
            final Message msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertEquals(TOPIC, new String(msg.getPayload()));
            msg.ack();
        }

        connection.disconnect();
        // resume session
        connection = mqtt.blockingConnection();
        connection.connect();
        // receive rest of the messages
        Message msg = null;
        do {
            msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            if (msg != null) {
                assertEquals(TOPIC, new String(msg.getPayload()));
                msg.ack();
            }
        } while (msg != null);

        // make sure we received all message ids
        for (short id = 1; id <= TOTAL_MESSAGES; id++) {
            assertNotNull("No message for id " + id, publishMap.get(id));
        }

        connection.unsubscribe(new String[] { TOPIC });
        connection.disconnect();
    }

    @Test(timeout = 90 * 1000)
    public void testPacketIdGeneratorCleanSession() throws Exception {
        addMQTTConnector("trace=true");
        brokerService.start();

        final String[] cleanClientIds = new String[] { "", "clean-packetid", null };
        final Map<Short, PUBLISH> publishMap = new ConcurrentHashMap<Short, PUBLISH>();
        MQTT[] mqtts = new MQTT[cleanClientIds.length];
        for (int i = 0; i < cleanClientIds.length; i++) {
            mqtts[i] = createMQTTConnection("", true);
            mqtts[i].setKeepAlive((short) 15);

            mqtts[i].setTracer(new Tracer() {
                @Override
                public void onReceive(MQTTFrame frame) {
                    LOG.info("Client received:\n" + frame);
                    if (frame.messageType() == PUBLISH.TYPE) {
                        PUBLISH publish = new PUBLISH();
                        try {
                            publish.decode(frame);
                            LOG.info("PUBLISH " + publish);
                        } catch (ProtocolException e) {
                            fail("Error decoding publish " + e.getMessage());
                        }
                        if (publishMap.get(publish.messageId()) != null) {
                            assertTrue(publish.dup());
                        }
                        publishMap.put(publish.messageId(), publish);
                    }
                }

                @Override
                public void onSend(MQTTFrame frame) {
                    LOG.info("Client sent:\n" + frame);
                }
            });
        }

        final Random random = new Random();
        for (short i = 0; i < 10; i++) {
            BlockingConnection connection = mqtts[random.nextInt(cleanClientIds.length)].blockingConnection();
            connection.connect();
            final String TOPIC = "TopicA/";
            connection.subscribe(new Topic[] { new Topic(TOPIC, QoS.EXACTLY_ONCE) });

            // publish non-retained message
            connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
            Message msg = connection.receive(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertEquals(TOPIC, new String(msg.getPayload()));
            msg.ack();

            assertEquals(1, publishMap.size());
            final short id = (short) (i + 1);
            assertNotNull("No message for id " + id, publishMap.get(id));
            publishMap.clear();

            connection.disconnect();
        }

    }

    @Test(timeout = 60 * 1000)
    public void testClientConnectionFailure() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection("reconnect", false);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        });

        final String TOPIC = "TopicA";
        final byte[] qos = connection.subscribe(new Topic[] { new Topic(TOPIC, QoS.EXACTLY_ONCE) });
        assertEquals(QoS.EXACTLY_ONCE.ordinal(), qos[0]);
        connection.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        // kill transport
        connection.kill();

        final BlockingConnection newConnection = mqtt.blockingConnection();
        newConnection.connect();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return newConnection.isConnected();
            }
        });

        assertEquals(QoS.EXACTLY_ONCE.ordinal(), qos[0]);
        Message msg = newConnection.receive(1000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(TOPIC, new String(msg.getPayload()));
        msg.ack();
        newConnection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testCleanSession() throws Exception {
        addMQTTConnector();
        brokerService.start();

        final String CLIENTID = "cleansession";
        final MQTT mqttNotClean = createMQTTConnection(CLIENTID, false);
        BlockingConnection notClean = mqttNotClean.blockingConnection();
        final String TOPIC = "TopicA";
        notClean.connect();
        notClean.subscribe(new Topic[] { new Topic(TOPIC, QoS.EXACTLY_ONCE) });
        notClean.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        notClean.disconnect();

        // MUST receive message from previous not clean session
        notClean = mqttNotClean.blockingConnection();
        notClean.connect();
        Message msg = notClean.receive(10000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(TOPIC, new String(msg.getPayload()));
        msg.ack();
        notClean.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        notClean.disconnect();

        // MUST NOT receive message from previous not clean session
        final MQTT mqttClean = createMQTTConnection(CLIENTID, true);
        final BlockingConnection clean = mqttClean.blockingConnection();
        clean.connect();
        msg = clean.receive(10000, TimeUnit.MILLISECONDS);
        assertNull(msg);
        clean.subscribe(new Topic[] { new Topic(TOPIC, QoS.EXACTLY_ONCE) });
        clean.publish(TOPIC, TOPIC.getBytes(), QoS.EXACTLY_ONCE, false);
        clean.disconnect();

        // MUST NOT receive message from previous clean session
        notClean = mqttNotClean.blockingConnection();
        notClean.connect();
        msg = notClean.receive(1000, TimeUnit.MILLISECONDS);
        assertNull(msg);
        notClean.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendMQTTReceiveJMS() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        final String DESTINATION_NAME = "foo.*";

        // send retained message
        final String RETAINED = "RETAINED";
        provider.publish("foo/bah", RETAINED.getBytes(), AT_LEAST_ONCE, true);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        // MUST set to true to receive retained messages
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(DESTINATION_NAME);
        MessageConsumer consumer = s.createConsumer(jmsTopic);

        // check whether we received retained message on JMS subscribe
        ActiveMQMessage message = (ActiveMQMessage) consumer.receive(5000);
        assertNotNull("Should get retained message", message);
        ByteSequence bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo/bah", payload.getBytes(), AT_LEAST_ONCE);
            message = (ActiveMQMessage) consumer.receive(5000);
            assertNotNull("Should get a message", message);
            bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }

        activeMQConnection.close();
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testSendJMSReceiveMQTT() throws Exception {
        addMQTTConnector();
        TransportConnector openwireTransport = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(openwireTransport.getConnectUri()).createConnection();
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic("foo.far");
        MessageProducer producer = s.createProducer(jmsTopic);

        // send retained message from JMS
        final String RETAINED = "RETAINED";
        TextMessage sendMessage = s.createTextMessage(RETAINED);
        // mark the message to be retained
        sendMessage.setBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAIN_PROPERTY, true);
        // MQTT QoS can be set using MQTTProtocolConverter.QOS_PROPERTY_NAME property
        sendMessage.setIntProperty(MQTTProtocolConverter.QOS_PROPERTY_NAME, 0);
        producer.send(sendMessage);

        provider.subscribe("foo/+", AT_MOST_ONCE);
        byte[] message = provider.receive(10000);
        assertNotNull("Should get retained message", message);
        assertEquals(RETAINED, new String(message));

        for (int i = 0; i < numberOfMessages; i++) {
            String payload = "This is Test Message: " + i;
            sendMessage = s.createTextMessage(payload);
            producer.send(sendMessage);
            message = provider.receive(5000);
            assertNotNull("Should get a message", message);

            assertEquals(payload, new String(message));
        }
        provider.disconnect();
        activeMQConnection.close();
    }

    @Test(timeout = 60 * 1000)
    public void testPingKeepsInactivityMonitorAlive() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testTurnOffInactivityMonitor() throws Exception {
        addMQTTConnector("transport.useInactivityMonitor=false");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo3");
        mqtt.setKeepAlive((short) 2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }

    @Test(timeout = 30 * 10000)
    public void testJmsMapping() throws Exception {
        addMQTTConnector();
        addOpenwireConnector();
        brokerService.start();

        // start up jms consumer
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:" + openwireConnector.getConnectUri().getPort());
        Connection jmsConn = factory.createConnection();
        Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = session.createTopic("test.foo");
        MessageConsumer consumer = session.createConsumer(dest);
        jmsConn.start();

        // set up mqtt producer
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo3");
        mqtt.setKeepAlive((short) 2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        int messagesToSend = 5;

        // publish
        for (int i = 0; i < messagesToSend; ++i) {
            connection.publish("test/foo", "hello world".getBytes(), QoS.AT_LEAST_ONCE, false);
        }

        connection.disconnect();

        for (int i = 0; i < messagesToSend; i++) {

            javax.jms.Message message = consumer.receive(2 * 1000);
            assertNotNull(message);
            assertTrue(message instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) message;

            int length = (int) bytesMessage.getBodyLength();
            byte[] buffer = new byte[length];
            bytesMessage.readBytes(buffer);
            assertEquals("hello world", new String(buffer));
        }

        jmsConn.close();

    }

    @Test(timeout = 30 * 10000)
    public void testSubscribeMultipleTopics() throws Exception {

        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }

        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Topic[] topics = { new Topic("Topic/A", QoS.EXACTLY_ONCE), new Topic("Topic/B", QoS.EXACTLY_ONCE) };
        Topic[] wildcardTopic = { new Topic("Topic/#", QoS.AT_LEAST_ONCE) };
        connection.subscribe(wildcardTopic);

        for (Topic topic : topics) {
            connection.publish(topic.name().toString(), payload, QoS.AT_LEAST_ONCE, false);
        }

        int received = 0;
        for (int i = 0; i < topics.length; ++i) {
            Message message = connection.receive();
            assertNotNull(message);
            received++;
            payload = message.getPayload();
            String messageContent = new String(payload);
            LOG.info("Received message from topic: " + message.getTopic() + " Message content: " + messageContent);
            message.ack();
        }

        assertEquals("Should have received " + topics.length + " messages", topics.length, received);
    }

    @Test(timeout = 60 * 1000)
    public void testReceiveMessageSentWhileOffline() throws Exception {
        final byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }

        int numberOfRuns = 100;
        int messagesPerRun = 2;

        addMQTTConnector("trace=true");
        brokerService.start();
        final MQTT mqttPub = createMQTTConnection("MQTT-Pub-Client", true);

        final MQTT mqttSub = createMQTTConnection("MQTT-Sub-Client", false);

        final BlockingConnection connectionPub = mqttPub.blockingConnection();
        connectionPub.connect();

        BlockingConnection connectionSub = mqttSub.blockingConnection();
        connectionSub.connect();

        Topic[] topics = { new Topic("TopicA", QoS.EXACTLY_ONCE) };
        connectionSub.subscribe(topics);

        for (int i = 0; i < messagesPerRun; ++i) {
            connectionPub.publish(topics[0].name().toString(), payload, QoS.AT_LEAST_ONCE, false);
        }

        int received = 0;
        for (int i = 0; i < messagesPerRun; ++i) {
            Message message = connectionSub.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            received++;
            assertTrue(Arrays.equals(payload, message.getPayload()));
            message.ack();
        }
        connectionSub.disconnect();

        for (int j = 0; j < numberOfRuns; j++) {

            for (int i = 0; i < messagesPerRun; ++i) {
                connectionPub.publish(topics[0].name().toString(), payload, QoS.AT_LEAST_ONCE, false);
            }

            connectionSub = mqttSub.blockingConnection();
            connectionSub.connect();
            connectionSub.subscribe(topics);

            for (int i = 0; i < messagesPerRun; ++i) {
                Message message = connectionSub.receive(5, TimeUnit.SECONDS);
                assertNotNull(message);
                received++;
                assertTrue(Arrays.equals(payload, message.getPayload()));
                message.ack();
            }
            connectionSub.disconnect();
        }
        assertEquals("Should have received " + (messagesPerRun * (numberOfRuns + 1)) + " messages", (messagesPerRun * (numberOfRuns + 1)), received);
    }

    @Test(timeout = 30 * 1000)
    public void testDefaultKeepAliveWhenClientSpecifiesZero() throws Exception {
        // default keep alive in milliseconds
        addMQTTConnector("transport.defaultKeepAlive=2000");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 0);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));
    }

    @Test(timeout = 60 * 1000)
    public void testReuseConnection() throws Exception {
        addMQTTConnector();
        brokerService.start();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("Test-Client");

        {
            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.disconnect();
            Thread.sleep(1000);
        }
        {
            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.disconnect();
            Thread.sleep(1000);
        }
    }

    @Test(timeout = 60 * 1000)
    public void testNoMessageReceivedAfterUnsubscribeMQTT() throws Exception {
        addMQTTConnector();
        brokerService.setPersistent(true);
        brokerService.start();
        Topic[] topics = { new Topic("TopicA", QoS.EXACTLY_ONCE) };

        MQTT mqttPub = createMQTTConnection("MQTTPub-Client", true);
        // mqttPub.setVersion("3.1.1");

        MQTT mqttSub = createMQTTConnection("MQTTSub-Client", false);
        // mqttSub.setVersion("3.1.1");

        BlockingConnection connectionPub = mqttPub.blockingConnection();
        connectionPub.connect();

        BlockingConnection connectionSub = mqttSub.blockingConnection();
        connectionSub.connect();
        connectionSub.subscribe(topics);
        connectionSub.disconnect();

        for (int i = 0; i < 5; i++) {
            String payload = "Message " + i;
            connectionPub.publish(topics[0].name().toString(), payload.getBytes(), QoS.EXACTLY_ONCE, false);
        }

        connectionSub = mqttSub.blockingConnection();
        connectionSub.connect();

        int received = 0;
        for (int i = 0; i < 5; ++i) {
            Message message = connectionSub.receive(5, TimeUnit.SECONDS);
            assertNotNull("Missing message " + i, message);
            LOG.info("Message is " + new String(message.getPayload()));
            received++;
            message.ack();
        }
        assertEquals(5, received);

        // unsubscribe from topic
        connectionSub.unsubscribe(new String[]{"TopicA"});

        // send more messages
        for (int i = 0; i < 5; i++) {
            String payload = "Message " + i;
            connectionPub.publish(topics[0].name().toString(), payload.getBytes(), QoS.EXACTLY_ONCE, false);
        }

        // these should not be received
        assertNull(connectionSub.receive(5, TimeUnit.SECONDS));

        connectionSub.disconnect();
        connectionPub.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testMQTT311Connection() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setVersion("3.1.1");
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testWildcardRetainedSubscription() throws Exception {
        addMQTTConnector();

        LinkedList<AuthenticationUser> users = new LinkedList<AuthenticationUser>();
        users.add(new AuthenticationUser("user", "user", "users"));
        users.add(new AuthenticationUser("admin", "admin", "admins"));
        final SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);


        DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        LinkedList<DestinationMapEntry> authz = new LinkedList<DestinationMapEntry>();

        AuthorizationEntry entryOne = new AuthorizationEntry();
        entryOne.setDestination(new ActiveMQTopic("one"));
        entryOne.setAdmin("admins");
        entryOne.setRead("admins");
        entryOne.setWrite("admins");
        authz.add(entryOne);

        AuthorizationEntry entryTwo = new AuthorizationEntry();
        entryTwo.setDestination(new ActiveMQTopic("two"));
        entryTwo.setAdmin("users");
        entryTwo.setRead("users");
        entryTwo.setWrite("users");
        authz.add(entryTwo);

        map.setAuthorizationEntries(authz);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(map);

        brokerService.setPlugins(new BrokerPlugin[] { authorizationPlugin, authenticationPlugin });

        brokerService.start();

        MQTT mqttPub = createMQTTConnection("pub", true);
        mqttPub.setUserName("admin");
        mqttPub.setPassword("admin");

        BlockingConnection connectionPub = mqttPub.blockingConnection();
        connectionPub.connect();
        connectionPub.publish("one", "test".getBytes(), QoS.AT_LEAST_ONCE, true);

        MQTT mqttSub = createMQTTConnection("sub", true);
        mqttSub.setUserName("user");
        mqttSub.setPassword("user");
        BlockingConnection connectionSub = mqttSub.blockingConnection();
        connectionSub.connect();
        connectionSub.subscribe(new Topic[]{new Topic("#", QoS.AT_LEAST_ONCE)});
        Message msg = connectionSub.receive(1, TimeUnit.SECONDS);
        assertNull("Shouldn't receive the message", msg);
    }

    @Test(timeout = 60 * 1000)
    public void testActiveMQRecoveryPolicy() throws Exception {
        addMQTTConnector();

        brokerService.start();

        // test with ActiveMQ LastImageSubscriptionRecoveryPolicy
        final PolicyMap policyMap = new PolicyMap();
        final PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setSubscriptionRecoveryPolicy(new LastImageSubscriptionRecoveryPolicy());
        policyMap.put(new ActiveMQTopic(">"), policyEntry);
        brokerService.setDestinationPolicy(policyMap);

        MQTT mqtt = createMQTTConnection("pub-sub", true);
        final int[] retain = new int[1];
        final int[] nonretain  = new int[1];
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                if (frame.messageType() == PUBLISH.TYPE) {
                    LOG.info("Received message with retain=" + frame.retain());
                    if (frame.retain()) {
                        retain[0]++;
                    } else {
                        nonretain[0]++;
                    }
                }
            }
        });

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        final String RETAINED = "RETAINED";
        connection.publish("one", RETAINED.getBytes(), QoS.AT_LEAST_ONCE, true);
        connection.publish("two", RETAINED.getBytes(), QoS.AT_LEAST_ONCE, true);

        final String NONRETAINED = "NONRETAINED";
        connection.publish("one", NONRETAINED.getBytes(), QoS.AT_LEAST_ONCE, false);
        connection.publish("two", NONRETAINED.getBytes(), QoS.AT_LEAST_ONCE, false);

        connection.subscribe(new Topic[]{new Topic("#", QoS.AT_LEAST_ONCE)});
        for (int i = 0; i < 4; i++) {
            final Message message = connection.receive(30, TimeUnit.SECONDS);
            assertNotNull("Should receive 4 messages", message);
            message.ack();
        }
        assertEquals("Should receive 2 retained messages", 2, retain[0]);
        assertEquals("Should receive 2 non-retained messages", 2, nonretain[0]);
    }

    @Override
    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected MQTTClientProvider getMQTTClientProvider() {
        return new FuseMQQTTClientProvider();
    }

    protected MQTT createMQTTConnection() throws Exception {
        return createMQTTConnection(null, false);
    }

    protected MQTT createMQTTConnection(String clientId, boolean clean) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setTracer(createTracer());
        if (clientId != null) {
            mqtt.setClientId(clientId);
        }
        mqtt.setCleanSession(clean);
        mqtt.setHost("localhost", mqttConnector.getConnectUri().getPort());
        // shut off connect retry
        return mqtt;
    }

    protected Tracer createTracer() {
        return new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client Received:\n" + frame);
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client Sent:\n" + frame);
            }

            @Override
            public void debug(String message, Object... args) {
                LOG.info(String.format(message, args));
            }
        };
    }
}
