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

import static org.fusesource.hawtbuf.UTF8Buffer.utf8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.ProtocolException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RetainedMessageSubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
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

public class MQTTTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTTest.class);

    private static final int NUM_MESSAGES = 200;

    @Test(timeout = 60 * 1000)
    public void testSendAndReceiveMQTT() throws Exception {
        final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
        initializeConnection(subscriptionProvider);

        subscriptionProvider.subscribe("foo/bah", AT_MOST_ONCE);

        final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < NUM_MESSAGES; i++) {
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

        for (int i = 0; i < NUM_MESSAGES; i++) {
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
        final MQTTClientProvider subscriptionProvider = getMQTTClientProvider();
        initializeConnection(subscriptionProvider);

        String topic = "foo/bah";

        subscriptionProvider.subscribe(topic, AT_MOST_ONCE);

        final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES / 2);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < NUM_MESSAGES; i++) {
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

        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Message " + i;
            if (i == NUM_MESSAGES / 2) {
                latch.await(20, TimeUnit.SECONDS);
                subscriptionProvider.unsubscribe(topic);
            }
            publishProvider.publish(topic, payload.getBytes(), AT_LEAST_ONCE);
        }

        latch.await(20, TimeUnit.SECONDS);
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
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testSendAtLeastOnceReceiveExactlyOnce() throws Exception {
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_LEAST_ONCE);
            byte[] message = provider.receive(2000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testSendAtLeastOnceReceiveAtMostOnce() throws Exception {
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_MOST_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
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
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_MOST_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Test Message: " + i;
            provider.publish("foo", payload.getBytes(), AT_MOST_ONCE);
            byte[] message = provider.receive(5000);
            assertNotNull("Should get a message", message);
            assertEquals(payload, new String(message));
        }
        provider.disconnect();
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testSendAndReceiveAtLeastOnce() throws Exception {
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        provider.subscribe("foo", AT_LEAST_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
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
        final MQTTClientProvider publisher = getMQTTClientProvider();
        initializeConnection(publisher);

        final MQTTClientProvider subscriber = getMQTTClientProvider();
        initializeConnection(subscriber);

        subscriber.subscribe("foo", EXACTLY_ONCE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
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
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("");
        mqtt.setCleanSession(true);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }

    @Test(timeout = 30 * 1000)
    public void testConnectWithUserButNoPassword() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("test");
        mqtt.setUserName("foo");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }

    @Test(timeout = 30 * 1000)
    public void testConnectWithPasswordButNoUsername() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setVersion("3.1.1"); // The V3.1 spec doesn't make the same assertion
        mqtt.setClientId("test");
        mqtt.setPassword("bar");

        BlockingConnection connection = mqtt.blockingConnection();

        try {
            connection.connect();
            fail("Should not be able to connect in this case.");
        } catch (Exception ex) {
            LOG.info("Exception expected on connect with password but no username");
        }
    }

    @Test(timeout = 2 *  60 * 1000)
    public void testMQTTWildcard() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("");
        mqtt.setCleanSession(true);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Topic[] topics = {new Topic(utf8("a/#"), QoS.values()[AT_MOST_ONCE])};
        connection.subscribe(topics);
        String payload = "Test Message";
        String publishedTopic = "a/b/1.2.3*4>";
        connection.publish(publishedTopic, payload.getBytes(), QoS.values()[AT_MOST_ONCE], false);

        Message msg = connection.receive(1, TimeUnit.SECONDS);
        assertEquals("Topic changed", publishedTopic, msg.getTopic());
    }

    @Test(timeout = 2 *  60 * 1000)
    public void testMQTTCompositeDestinations() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("");
        mqtt.setCleanSession(true);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Topic[] topics = {new Topic(utf8("a/1"), QoS.values()[AT_MOST_ONCE]), new Topic(utf8("a/2"), QoS.values()[AT_MOST_ONCE])};
        connection.subscribe(topics);

        String payload = "Test Message";
        String publishedTopic = "a/1,a/2";
        connection.publish(publishedTopic, payload.getBytes(), QoS.values()[AT_MOST_ONCE], false);

        Message msg = connection.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals("a/2", msg.getTopic());

        msg = connection.receive(1, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals("a/1", msg.getTopic());

        msg = connection.receive(1, TimeUnit.SECONDS);
        assertNull(msg);

    }

    @Test(timeout = 2 *  60 * 1000)
    public void testMQTTPathPatterns() throws Exception {
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
            Message msg = connection.receive(2, TimeUnit.SECONDS);
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
        doTestRetainedMessages("TopicA");
    }

    @Test(timeout = 120 * 1000)
    public void testRetainedMessageOnVirtualTopics() throws Exception {
        doTestRetainedMessages("VirtualTopic/TopicA");
    }

    public void doTestRetainedMessages(String topicName) throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short) 60);

        final String RETAIN = "RETAIN";
        final String TOPICA = topicName;

        final String[] clientIds = { null, "foo", "durable" };
        for (String clientId : clientIds) {
            boolean cleanSession = !"durable".equals(clientId);
            LOG.info("Testing now with Client ID: {} clean: {}", clientId, cleanSession);

            mqtt.setClientId(clientId);
            mqtt.setCleanSession(cleanSession);

            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            // set retained message and check
            connection.publish(TOPICA, RETAIN.getBytes(), QoS.EXACTLY_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            Message msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(500, TimeUnit.MILLISECONDS));

            // test duplicate subscription
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(15000, TimeUnit.MILLISECONDS);
            assertNotNull("No retained message on duplicate subscription for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(500, TimeUnit.MILLISECONDS));
            connection.unsubscribe(new String[]{TOPICA});

            // clear retained message and check that we don't receive it
            connection.publish(TOPICA, "".getBytes(), QoS.AT_MOST_ONCE, true);
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(500, TimeUnit.MILLISECONDS);
            assertNull("Retained message not cleared for " + clientId, msg);
            connection.unsubscribe(new String[]{TOPICA});

            // set retained message again and check
            connection.publish(TOPICA, RETAIN.getBytes(), QoS.EXACTLY_ONCE, true);
            LOG.info("Performing first subscription");
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No reset retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(500, TimeUnit.MILLISECONDS));

            // re-connect and check
            connection.disconnect();
            connection = mqtt.blockingConnection();
            connection.connect();
            LOG.info("Performing second subscription");
            connection.subscribe(new Topic[]{new Topic(TOPICA, QoS.AT_LEAST_ONCE)});
            msg = connection.receive(5000, TimeUnit.MILLISECONDS);
            assertNotNull("No reset retained message for " + clientId, msg);
            assertEquals(RETAIN, new String(msg.getPayload()));
            msg.ack();
            assertNull(connection.receive(500, TimeUnit.MILLISECONDS));

            LOG.info("Test now unsubscribing from: {} for the last time", TOPICA);
            connection.unsubscribe(new String[]{TOPICA});
            connection.disconnect();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testUniqueMessageIds() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setKeepAlive((short) 2);
        mqtt.setCleanSession(true);

        final List<PUBLISH> publishList = new ArrayList<PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.debug("Client received:\n" + frame);
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
                LOG.debug("Client sent:\n" + frame);
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

        Message msg = connection.receive(2000, TimeUnit.MILLISECONDS);
        do {
            assertNotNull(msg);
            assertEquals(TOPIC, new String(msg.getPayload()));
            msg.ack();
            int waitCount = 0;
            while (publishList.size() <= received && waitCount < 10) {
                Thread.sleep(1000);
                waitCount++;
            }
            msg = connection.receive(2000, TimeUnit.MILLISECONDS);
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
        final MQTT mqtt = createMQTTConnection("resend", false);
        mqtt.setKeepAlive((short) 5);

        final List<PUBLISH> publishList = new ArrayList<PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.debug("Client received:\n" + frame);
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
                LOG.debug("Client sent:\n" + frame);
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
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(100));
        assertEquals(2, publishList.size());

        connection.disconnect();

        connection = mqtt.blockingConnection();
        connection.connect();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return publishList.size() == 4;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(100));
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
        final MQTT mqtt = createMQTTConnection("nonclean-packetid", false);
        mqtt.setKeepAlive((short) 15);

        final Map<Short, PUBLISH> publishMap = new ConcurrentHashMap<Short, PUBLISH>();
        mqtt.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.debug("Client received:\n" + frame);
                if (frame.messageType() == PUBLISH.TYPE) {
                    PUBLISH publish = new PUBLISH();
                    try {
                        publish.decode(frame);
                        LOG.debug("PUBLISH " + publish);
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
                LOG.debug("Client sent:\n" + frame);
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
        final String[] cleanClientIds = new String[] { "", "clean-packetid", null };
        final Map<Short, PUBLISH> publishMap = new ConcurrentHashMap<Short, PUBLISH>();
        MQTT[] mqtts = new MQTT[cleanClientIds.length];
        for (int i = 0; i < cleanClientIds.length; i++) {
            mqtts[i] = createMQTTConnection("", true);
            mqtts[i].setKeepAlive((short) 15);

            mqtts[i].setTracer(new Tracer() {
                @Override
                public void onReceive(MQTTFrame frame) {
                    LOG.debug("Client received:\n" + frame);
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
                    LOG.debug("Client sent:\n" + frame);
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
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return newConnection.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        assertEquals(QoS.EXACTLY_ONCE.ordinal(), qos[0]);
        Message msg = newConnection.receive(1000, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        assertEquals(TOPIC, new String(msg.getPayload()));
        msg.ack();
        newConnection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testNoClientId() throws Exception {
        final MQTT mqtt = createMQTTConnection("", true);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        connection.subscribe(new Topic[]{new Topic("TopicA", QoS.AT_LEAST_ONCE)});
        connection.publish("TopicA", "test".getBytes(), QoS.AT_LEAST_ONCE, true);
        Message message = connection.receive(3, TimeUnit.SECONDS);
        assertNotNull(message);
        //TODO fix audit problem for retained messages
        //Thread.sleep(2000);
        //connection.subscribe(new Topic[]{new Topic("TopicA", QoS.AT_LEAST_ONCE)});
        //message = connection.receive(3, TimeUnit.SECONDS);
        //assertNotNull(message);
    }

    @Test(timeout = 60 * 1000)
    public void testCleanSession() throws Exception {
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
        msg = clean.receive(2000, TimeUnit.MILLISECONDS);
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
        doTestSendMQTTReceiveJMS("foo.*");
    }

    public void doTestSendMQTTReceiveJMS(String destinationName) throws Exception {
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        // send retained message
        final String RETAINED = "RETAINED";
        provider.publish("foo/bah", RETAINED.getBytes(), AT_LEAST_ONCE, true);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) cf.createConnection();
        // MUST set to true to receive retained messages
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(destinationName);
        MessageConsumer consumer = s.createConsumer(jmsTopic);

        // check whether we received retained message on JMS subscribe
        ActiveMQMessage message = (ActiveMQMessage) consumer.receive(5000);
        assertNotNull("Should get retained message", message);
        ByteSequence bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));

        for (int i = 0; i < NUM_MESSAGES; i++) {
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

    @Test(timeout = 2 * 60 * 1000)
    public void testSendJMSReceiveMQTT() throws Exception {
        doTestSendJMSReceiveMQTT("foo.far");
    }

    public void doTestSendJMSReceiveMQTT(String destinationName) throws Exception {
        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) cf.createConnection();
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic jmsTopic = s.createTopic(destinationName);
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

        for (int i = 0; i < NUM_MESSAGES; i++) {
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
        stopBroker();
        protocolConfig = "transport.useInactivityMonitor=false";
        startBroker();

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

    @Test(timeout = 60 * 1000)
    public void testPublishDollarTopics() throws Exception {
        MQTT mqtt = createMQTTConnection();
        final String clientId = "publishDollar";
        mqtt.setClientId(clientId);
        mqtt.setKeepAlive((short) 2);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final String DOLLAR_TOPIC = "$TopicA";
        connection.subscribe(new Topic[] { new Topic(DOLLAR_TOPIC, QoS.EXACTLY_ONCE)});
        connection.publish(DOLLAR_TOPIC, DOLLAR_TOPIC.getBytes(), QoS.EXACTLY_ONCE, true);

        Message message = connection.receive(3, TimeUnit.SECONDS);
        assertNull("Publish enabled for $ Topics by default", message);

        connection.disconnect();

        stopBroker();
        protocolConfig = "transport.publishDollarTopics=true";
        startBroker();

        mqtt = createMQTTConnection();
        mqtt.setClientId(clientId);
        mqtt.setKeepAlive((short) 2);
        connection = mqtt.blockingConnection();
        connection.connect();

        connection.subscribe(new Topic[] { new Topic(DOLLAR_TOPIC, QoS.EXACTLY_ONCE)});
        connection.publish(DOLLAR_TOPIC, DOLLAR_TOPIC.getBytes(), QoS.EXACTLY_ONCE, true);

        message = connection.receive(10, TimeUnit.SECONDS);
        assertNotNull(message);
        message.ack();
        assertEquals("Message body", DOLLAR_TOPIC, new String(message.getPayload()));

        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testPublishWildcard31() throws Exception {
        testPublishWildcard("3.1");
    }

    @Test(timeout = 60 * 1000)
    public void testPublishWildcard311() throws Exception {
        testPublishWildcard("3.1.1");
    }

    private void testPublishWildcard(String version) throws Exception {
        MQTT mqttPub = createMQTTConnection("MQTTPub-Client", true);
        mqttPub.setVersion(version);
        BlockingConnection blockingConnection = mqttPub.blockingConnection();
        blockingConnection.connect();
        String payload = "Test Message";
        try {
            blockingConnection.publish("foo/#", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            fail("Should not be able to publish with wildcard in topic.");
        } catch (Exception ex) {
            LOG.info("Exception expected on publish with wildcard in topic name");
        }
        try {
            blockingConnection.publish("foo/+", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
            fail("Should not be able to publish with wildcard in topic.");
        } catch (Exception ex) {
            LOG.info("Exception expected on publish with wildcard in topic name");
        }
        blockingConnection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testDuplicateClientId() throws Exception {
        // test link stealing enabled by default
        final String clientId = "duplicateClient";
        MQTT mqtt = createMQTTConnection(clientId, false);
        mqtt.setKeepAlive((short) 2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        final String TOPICA = "TopicA";
        connection.publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);

        MQTT mqtt1 = createMQTTConnection(clientId, false);
        mqtt1.setKeepAlive((short) 2);
        final BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();

        assertTrue("Duplicate client disconnected", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return connection1.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        assertTrue("Old client still connected", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return !connection.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100)));

        connection1.publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);
        connection1.disconnect();

        // disable link stealing
        stopBroker();
        protocolConfig = "allowLinkStealing=false";
        startBroker();

        mqtt = createMQTTConnection(clientId, false);
        mqtt.setKeepAlive((short) 2);
        final BlockingConnection connection2 = mqtt.blockingConnection();
        connection2.connect();
        connection2.publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);

        mqtt1 = createMQTTConnection(clientId, false);
        mqtt1.setKeepAlive((short) 2);
        final BlockingConnection connection3 = mqtt1.blockingConnection();
        try {
            connection3.connect();
            fail("Duplicate client connected");
        } catch (Exception e) {
            // ignore
        }

        assertTrue("Old client disconnected", connection2.isConnected());
        connection2.publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);
        connection2.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testRepeatedLinkStealing() throws Exception {
        final String clientId = "duplicateClient";
        final AtomicReference<BlockingConnection> oldConnection = new AtomicReference<BlockingConnection>();
        final String TOPICA = "TopicA";

        for (int i = 1; i <= 10; ++i) {

            LOG.info("Creating MQTT Connection {}", i);

            MQTT mqtt = createMQTTConnection(clientId, false);
            mqtt.setKeepAlive((short) 2);
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);

            assertTrue("Client connect failed for attempt: " + i, Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return connection.isConnected();
                }
            }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(200)));

            if (oldConnection.get() != null) {
                assertTrue("Old client still connected", Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return !oldConnection.get().isConnected();
                    }
                }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(200)));
            }

            oldConnection.set(connection);
        }

        oldConnection.get().publish(TOPICA, TOPICA.getBytes(), QoS.EXACTLY_ONCE, true);
        oldConnection.get().disconnect();
    }

    @Test(timeout = 30 * 10000)
    public void testJmsMapping() throws Exception {
        doTestJmsMapping("test.foo");
    }

    public void doTestJmsMapping(String destinationName) throws Exception {
        // start up jms consumer
        Connection jmsConn = cf.createConnection();
        Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = session.createTopic(destinationName);
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
    public void testSubscribeWithLargeTopicFilter() throws Exception {

        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }

        StringBuilder topicBuilder = new StringBuilder("/topic/");

        for (int i = 0; i < 32800; ++i) {
            topicBuilder.append("a");
        }

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Topic[] topic = { new Topic(topicBuilder.toString(), QoS.EXACTLY_ONCE) };
        connection.subscribe(topic);
        connection.publish(topic[0].name().toString(), payload, QoS.AT_LEAST_ONCE, false);

        Message message = connection.receive();
        assertNotNull(message);
        message.ack();
    }

    @Test(timeout = 30 * 10000)
    public void testSubscribeWithZeroLengthTopic() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic topic = new Topic("", QoS.EXACTLY_ONCE);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        LOG.info("Trying to subscrobe to topic: {}", topic.name());

        try {
            connection.subscribe(new Topic[] { topic });
            fail("Should not be able to subscribe with invalid Topic");
        } catch (Exception ex) {
            LOG.info("Caught expected error on subscribe");
        }

        assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !connection.isConnected();
            }
        }));
    }

    @Test(timeout = 30 * 10000)
    public void testUnsubscribeWithZeroLengthTopic() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic topic = new Topic("", QoS.EXACTLY_ONCE);

        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        LOG.info("Trying to subscrobe to topic: {}", topic.name());

        try {
            connection.unsubscribe(new String[] { topic.name().toString() });
            fail("Should not be able to subscribe with invalid Topic");
        } catch (Exception ex) {
            LOG.info("Caught expected error on subscribe");
        }

        assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !connection.isConnected();
            }
        }));
    }

    @Test(timeout = 30 * 10000)
    public void testSubscribeWithInvalidMultiLevelWildcards() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic[] topics = { new Topic("#/Foo", QoS.EXACTLY_ONCE),
                           new Topic("#/Foo/#", QoS.EXACTLY_ONCE),
                           new Topic("Foo/#/Level", QoS.EXACTLY_ONCE),
                           new Topic("Foo/X#", QoS.EXACTLY_ONCE) };

        for (int i = 0; i < topics.length; ++i) {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            LOG.info("Trying to subscrobe to topic: {}", topics[i].name());

            try {
                connection.subscribe(new Topic[] { topics[i] });
                fail("Should not be able to subscribe with invalid Topic");
            } catch (Exception ex) {
                LOG.info("Caught expected error on subscribe");
            }

            assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return !connection.isConnected();
                }
            }));
        }
    }

    @Test(timeout = 30 * 10000)
    public void testSubscribeWithInvalidSingleLevelWildcards() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic[] topics = { new Topic("Foo+", QoS.EXACTLY_ONCE),
                           new Topic("+Foo/#", QoS.EXACTLY_ONCE),
                           new Topic("+#", QoS.EXACTLY_ONCE),
                           new Topic("Foo/+X/Level", QoS.EXACTLY_ONCE),
                           new Topic("Foo/+F", QoS.EXACTLY_ONCE) };

        for (int i = 0; i < topics.length; ++i) {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            LOG.info("Trying to subscrobe to topic: {}", topics[i].name());

            try {
                connection.subscribe(new Topic[] { topics[i] });
                fail("Should not be able to subscribe with invalid Topic");
            } catch (Exception ex) {
                LOG.info("Caught expected error on subscribe");
            }

            assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return !connection.isConnected();
                }
            }));
        }
    }

    @Test(timeout = 30 * 10000)
    public void testUnsubscribeWithInvalidMultiLevelWildcards() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic[] topics = { new Topic("#/Foo", QoS.EXACTLY_ONCE),
                           new Topic("#/Foo/#", QoS.EXACTLY_ONCE),
                           new Topic("Foo/#/Level", QoS.EXACTLY_ONCE),
                           new Topic("Foo/X#", QoS.EXACTLY_ONCE) };

        for (int i = 0; i < topics.length; ++i) {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            LOG.info("Trying to subscrobe to topic: {}", topics[i].name());

            try {
                connection.unsubscribe(new String[] { topics[i].name().toString() });
                fail("Should not be able to unsubscribe with invalid Topic");
            } catch (Exception ex) {
                LOG.info("Caught expected error on subscribe");
            }

            assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return !connection.isConnected();
                }
            }));
        }
    }

    @Test(timeout = 30 * 10000)
    public void testUnsubscribeWithInvalidSingleLevelWildcards() throws Exception {

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("MQTT-Client");
        mqtt.setCleanSession(false);

        Topic[] topics = { new Topic("Foo+", QoS.EXACTLY_ONCE),
                           new Topic("+Foo/#", QoS.EXACTLY_ONCE),
                           new Topic("+#", QoS.EXACTLY_ONCE),
                           new Topic("Foo/+X/Level", QoS.EXACTLY_ONCE),
                           new Topic("Foo/+F", QoS.EXACTLY_ONCE) };

        for (int i = 0; i < topics.length; ++i) {
            final BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();

            LOG.info("Trying to subscrobe to topic: {}", topics[i].name());

            try {
                connection.unsubscribe(new String[] { topics[i].name().toString() });
                fail("Should not be able to unsubscribe with invalid Topic");
            } catch (Exception ex) {
                LOG.info("Caught expected error on subscribe");
            }

            assertTrue("Should have lost connection", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return !connection.isConnected();
                }
            }));
        }
    }

    @Test(timeout = 30 * 10000)
    public void testSubscribeMultipleTopics() throws Exception {

        byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }

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

        try {
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
        } catch (Exception exception) {
            LOG.error("unexpected exception", exception);
            exception.printStackTrace();
        }
        assertEquals("Should have received " + (messagesPerRun * (numberOfRuns + 1)) + " messages", (messagesPerRun * (numberOfRuns + 1)), received);
    }

    @Test(timeout = 60 * 1000)
    public void testReceiveMessageSentWhileOfflineAndBrokerRestart() throws Exception {
        stopBroker();
        this.persistent = true;
        startBroker();

        final byte[] payload = new byte[1024 * 32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = '2';
        }

        int messagesPerRun = 10;

        Topic[] topics = { new Topic("TopicA", QoS.EXACTLY_ONCE) };

        {
            // Establish a durable subscription.
            MQTT mqttSub = createMQTTConnection("MQTT-Sub-Client", false);
            BlockingConnection connectionSub = mqttSub.blockingConnection();
            connectionSub.connect();
            connectionSub.subscribe(topics);
            connectionSub.disconnect();
        }

        MQTT mqttPubLoop = createMQTTConnection("MQTT-Pub-Client", true);
        BlockingConnection connectionPub = mqttPubLoop.blockingConnection();
        connectionPub.connect();

        for (int i = 0; i < messagesPerRun; ++i) {
            connectionPub.publish(topics[0].name().toString(), payload, QoS.AT_LEAST_ONCE, false);
        }

        connectionPub.disconnect();

        restartBroker();

        MQTT mqttSub = createMQTTConnection("MQTT-Sub-Client", false);
        BlockingConnection connectionSub = mqttSub.blockingConnection();
        connectionSub.connect();
        connectionSub.subscribe(topics);

        for (int i = 0; i < messagesPerRun; ++i) {
            Message message = connectionSub.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            assertTrue(Arrays.equals(payload, message.getPayload()));
            message.ack();
        }
        connectionSub.disconnect();
    }

    @Test(timeout = 30 * 1000)
    public void testDefaultKeepAliveWhenClientSpecifiesZero() throws Exception {
        stopBroker();
        protocolConfig = "transport.defaultKeepAlive=2000";
        startBroker();

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
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("Test-Client");

        {
            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.disconnect();
            Thread.sleep(500);
        }
        {
            BlockingConnection connection = mqtt.blockingConnection();
            connection.connect();
            connection.disconnect();
            Thread.sleep(500);
        }
    }

    @Test(timeout = 60 * 1000)
    public void testNoMessageReceivedAfterUnsubscribeMQTT() throws Exception {
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
        assertNull(connectionSub.receive(2, TimeUnit.SECONDS));

        connectionSub.disconnect();
        connectionPub.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testMQTT311Connection() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setVersion("3.1.1");
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testActiveMQRecoveryPolicy() throws Exception {
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

    @Test(timeout = 60 * 1000)
    public void testSendMQTTReceiveJMSVirtualTopic() throws Exception {

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);
        final String DESTINATION_NAME = "Consumer.jms.VirtualTopic.TopicA";

        // send retained message
        final String RETAINED = "RETAINED";
        final String MQTT_DESTINATION_NAME = "VirtualTopic/TopicA";
        provider.publish(MQTT_DESTINATION_NAME, RETAINED.getBytes(), AT_LEAST_ONCE, true);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(jmsUri).createConnection();
        // MUST set to true to receive retained messages
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue jmsQueue = s.createQueue(DESTINATION_NAME);
        MessageConsumer consumer = s.createConsumer(jmsQueue);

        // check whether we received retained message on JMS subscribe
        ActiveMQMessage message = (ActiveMQMessage) consumer.receive(5000);
        assertNotNull("Should get retained message", message);
        ByteSequence bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));

        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Test Message: " + i;
            provider.publish(MQTT_DESTINATION_NAME, payload.getBytes(), AT_LEAST_ONCE);
            message = (ActiveMQMessage) consumer.receive(5000);
            assertNotNull("Should get a message", message);
            bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }

        // re-create consumer and check we received retained message again
        consumer.close();
        consumer = s.createConsumer(jmsQueue);
        message = (ActiveMQMessage) consumer.receive(5000);
        assertNotNull("Should get retained message", message);
        bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));

        activeMQConnection.close();
        provider.disconnect();
    }

    @Test(timeout = 60 * 1000)
    public void testPingOnMQTT() throws Exception {
        stopBroker();
        protocolConfig = "maxInactivityDuration=-1";
        startBroker();

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("test-mqtt");
        mqtt.setKeepAlive((short)2);
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

    @Test
    public void testConnectWithLargePassword() throws Exception {
       for (String version : Arrays.asList("3.1", "3.1.1")) {
          String longString = new String(new char[65535]);

          BlockingConnection connection = null;
          try {
             MQTT mqtt = createMQTTConnection("test-" + version, true);
             mqtt.setUserName(longString);
             mqtt.setPassword(longString);
             mqtt.setConnectAttemptsMax(1);
             mqtt.setVersion(version);
             connection = mqtt.blockingConnection();
             connection.connect();
             final BlockingConnection con = connection;
             assertTrue(Wait.waitFor(new Wait.Condition() {
                 @Override
                 public boolean isSatisified() throws Exception {
                     return con.isConnected();
                 }
             }));
          } finally {
             if (connection != null && connection.isConnected()) connection.disconnect();
          }
       }
    }
}
