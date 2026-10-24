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
package org.apache.activemq.usecases;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.SharedTopicConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Parameterized test validating that topic consumer advisories fire correctly
 * across all combinations of broker advisory support, consumer type
 * (durable/non-durable, shared/non-shared), and single vs multiple consumers.
 *
 * <p>Consumer add/remove advisories are gated only by the broker-wide
 * {@code advisorySupport} flag. There is no per-destination policy entry
 * that controls consumer lifecycle advisories.
 */
@RunWith(Parameterized.class)
@Category(ParallelTest.class)
public class TopicConsumerAdvisoryTest {

    private static final AtomicInteger BROKER_SEQ = new AtomicInteger();
    private static final String TOPIC_NAME = "test.advisory.topic";

    @Parameters(name = "brokerAdv={0}, durable={1}, shared={2}, multiple={3}, expected={4}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (boolean broker : new boolean[]{true, false}) {
            for (boolean durable : new boolean[]{true, false}) {
                for (boolean shared : new boolean[]{true, false}) {
                    for (boolean multiple : new boolean[]{true, false}) {
                        boolean expected = broker;
                        params.add(new Object[]{broker, durable, shared, multiple, expected});
                    }
                }
            }
        }
        return params;
    }

    private final boolean brokerAdvisory;
    private final boolean durable;
    private final boolean shared;
    private final boolean multiple;
    private final boolean expected;

    private SharedTopicBrokerService broker;
    private String brokerUrl;
    private final List<Connection> connections = new ArrayList<>();

    public TopicConsumerAdvisoryTest(boolean brokerAdvisory,
            boolean durable, boolean shared, boolean multiple, boolean expected) {
        this.brokerAdvisory = brokerAdvisory;
        this.durable = durable;
        this.shared = shared;
        this.multiple = multiple;
        this.expected = expected;
    }

    @Before
    public void setUp() throws Exception {
        String brokerName = "adv-test-" + BROKER_SEQ.incrementAndGet();
        brokerUrl = "vm://" + brokerName;

        broker = new SharedTopicBrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.setAdvisorySupport(brokerAdvisory);

        broker.addConnector(brokerUrl);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        for (Connection c : connections) {
            try { c.close(); } catch (Exception ignored) {}
        }
        connections.clear();
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testConsumerAdvisory() throws Exception {
        ActiveMQTopic dest = new ActiveMQTopic(TOPIC_NAME);
        ActiveMQTopic advisoryDest = AdvisorySupport.getConsumerAdvisoryTopic(dest);

        ActiveMQConnectionFactory advFactory = new ActiveMQConnectionFactory(brokerUrl);
        Connection advConn = advFactory.createConnection();
        connections.add(advConn);
        advConn.start();
        Session advSession = advConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advConsumer = advSession.createConsumer(advisoryDest);

        Thread.sleep(100);

        int consumerCount = multiple ? 2 : 1;
        createTestConsumers(consumerCount);

        int expectedCount = expected ? consumerCount : 0;

        List<Message> received = new ArrayList<>();
        for (int i = 0; i < expectedCount; i++) {
            Message msg = advConsumer.receive(5000);
            if (msg != null) {
                received.add(msg);
            }
        }

        Message extra = advConsumer.receive(500);
        assertNull("Should not receive more than " + expectedCount + " advisory message(s)", extra);

        assertEquals("Advisory message count", expectedCount, received.size());

        for (Message msg : received) {
            assertTrue("Advisory should carry consumerCount property",
                    msg.propertyExists(AdvisorySupport.MSG_PROPERTY_CONSUMER_COUNT));
        }
    }

    private void createTestConsumers(int count) throws Exception {
        if (shared) {
            createSharedConsumers(count);
        } else {
            createNonSharedConsumers(count);
        }
    }

    private void createSharedConsumers(int count) throws Exception {
        SharedTopicConnectionFactory sharedFactory = new SharedTopicConnectionFactory(brokerUrl);
        for (int i = 0; i < count; i++) {
            Connection conn = sharedFactory.createConnection();
            connections.add(conn);
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME);
            if (durable) {
                session.createSharedDurableConsumer(topic, "sharedSub");
            } else {
                session.createSharedConsumer(topic, "sharedSub");
            }
        }
    }

    private void createNonSharedConsumers(int count) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        for (int i = 0; i < count; i++) {
            Connection conn = factory.createConnection();
            connections.add(conn);
            if (durable) {
                conn.setClientID("client-" + i);
            }
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic(TOPIC_NAME);
            if (durable) {
                session.createDurableSubscriber(topic, "sub-" + i);
            } else {
                session.createConsumer(topic);
            }
        }
    }
}
