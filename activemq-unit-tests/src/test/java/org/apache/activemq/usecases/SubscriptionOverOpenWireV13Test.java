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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.SharedTopicConnectionFactory;
import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Topic subscribers that reach the broker over a TCP connection negotiated at
 * OpenWire v13, where every consumer is unmarshalled as a SharedConsumerInfo.
 */
public class SubscriptionOverOpenWireV13Test {

    private static final String TOPIC_NAME = "test.subscription.v13";
    private static final String V13 = "?wireFormat.version=13";

    private SharedTopicBrokerService broker;
    private String url;

    @Before
    public void setUp() throws Exception {
        broker = new SharedTopicBrokerService();
        broker.setBrokerName("v13-subscriptions");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        final var connector = broker.addConnector("tcp://localhost:0" + V13);
        broker.start();
        broker.waitUntilStarted();
        url = "tcp://localhost:" + connector.getConnectUri().getPort() + V13;
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    // An ordinary durable subscriber is still durable: the broker keeps its
    // subscription, and its messages, while it is away.
    @Test(timeout = 60000)
    public void testOrdinaryDurableSubscriberStaysDurable() throws Exception {
        final var factory = new ActiveMQConnectionFactory(url);

        try (var conn = factory.createConnection()) {
            conn.setClientID("client-1");
            conn.start();
            final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createDurableSubscriber(session.createTopic(TOPIC_NAME), "sub-1");
            assertTrue("durable subscription on the broker",
                Wait.waitFor(() -> durableSubscriptionCount() == 1, 5000, 10));
        }

        send("while-away-1", "while-away-2");
        assertEquals("subscription kept while the subscriber is away", 1, durableSubscriptionCount());

        try (var conn = factory.createConnection()) {
            conn.setClientID("client-1");
            conn.start();
            final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = session.createDurableSubscriber(session.createTopic(TOPIC_NAME), "sub-1");
            assertEquals(List.of("while-away-1", "while-away-2"), drain(consumer));
        }
    }

    // A shared durable subscription created on a connection with a client id
    // belongs to that client id. One with the same name on a connection
    // without a client id is a separate subscription, so each gets every message.
    @Test(timeout = 60000)
    public void testSharedDurableSubscriptionKeepsItsClientId() throws Exception {
        final var factory = new SharedTopicConnectionFactory(url);

        try (var identified = factory.createConnection(); var unidentified = factory.createConnection()) {
            identified.setClientID("client-1");
            identified.start();
            final var session1 = identified.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer1 = session1.createSharedDurableConsumer(session1.createTopic(TOPIC_NAME), "shared-sub");

            unidentified.start();
            final var session2 = unidentified.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer2 = session2.createSharedDurableConsumer(session2.createTopic(TOPIC_NAME), "shared-sub");

            final var expected = List.of("m-1", "m-2", "m-3", "m-4");
            send(expected.toArray(new String[0]));

            assertEquals("subscription under client-1", expected, drain(consumer1));
            assertEquals("subscription without a client id", expected, drain(consumer2));
        }
    }

    private void send(String... bodies) throws Exception {
        try (var conn = new ActiveMQConnectionFactory(url).createConnection()) {
            final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(session.createTopic(TOPIC_NAME));
            for (var body : bodies) {
                producer.send(session.createTextMessage(body));
            }
        }
    }

    private List<String> drain(MessageConsumer consumer) throws Exception {
        final var bodies = new ArrayList<String>();
        TextMessage msg;
        while ((msg = (TextMessage) consumer.receive(1000)) != null) {
            bodies.add(msg.getText());
        }
        return bodies;
    }

    private int durableSubscriptionCount() {
        final var region = (TopicRegion) ((RegionBroker) broker.getRegionBroker()).getTopicRegion();
        return region.getDurableSubscriptions().size();
    }
}
