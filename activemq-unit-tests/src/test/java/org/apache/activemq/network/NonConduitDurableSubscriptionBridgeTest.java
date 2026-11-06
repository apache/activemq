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
package org.apache.activemq.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.SharedTopicConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;

/**
 * Durable subscribers on broker B, a producer on broker A, and A bridged to B
 * with conduitSubscriptions=false. The durable subscription the bridge keeps
 * on A has to be found again each time the subscribers on B reconnect, so
 * the messages A held while they were away reach them.
 */
public class NonConduitDurableSubscriptionBridgeTest {

    private static final String TOPIC_NAME = "test.nonconduit.durable";
    // The shared and durable flags of a consumer only travel in OpenWire v13.
    // Shared consumers connect in-VM and the bridge link negotiates v13, so
    // both brokers see them as shared.
    private static final String SHARED_CLIENT_URL = "vm://B?create=false";
    private static final String V13 = "?wireFormat.version=13";
    private static final int CYCLES = 3;

    private BrokerService brokerA;
    private BrokerService brokerB;
    private String urlA;
    private String urlB;

    @After
    public void tearDown() throws Exception {
        if (brokerA != null) {
            brokerA.stop();
            brokerA.waitUntilStopped();
        }
        if (brokerB != null) {
            brokerB.stop();
            brokerB.waitUntilStopped();
        }
    }

    @Test(timeout = 120000)
    public void testDurableSubscriberBacklogSurvivesReconnect() throws Exception {
        startBrokers(false);
        final var factory = new ActiveMQConnectionFactory(urlB);
        final var expected = new ArrayList<String>();
        final var received = new ArrayList<String>();

        for (int cycle = 0; cycle < CYCLES; cycle++) {
            try (var conn = factory.createConnection()) {
                final var consumer = durableSubscriber(conn, "client-1", "sub-1");
                waitForActiveSubsOnA(1);
                drain(received, consumer);

                send("online-" + cycle, expected);
                drain(received, consumer);
            }
            waitForActiveSubsOnA(0);
            send("offline-" + cycle, expected);
        }

        try (var conn = factory.createConnection()) {
            final var consumer = durableSubscriber(conn, "client-1", "sub-1");
            waitForActiveSubsOnA(1);
            drain(received, consumer);
        }

        assertEquals("subs on A: " + describeSubsOnA(), expected, received);
        // the bridge keeps the subscriber's own subscription name on A
        assertEquals("subs on A: " + describeSubsOnA(), List.of("sub-1"), subNamesOnA());
    }

    @Test(timeout = 120000)
    public void testSharedDurableBacklogSurvivesReconnect() throws Exception {
        startBrokers(true);
        final var factory = new SharedTopicConnectionFactory(SHARED_CLIENT_URL);
        final var expected = new ArrayList<String>();
        final var received = new ArrayList<String>();
        String nameOnA = null;

        for (int cycle = 0; cycle < CYCLES; cycle++) {
            try (var conn1 = factory.createConnection(); var conn2 = factory.createConnection()) {
                final var consumer1 = sharedDurableConsumer(conn1, "shared-sub");
                final var consumer2 = sharedDurableConsumer(conn2, "shared-sub");
                waitForActiveSubsOnA(1);
                drain(received, consumer1, consumer2);

                send("online-" + cycle, expected);
                drain(received, consumer1, consumer2);

                // one durable subscription on A for the shared subscription,
                // under the same name every time
                final var names = subNamesOnA();
                assertEquals("subs on A: " + describeSubsOnA(), 1, names.size());
                if (nameOnA == null) {
                    nameOnA = names.get(0);
                }
                assertEquals(nameOnA, names.get(0));
            }
            waitForActiveSubsOnA(0);
            send("offline-" + cycle, expected);
        }

        try (var conn = factory.createConnection()) {
            final var consumer = sharedDurableConsumer(conn, "shared-sub");
            waitForActiveSubsOnA(1);
            drain(received, consumer);
        }

        // every message once: none lost while the consumers were away, and
        // none forwarded once per consumer
        assertEquals("subs on A: " + describeSubsOnA(), new TreeSet<>(expected), new TreeSet<>(received));
        assertEquals("subs on A: " + describeSubsOnA(), expected.size(), received.size());
        assertEquals("subs on A: " + describeSubsOnA(), 1, subsOnA().size());
    }

    @Test(timeout = 120000)
    public void testSharedDurableDemandStaysWhileAnyConsumerRemains() throws Exception {
        startBrokers(true);
        final var factory = new SharedTopicConnectionFactory(SHARED_CLIENT_URL);
        final var expected = new ArrayList<String>();
        final var received = new ArrayList<String>();

        try (var conn2 = factory.createConnection()) {
            final MessageConsumer consumer2;
            try (var conn1 = factory.createConnection()) {
                sharedDurableConsumer(conn1, "shared-sub");
                consumer2 = sharedDurableConsumer(conn2, "shared-sub");
                waitForActiveSubsOnA(1);
            }
            // the first consumer has gone; the second still needs the demand
            assertFalse("demand on A must stay while a consumer remains: " + describeSubsOnA(),
                Wait.waitFor(() -> activeSubsOnA() == 0, 1000, 10));

            send("one-remaining", expected);
            drain(received, consumer2);
            assertEquals(expected, received);
        }
        waitForActiveSubsOnA(0);
        send("none-remaining", expected);

        try (var conn = factory.createConnection()) {
            final var consumer = sharedDurableConsumer(conn, "shared-sub");
            waitForActiveSubsOnA(1);
            drain(received, consumer);
        }

        assertEquals("subs on A: " + describeSubsOnA(), expected, received);
        assertEquals("subs on A: " + describeSubsOnA(), 1, subsOnA().size());
    }

    private MessageConsumer durableSubscriber(Connection conn, String clientId, String subName) throws Exception {
        conn.setClientID(clientId);
        conn.start();
        final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createDurableSubscriber(session.createTopic(TOPIC_NAME), subName);
    }

    private MessageConsumer sharedDurableConsumer(Connection conn, String subName) throws Exception {
        conn.start();
        final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createSharedDurableConsumer(session.createTopic(TOPIC_NAME), subName);
    }

    private void send(String body, List<String> expected) throws Exception {
        try (var conn = new ActiveMQConnectionFactory(urlA).createConnection()) {
            final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createProducer(session.createTopic(TOPIC_NAME)).send(session.createTextMessage(body));
        }
        expected.add(body);
    }

    // receives from each consumer in turn until a full pass finds nothing
    private void drain(List<String> into, MessageConsumer... consumers) throws Exception {
        boolean any = true;
        while (any) {
            any = false;
            for (var consumer : consumers) {
                final var msg = consumer.receive(1000);
                if (msg != null) {
                    into.add(((TextMessage) msg).getText());
                    any = true;
                }
            }
        }
    }

    private void waitForActiveSubsOnA(int count) throws Exception {
        assertTrue("active durable subs on A should be " + count + ": " + describeSubsOnA(),
            Wait.waitFor(() -> activeSubsOnA() == count, 10000, 10));
    }

    private Collection<DurableTopicSubscription> subsOnA() {
        final var region = (TopicRegion) ((RegionBroker) brokerA.getRegionBroker()).getTopicRegion();
        final var subs = new ArrayList<DurableTopicSubscription>();
        for (var sub : region.getDurableSubscriptions().values()) {
            if (sub.getConsumerInfo().getDestination().getPhysicalName().equals(TOPIC_NAME)) {
                subs.add(sub);
            }
        }
        return subs;
    }

    private long activeSubsOnA() {
        return subsOnA().stream().filter(DurableTopicSubscription::isActive).count();
    }

    private List<String> subNamesOnA() {
        final var names = new TreeSet<String>();
        for (var sub : subsOnA()) {
            names.add(sub.getSubscriptionKey().getSubscriptionName());
        }
        return new ArrayList<>(names);
    }

    private String describeSubsOnA() {
        final var described = new TreeSet<String>();
        for (var sub : subsOnA()) {
            described.add(sub.getSubscriptionKey().getSubscriptionName()
                + (sub.isActive() ? " (active)" : " (inactive)"));
        }
        return described.toString();
    }

    private void startBrokers(boolean sharedTopics) throws Exception {
        final var options = sharedTopics ? V13 : "";
        brokerB = createBroker("B", sharedTopics);
        final var connectorB = brokerB.addConnector("tcp://localhost:0" + options);
        brokerB.start();
        brokerB.waitUntilStarted();
        urlB = "tcp://localhost:" + connectorB.getConnectUri().getPort();

        brokerA = createBroker("A", sharedTopics);
        final var connectorA = brokerA.addConnector("tcp://localhost:0");
        final var nc = brokerA.addNetworkConnector("static:(" + urlB + options + ")");
        nc.setName("A-to-B");
        nc.setConduitSubscriptions(false);
        brokerA.start();
        brokerA.waitUntilStarted();
        urlA = connectorA.getPublishableConnectString();

        assertNotNull(nc);
        assertTrue("bridge should start", Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 10000, 10));
    }

    private BrokerService createBroker(String name, boolean sharedTopics) throws Exception {
        final var broker = sharedTopics ? new SharedTopicBrokerService() : new BrokerService();
        broker.setBrokerName(name);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        return broker;
    }
}
