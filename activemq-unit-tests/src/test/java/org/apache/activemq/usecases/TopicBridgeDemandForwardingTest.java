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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.broker.SharedTopicBrokerService;
import org.apache.activemq.SharedTopicConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parameterized test validating that DemandForwardingBridge correctly
 * propagates topic consumer demand across a two-broker network for all
 * combinations of subscription type.
 *
 * <p>Broker A (local/producer) bridges to broker B (remote/consumer).
 * Consumers created on B generate demand that the bridge forwards to A.
 * Messages published on A are forwarded to B's consumers.
 *
 * <p>With {@code conduitSubscriptions=true}, multiple remote consumers
 * are condensed into a single network subscription on the local broker.
 * With {@code conduitSubscriptions=false}, each remote consumer gets its
 * own demand subscription regardless of shared type — each consumer has
 * its own prefetch and should be treated individually.
 */
@RunWith(Parameterized.class)
@Category(ParallelTest.class)
public class TopicBridgeDemandForwardingTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicBridgeDemandForwardingTest.class);

    private static final AtomicInteger BROKER_SEQ = new AtomicInteger();
    private static final String TOPIC_NAME = "test.bridge.demand";
    private static final int MESSAGE_COUNT = 10;

    @Parameters(name = "durable={0}, shared={1}, consumers={2}, conduit={3}, expectedDemand={4}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (boolean durable : new boolean[]{true, false}) {
            for (boolean shared : new boolean[]{true, false}) {
                for (int consumers : new int[]{1, 2}) {
                    for (boolean conduit : new boolean[]{true, false}) {
                        // Conduit merges all remote consumers into one demand sub.
                        // Without conduit, each consumer should get its own demand
                        // regardless of shared type — each has its own prefetch.
                        int expected = conduit ? 1 : consumers;
                        params.add(new Object[]{durable, shared, consumers, conduit, expected});
                    }
                }
            }
        }
        return params;
    }

    private final boolean durable;
    private final boolean shared;
    private final int remoteConsumerCount;
    private final boolean conduit;
    private final int expectedDemand;

    private SharedTopicBrokerService brokerA;
    private SharedTopicBrokerService brokerB;
    private NetworkConnector networkConnector;
    private final List<Connection> connections = new ArrayList<>();

    public TopicBridgeDemandForwardingTest(boolean durable, boolean shared,
            int remoteConsumerCount, boolean conduit, int expectedDemand) {
        this.durable = durable;
        this.shared = shared;
        this.remoteConsumerCount = remoteConsumerCount;
        this.conduit = conduit;
        this.expectedDemand = expectedDemand;
    }

    @Before
    public void setUp() throws Exception {
        int seq = BROKER_SEQ.incrementAndGet();

        brokerB = createBroker("B-" + seq);
        brokerB.start();
        brokerB.waitUntilStarted();

        brokerA = createBroker("A-" + seq);
        networkConnector = bridgeBrokers(brokerA, brokerB);
        brokerA.start();
        brokerA.waitUntilStarted();

        assertTrue("Bridge should activate within 30s",
                Wait.waitFor(() -> !networkConnector.activeBridges().isEmpty(), 30_000));
    }

    @After
    public void tearDown() throws Exception {
        for (Connection c : connections) {
            try { c.close(); } catch (Exception ignored) {}
        }
        connections.clear();
        if (brokerA != null) {
            brokerA.stop();
            brokerA.waitUntilStopped();
        }
        if (brokerB != null) {
            brokerB.stop();
            brokerB.waitUntilStopped();
        }
    }

    @Test
    public void testDemandForwarding() throws Exception {
        ActiveMQTopic dest = new ActiveMQTopic(TOPIC_NAME);
        String brokerBUrl = brokerB.getTransportConnectorByScheme("tcp")
                .getPublishableConnectString() + "?jms.watchTopicAdvisories=false";

        List<MessageConsumer> consumers = createRemoteConsumers(brokerBUrl);

        // Wait for demand to propagate from B to A.
        Destination destOnA = brokerA.getDestination(dest);
        assertNotNull("Topic should exist on broker A", destOnA);
        assertTrue("Demand should propagate within 30s (expected " + expectedDemand + " demand subs)",
                Wait.waitFor(() -> ((Topic) destOnA).getConsumers().size() == expectedDemand,
                        30_000, 200));

        int demandCount = ((Topic) destOnA).getConsumers().size();
        LOG.info("Demand on broker A: {} (expected {}), conduit={}, shared={}, durable={}, remoteConsumers={}",
                demandCount, expectedDemand, conduit, shared, durable, remoteConsumerCount);
        assertEquals("Demand subscription count on broker A", expectedDemand, demandCount);

        // Publish on A, verify messages are forwarded to B.
        String brokerAUrl = brokerA.getTransportConnectorByScheme("tcp")
                .getPublishableConnectString() + "?jms.watchTopicAdvisories=false";
        ActiveMQConnectionFactory producerFactory = new ActiveMQConnectionFactory(brokerAUrl);
        Connection prodConn = producerFactory.createConnection();
        connections.add(prodConn);
        prodConn.start();
        Session prodSession = prodConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = prodSession.createProducer(prodSession.createTopic(TOPIC_NAME));

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            producer.send(prodSession.createTextMessage("msg-" + i));
        }

        // Verify at least 1 consumer receives at least 1 message, proving
        // the bridge forwarded messages from A to B.
        int totalReceived = 0;
        for (MessageConsumer consumer : consumers) {
            while (true) {
                Message msg = consumer.receive(3000);
                if (msg == null) break;
                totalReceived++;
            }
        }

        assertTrue("At least one message should be forwarded from broker A to broker B",
                totalReceived >= MESSAGE_COUNT);

        LOG.info("Messages received on broker B: {} (published {})", totalReceived, MESSAGE_COUNT);
    }

    private List<MessageConsumer> createRemoteConsumers(String brokerBUrl) throws Exception {
        List<MessageConsumer> consumers = new ArrayList<>();

        if (shared) {
            SharedTopicConnectionFactory sharedFactory = new SharedTopicConnectionFactory(brokerBUrl);
            for (int i = 0; i < remoteConsumerCount; i++) {
                Connection conn = sharedFactory.createConnection();
                connections.add(conn);
                conn.start();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                jakarta.jms.Topic topic = session.createTopic(TOPIC_NAME);
                if (durable) {
                    consumers.add(session.createSharedDurableConsumer(topic, "bridgeSub"));
                } else {
                    consumers.add(session.createSharedConsumer(topic, "bridgeSub"));
                }
            }
        } else {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerBUrl);
            for (int i = 0; i < remoteConsumerCount; i++) {
                Connection conn = factory.createConnection();
                connections.add(conn);
                if (durable) {
                    conn.setClientID("remote-client-" + i);
                }
                conn.start();
                Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                jakarta.jms.Topic topic = session.createTopic(TOPIC_NAME);
                if (durable) {
                    consumers.add(session.createDurableSubscriber(topic, "sub-" + i));
                } else {
                    consumers.add(session.createConsumer(topic));
                }
            }
        }

        return consumers;
    }

    private SharedTopicBrokerService createBroker(String name) throws Exception {
        SharedTopicBrokerService broker = new SharedTopicBrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(name);
        broker.addConnector("tcp://0.0.0.0:0");
        return broker;
    }

    private NetworkConnector bridgeBrokers(SharedTopicBrokerService local,
            SharedTopicBrokerService remote) throws Exception {
        String uri = "static:(" + remote.getTransportConnectorByScheme("tcp")
                .getPublishableConnectString() + ")";
        DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName(local.getBrokerName() + "-to-" + remote.getBrokerName());
        connector.setConduitSubscriptions(conduit);
        connector.setDynamicOnly(true);
        local.addNetworkConnector(connector);
        return connector;
    }
}
