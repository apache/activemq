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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

/**
 * Test class to verify composite consumers correctly create demand
 * with a network of brokers, especially conduit subs
 * See AMQ-9262
 */
@RunWith(Parameterized.class)
@Category(ParallelTest.class)
public class CompositeConsumerNetworkBridgeTest extends DynamicNetworkTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(CompositeConsumerNetworkBridgeTest.class);

    private final static String testTopic1 = "test.composite.topic.1";
    private final static String testTopic2 = "test.composite.topic.2";
    private final static String testQueue1 = "test.composite.queue.1";
    private final static String testQueue2 = "test.composite.queue.2";
    private BrokerService broker1;
    private BrokerService broker2;
    private Session session1;
    private Session session2;
    private final FLOW flow;
    private final static List<ActiveMQTopic> topics = List.of(
        new ActiveMQTopic(testTopic1), new ActiveMQTopic(testTopic2));
    private final static List<ActiveMQQueue> queues = List.of(
        new ActiveMQQueue(testQueue1), new ActiveMQQueue(testQueue2));

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {FLOW.FORWARD},
                {FLOW.REVERSE}
        });
    }

    public CompositeConsumerNetworkBridgeTest(final FLOW flow) {
        this.flow = flow;
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    /**
     * Test a composite durable subscription
     */
    @Test
    public void testCompositeDurableSubscriber() throws Exception {
        setUp();
        final ActiveMQTopic compositeTopic = new ActiveMQTopic(testTopic1 + "," + testTopic2);

        // Create durable sub on composite destination
        // Will create a composite consumer on the local broker but
        // should create 2 consumers on the remote
        TopicSubscriber durSub = session1.createDurableSubscriber(compositeTopic, subName);
        assertConsumersCount(broker1, compositeTopic, 1);

        // The remote broker should create two durable subs instead of 1
        // Should be 1 durable on each of the topics that are part of the composite
        assertConsumersCount(broker2, compositeTopic, 0);
        assertNCDurableSubsCount(broker2, compositeTopic, 0);
        for (ActiveMQTopic topic : topics) {
            assertConsumersCount(broker2, topic, 1);
            assertNCDurableSubsCount(broker2, topic, 1);
        }
        assertCompositeMapCounts(1, 1);

        durSub.close();
        Thread.sleep(1000);
        removeSubscription(broker1, subName);

        //Verify cleanup
        for (ActiveMQTopic topic : topics) {
            assertConsumersCount(broker2, topic, 0);
            assertNCDurableSubsCount(broker2, topic, 0);
        }
        assertCompositeMapCounts(0, 0);
    }

    /**
     * Test a composite durable subscription and normal subscription
     */
    @Test
    public void testCompositeAndNormalDurableSub() throws Exception {
        setUp();
        final ActiveMQTopic compositeTopic = new ActiveMQTopic(testTopic1 + "," + testTopic2);

        // create composite sub and a sub on one of the individual topics
        TopicSubscriber durSub1 = session1.createDurableSubscriber(compositeTopic, subName + "1");
        TopicSubscriber durSub2 = session1.createDurableSubscriber(topics.get(0), subName + "2");

        // Should split the composite and create network subs on individual topics
        for (ActiveMQTopic topic : topics) {
            assertNCDurableSubsCount(broker2, topic, 1);
        }
        assertNCDurableSubsCount(broker2, compositeTopic, 0);
        // Only 1 sub is composite so should just have 1 map entry
        assertCompositeMapCounts(1, 1);

        // Verify message received
        MessageProducer producer = session2.createProducer(topics.get(0));
        producer.send(session2.createTextMessage("test"));
        assertNotNull(durSub1.receive(1000));
        assertNotNull(durSub2.receive(1000));

        durSub1.close();
        durSub2.close();;

        Thread.sleep(1000);
        removeSubscription(broker1, subName + "1");
        removeSubscription(broker1, subName + "2");
        assertCompositeMapCounts(0, 0);
    }


    /**
     * Test two topic subscriptions that match
     */
    @Test
    public void testTopicCompositeSubs() throws Exception {
        setUp();
        final ActiveMQTopic compositeTopic = new ActiveMQTopic(testTopic1 + "," + testTopic2);

        // Create two identical subscriptions on a composite topic
        MessageConsumer sub1 = session1.createConsumer(compositeTopic);
        MessageConsumer sub2 = session1.createConsumer(compositeTopic);
        for (ActiveMQTopic topic : topics) {
            // Verify the local broker has two subs on each individual topic
            assertConsumersCount(broker1, topic, 2);
            // Verify that conduit subscription works correctly now
            // and only 1 sub on each topic. This used to be broken before AMQ-9262
            // and would create two subscriptions even though conduit was true
            assertConsumersCount(broker2, topic, 1);
        }
        assertCompositeMapCounts(2, 0);

        MessageProducer producer = session2.createProducer(topics.get(0));
        producer.send(session2.createTextMessage("test"));

        assertNotNull(sub1.receive(1000));
        assertNotNull(sub2.receive(1000));

        sub1.close();
        sub2.close();

        assertCompositeMapCounts(0, 0);
    }

    /**
     * Test two queue composite subscriptions that match
     */
    @Test
    public void testCompositeQueueSubs() throws Exception {
        setUp();
        final ActiveMQQueue compositeQueue = new ActiveMQQueue(testQueue1 + "," + testQueue2);

        // Create two matching composite queue subs to test conduit subs
        MessageConsumer sub1 = session1.createConsumer(compositeQueue);
        MessageConsumer sub2 = session1.createConsumer(compositeQueue);
        for (ActiveMQDestination queue : queues) {
            assertConsumersCount(broker1, queue, 2);
            // Verify conduit subs now work correctly, this used to be 2
            // which was wrong as conduit is true and is fixed as of AMQ-9262
            assertConsumersCount(broker2, queue, 1);
        }
        assertCompositeMapCounts(2, 0);

        MessageProducer producer = session2.createProducer(queues.get(0));
        producer.send(session2.createTextMessage("test"));

        // Make sure one of the queue receivers gets the message
        assertTrue(sub1.receive(1000) != null
            || sub2.receive(1000) != null);

        sub1.close();
        sub2.close();
        assertCompositeMapCounts(0, 0);
    }

    /**
     * Test a composite queue and normal queue sub
     */
    @Test
    public void testCompositeAndNormalQueueSubs() throws Exception {
        setUp();
        final ActiveMQQueue compositeQueue = new ActiveMQQueue(testQueue1 + "," + testQueue2);

        // Create two matching composite queue subs to test conduit subs
        MessageConsumer sub1 = session1.createConsumer(compositeQueue);
        MessageConsumer sub2 = session1.createConsumer(new ActiveMQQueue(testQueue2));

        assertConsumersCount(broker1, queues.get(0), 1);
        assertConsumersCount(broker1, queues.get(1), 2);
        for (ActiveMQDestination queue : queues) {
            assertConsumersCount(broker2, queue, 1);
        }
        // Only 1 sub is a composite sub
        assertCompositeMapCounts(1, 0);

        MessageProducer producer = session2.createProducer(queues.get(0));
        producer.send(session2.createTextMessage("test"));

        // Make sure message received by sub1
        assertNotNull(sub1.receive(1000));

        sub1.close();
        sub2.close();
        assertCompositeMapCounts(0, 0);
    }

    /**
     * Test two matching durable composite subs
     *
     * This test used to fail with an exception as the bridge would
     * try and create a duplicate network durable with the same client id
     * and sub and would error
     */
    @Test
    public void testCompositeTwoDurableSubscribers() throws Exception {
        setUp();
        final ActiveMQTopic compositeTopic = new ActiveMQTopic(testTopic1 + "," + testTopic2);

        TopicSubscriber durSub1 = session1.createDurableSubscriber(compositeTopic, subName + "1");
        TopicSubscriber durSub2 = session1.createDurableSubscriber(compositeTopic, subName + "2");
        assertConsumersCount(broker1, compositeTopic, 2);

        assertConsumersCount(broker2, compositeTopic, 0);
        assertNCDurableSubsCount(broker2, compositeTopic, 0);
        for (ActiveMQTopic topic : topics) {
            assertConsumersCount(broker2, topic, 1);
            assertNCDurableSubsCount(broker2, topic, 1);
        }
        assertCompositeMapCounts(2, 2);

        durSub1.close();
        Thread.sleep(1000);
        removeSubscription(broker1, subName + "1");

        for (ActiveMQTopic topic : topics) {
            assertConsumersCount(broker2, topic, 1);
            assertNCDurableSubsCount(broker2, topic, 1);
        }

        durSub2.close();
        Thread.sleep(1000);
        removeSubscription(broker1, subName + "2");

        for (ActiveMQTopic topic : topics) {
            assertConsumersCount(broker2, topic, 0);
            assertNCDurableSubsCount(broker2, topic, 0);
        }

        assertCompositeMapCounts(0, 0);
    }


    private void setUp() throws Exception {
        doSetUp(tempFolder.newFolder(), tempFolder.newFolder());
    }

    protected void doSetUp(File localDataDir, File remoteDataDir) throws Exception {
        doSetUpRemoteBroker(remoteDataDir);
        doSetUpLocalBroker(localDataDir);
        //Give time for advisories to propagate
        Thread.sleep(1000);
    }

    protected void doSetUpLocalBroker(File dataDir) throws Exception {
        localBroker = createLocalBroker(dataDir);
        localBroker.setDeleteAllMessagesOnStartup(true);
        localBroker.start();
        localBroker.waitUntilStarted();
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection();
        localConnection.setClientID("clientId");
        localConnection.start();

        Wait.waitFor(() -> localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1, 10000, 500);
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (flow.equals(FLOW.FORWARD)) {
            broker1 = localBroker;
            session1 = localSession;
        } else {
            broker2 = localBroker;
            session2 = localSession;
        }
    }

    protected void doSetUpRemoteBroker(File dataDir) throws Exception {
        remoteBroker = createRemoteBroker(dataDir);
        remoteBroker.setDeleteAllMessagesOnStartup(true);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        URI remoteURI = remoteBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(remoteURI);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("clientId");
        remoteConnection.start();
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (flow.equals(FLOW.FORWARD)) {
            broker2 = remoteBroker;
            session2 = remoteSession;
        } else {
            broker1 = remoteBroker;
            session1 = remoteSession;
        }
    }

    protected BrokerService createLocalBroker(File dataDir) throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setMonitorConnectionSplits(true);
        brokerService.setDataDirectoryFile(dataDir);
        brokerService.setBrokerName("localBroker");
        brokerService.addNetworkConnector(configureLocalNetworkConnector());
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setDestinations(new ActiveMQDestination[] {
                new ActiveMQTopic(testTopic1),
                new ActiveMQTopic(testTopic2),
                new ActiveMQQueue(testQueue1),
                new ActiveMQQueue(testQueue2)});

        return brokerService;
    }

    protected NetworkConnector configureLocalNetworkConnector() throws Exception {

        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI = transportConnectors.get(0).getConnectUri();
        String uri = "static:(" + remoteURI + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("networkConnector");
        connector.setDynamicOnly(false);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setDuplex(true);
        connector.setStaticBridge(false);
        ArrayList<ActiveMQDestination> dynamicIncludedDestinations = new ArrayList<>();
        dynamicIncludedDestinations.addAll(List.of(new ActiveMQTopic("test.composite.topic.>"),
            new ActiveMQQueue("test.composite.queue.>")));
        connector.setDynamicallyIncludedDestinations(dynamicIncludedDestinations);
        return connector;
    }


    protected BrokerService createRemoteBroker(File dataDir) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(dataDir);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setDestinations(new ActiveMQDestination[] {
                new ActiveMQTopic(testTopic1),
                new ActiveMQTopic(testTopic2),
                new ActiveMQQueue(testQueue1),
                new ActiveMQQueue(testQueue2)});

        return brokerService;
    }

    protected void assertCompositeMapCounts(int compositeConsumerIdsSize, int compositeSubSize)
        throws Exception {
        DurableConduitBridge bridge = findBridge();
        assertTrue( Wait.waitFor(() -> compositeConsumerIdsSize == bridge.compositeConsumerIds.size(), 5000, 500));
        assertTrue( Wait.waitFor(() -> compositeSubSize == bridge.compositeSubscriptions.size(), 5000, 500));
    }

    protected DurableConduitBridge findBridge() throws Exception {
        if (flow.equals(FLOW.FORWARD)) {
            return findBridge(remoteBroker);
        } else {
            return findBridge(localBroker);
        }
    }

    protected DurableConduitBridge findBridge(BrokerService broker) throws Exception {
        final NetworkBridge bridge;
        if (broker.getNetworkConnectors().size() > 0) {
            assertTrue(Wait.waitFor(() -> broker.getNetworkConnectors().get(0).activeBridges().size() == 1, 5000, 500));
            bridge = broker.getNetworkConnectors().get(0).activeBridges().iterator().next();
        } else {
            bridge = findDuplexBridge(broker.getTransportConnectorByScheme("tcp"));
        }
        return (DurableConduitBridge)bridge;
    }
}
