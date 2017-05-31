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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationBroker;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationPlugin;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class DurableSyncNetworkBridgeTest extends DynamicNetworkTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(DurableSyncNetworkBridgeTest.class);

    protected JavaRuntimeConfigurationBroker remoteRuntimeBroker;
    protected String staticIncludeTopics = "include.static.test";
    protected String includedTopics = "include.test.>";
    protected String testTopicName2 = "include.test.bar2";
    private boolean dynamicOnly = false;
    private boolean forceDurable = false;
    private boolean useVirtualDestSubs = false;
    private byte remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION;
    public static enum FLOW {FORWARD, REVERSE}

    private BrokerService broker1;
    private BrokerService broker2;
    private Session session1;
    private Session session2;
    private final FLOW flow;

    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {FLOW.FORWARD},
                {FLOW.REVERSE}
        });
    }

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
    }


    public DurableSyncNetworkBridgeTest(final FLOW flow) {
        this.flow = flow;
    }

    @Before
    public void setUp() throws Exception {
        includedTopics = "include.test.>";
        staticIncludeTopics = "include.static.test";
        dynamicOnly = false;
        forceDurable = false;
        useVirtualDestSubs = false;
        remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION;
        doSetUp(true, true, tempFolder.newFolder(), tempFolder.newFolder());
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }


    @Test
    public void testRemoveSubscriptionPropagate() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

    }

    @Test
    public void testRemoveSubscriptionPropegateAfterRestart() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        restartBrokers(true);
        assertBridgeStarted();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

    }

    @Test
    public void testRemoveSubscriptionWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();
        restartBroker(broker1, false);
        restartBroker(broker2, false);

        //Send some messages to the NC sub and make sure it can still be deleted
        MessageProducer prod = session2.createProducer(topic);
        for (int i = 0; i < 10; i++) {
            prod.send(session2.createTextMessage("test"));
        }

        assertSubscriptionsCount(broker1, topic, 1);
        removeSubscription(broker1, topic, subName);
        assertSubscriptionsCount(broker1, topic, 0);
        doTearDown();

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed
        restartBroker(broker2, true);
        assertNCDurableSubsCount(broker2, topic, 1);
        restartBroker(broker1, true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 0);

    }

    @Test
    public void testRemoveSubscriptionWithBridgeOfflineIncludedChanged() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();

        //change the included topics to make sure we still cleanup non-matching NC durables
        includedTopics = "different.topic";
        restartBroker(broker1, false);
        assertSubscriptionsCount(broker1, topic, 1);
        removeSubscription(broker1, topic, subName);
        assertSubscriptionsCount(broker1, topic, 0);

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed
        restartBroker(broker2, true);
        assertNCDurableSubsCount(broker2, topic, 1);
        restartBroker(broker1, true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 0);

    }

    @Test
    public void testSubscriptionRemovedAfterIncludedChanged() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();

        //change the included topics to make sure we still cleanup non-matching NC durables
        includedTopics = "different.topic";
        restartBroker(broker1, false);
        assertSubscriptionsCount(broker1, topic, 1);

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed because even though the local subscription exists,
        //it no longer matches the included filter
        restartBroker(broker2, true);
        assertNCDurableSubsCount(broker2, topic, 1);
        restartBroker(broker1, true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 0);
        assertSubscriptionsCount(broker1, topic, 1);

    }

    @Test
    public void testSubscriptionRemovedAfterStaticChanged() throws Exception {
        forceDurable = true;
        this.restartBrokers(true);

        final ActiveMQTopic topic = new ActiveMQTopic(this.staticIncludeTopics);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();

        //change the included topics to make sure we still cleanup non-matching NC durables
        staticIncludeTopics = "different.topic";
        this.restartBrokers(false);
        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        //Send some messages to the NC sub and make sure it can still be deleted
        MessageProducer prod = session2.createProducer(topic);
        for (int i = 0; i < 10; i++) {
            prod.send(session2.createTextMessage("test"));
        }

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed because even though the local subscription exists,
        //it no longer matches the included static filter
        restartBroker(broker2, true);
        assertNCDurableSubsCount(broker2, topic, 1);
        restartBroker(broker1, true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 0);
        assertSubscriptionsCount(broker1, topic, 1);
    }

    @Test
    public void testAddAndRemoveSubscriptionWithBridgeOfflineMultiTopics() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        final ActiveMQTopic topic2 = new ActiveMQTopic(testTopicName2);
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 1);
        session1.createDurableSubscriber(topic2, "sub2");
        removeSubscription(broker1, topic, subName);
        assertSubscriptionsCount(broker1, topic, 0);
        assertSubscriptionsCount(broker1, topic2, 1);

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed for topic1 but will stay for topic2

        //before sync, the old NC should exist
        restartBroker(broker2, true);
        assertNCDurableSubsCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic2, 0);

        //After sync, remove old NC and create one for topic 2
        restartBroker(broker1, true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 0);
        assertNCDurableSubsCount(broker2, topic2, 1);
    }

    @Test
    public void testAddSubscriptionsWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        final ActiveMQTopic topic2 = new ActiveMQTopic(testTopicName2);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        //add three subs, should only create 2 NC subs because of conduit
        session1.createDurableSubscriber(topic, subName).close();
        session1.createDurableSubscriber(topic, "sub2").close();
        session1.createDurableSubscriber(topic2, "sub3").close();
        assertSubscriptionsCount(broker1, topic, 2);
        assertSubscriptionsCount(broker1, topic2, 1);

        restartBrokers(true);
        assertBridgeStarted();
        assertNCDurableSubsCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic2, 1);
        assertNCDurableSubsCount(broker2, excludeTopic, 0);

    }

    @Test
    public void testSyncLoadTest() throws Exception {
        String subName = this.subName;
        //Create 1000 subs
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                session1.createDurableSubscriber(new ActiveMQTopic("include.test." + i), subName + i + j).close();
            }
        }
        for (int i = 0; i < 100; i++) {
            assertNCDurableSubsCount(broker2, new ActiveMQTopic("include.test." + i), 1);
        }

        doTearDown();
        restartBroker(broker1, false);

        //with bridge off, remove 100 subs
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                removeSubscription(broker1, new ActiveMQTopic("include.test." + i), subName + i + j);
            }
        }

        //restart test that 900 are resynced and 100 are deleted
        restartBrokers(true);

        for (int i = 0; i < 10; i++) {
            assertNCDurableSubsCount(broker2, new ActiveMQTopic("include.test." + i), 0);
        }

        for (int i = 10; i < 100; i++) {
            assertNCDurableSubsCount(broker2, new ActiveMQTopic("include.test." + i), 1);
        }

        assertBridgeStarted();
    }


    /**
     * Using an older version of openwire should not sync but the network bridge
     * should still start without error
     */
    @Test
    public void testAddSubscriptionsWithBridgeOfflineOpenWire11() throws Exception {
        this.remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION_DURABLE_SYNC - 1;
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        //Since we are using an old version of openwire, the NC should
        //not be added
        restartBrokers(true);
        assertNCDurableSubsCount(broker2, topic, 0);
        assertBridgeStarted();

    }

    @Test
    public void testAddOfflineSubscriptionWithBridgeOfflineDynamicTrue() throws Exception {
        //set dynamicOnly to true
        this.dynamicOnly = true;
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCDurableSubsCount(broker2, topic, 0);
        assertBridgeStarted();
    }

    @Test
    public void testAddOnlineSubscriptionWithBridgeOfflineDynamicTrue() throws Exception {
        //set dynamicOnly to true
        this.dynamicOnly = true;
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCDurableSubsCount(broker2, topic, 0);
        //bring online again
        session1.createDurableSubscriber(topic, subName);
        assertNCDurableSubsCount(broker2, topic, 1);
        assertBridgeStarted();

    }

    @Test
    public void testAddAndRemoveSubscriptionsWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);

        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 1);
        removeSubscription(broker1, topic, subName);
        session1.createDurableSubscriber(topic, "sub2").close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCDurableSubsCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, excludeTopic, 0);
        assertBridgeStarted();

    }

    @Test
    public void testAddOnlineSubscriptionsWithBridgeOffline() throws Exception {
        Assume.assumeTrue(flow == FLOW.FORWARD);

        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        doTearDown();
        restartBrokers(false);

        assertSubscriptionsCount(broker1, topic, 0);

        //create durable that shouldn't be propagated
        session1.createDurableSubscriber(excludeTopic, "sub-exclude");

        //Add 3 online subs
        session1.createDurableSubscriber(topic, subName);
        session1.createDurableSubscriber(topic, "sub2");
        session1.createDurableSubscriber(topic, "sub3");
        assertSubscriptionsCount(broker1, topic, 3);

        //Restart brokers and make sure we don't have duplicate NCs created
        //between the sync command and the online durables that are added over
        //the consumer advisory
        restartBrokers(true);
        assertBridgeStarted();

        //Re-create
        session1.createDurableSubscriber(topic, subName);
        session1.createDurableSubscriber(topic, "sub2");
        session1.createDurableSubscriber(topic, "sub3");
        session1.createDurableSubscriber(excludeTopic, "sub-exclude");

        Thread.sleep(1000);
        assertNCDurableSubsCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, excludeTopic, 0);

    }

    //Test that durable sync works with more than one bridge
    @Test
    public void testAddOnlineSubscriptionsTwoBridges() throws Exception {

        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);
        final ActiveMQTopic topic2 = new ActiveMQTopic("include.new.topic");

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);

        //create durable that shouldn't be propagated
        session1.createDurableSubscriber(excludeTopic, "sub-exclude");

        //Add 3 online subs
        session1.createDurableSubscriber(topic, subName);
        session1.createDurableSubscriber(topic, "sub2");
        session1.createDurableSubscriber(topic, "sub3");
        //Add sub on second topic/bridge
        session1.createDurableSubscriber(topic2, "secondTopicSubName");
        assertSubscriptionsCount(broker1, topic, 3);
        assertSubscriptionsCount(broker1, topic2, 1);

        //Add the second network connector
        NetworkConnector secondConnector = configureLocalNetworkConnector();
        secondConnector.setName("networkConnector2");
        secondConnector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(
                        new ActiveMQTopic("include.new.topic?forceDurable=" + forceDurable)));
        localBroker.addNetworkConnector(secondConnector);
        secondConnector.start();

        //Make sure both bridges are connected
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1 &&
                        localBroker.getNetworkConnectors().get(1).activeBridges().size() == 1;
            }
        }, 10000, 500));

        //Make sure NC durables exist for both bridges
        assertNCDurableSubsCount(broker2, topic2, 1);
        assertNCDurableSubsCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, excludeTopic, 0);

        //Make sure message can reach remote broker
        MessageProducer producer = session2.createProducer(topic2);
        producer.send(session2.createTextMessage("test"));
        waitForDispatchFromLocalBroker(broker2.getDestination(topic2).getDestinationStatistics(), 1);
        assertLocalBrokerStatistics(broker2.getDestination(topic2).getDestinationStatistics(), 1);
    }

    @Test(timeout = 60 * 1000)
    public void testVirtualDestSubForceDurableSync() throws Exception {
        Assume.assumeTrue(flow == FLOW.FORWARD);
        forceDurable = true;
        useVirtualDestSubs = true;
        this.restartBrokers(true);

        //configure a virtual destination that forwards messages from topic testQueueName
        CompositeTopic compositeTopic = createCompositeTopic(testTopicName,
                new ActiveMQQueue("include.test.bar.bridge"));
        remoteRuntimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();
        final DestinationStatistics remoteDestStatistics = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        //Make sure that the NC durable is created because of the compositeTopic
        waitForConsumerCount(destinationStatistics, 1);
        assertNCDurableSubsCount(localBroker, included, 1);

        //Send message and make sure it is dispatched across the bridge
        includedProducer.send(test);
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);
        assertEquals("remote dest messages", 1, remoteDestStatistics.getMessages().getCount());

        //Stop the remote broker so the bridge stops and then send 500 messages so
        //the messages build up on the NC durable
        this.stopRemoteBroker();
        for (int i = 0; i < 500; i++) {
            includedProducer.send(test);
        }
        this.stopLocalBroker();

        //Restart the brokers
        this.restartRemoteBroker();
        remoteRuntimeBroker.setVirtualDestinations(new VirtualDestination[] {compositeTopic}, true);
        this.restartLocalBroker(true);

        //We now need to verify that 501 messages made it to the queue on the remote side
        //which means that the NC durable was not deleted and recreated during the sync
        final DestinationStatistics remoteDestStatistics2 = remoteBroker.getDestination(
                new ActiveMQQueue("include.test.bar.bridge")).getDestinationStatistics();

        assertTrue(Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return remoteDestStatistics2.getMessages().getCount() == 501;
            }
        }));

    }

    @Test(timeout = 60 * 1000)
    public void testForceDurableTopicSubSync() throws Exception {
        Assume.assumeTrue(flow == FLOW.FORWARD);
        forceDurable = true;
        this.restartBrokers(true);

        //configure a virtual destination that forwards messages from topic testQueueName
        remoteSession.createConsumer(included);

        MessageProducer includedProducer = localSession.createProducer(included);
        Message test = localSession.createTextMessage("test");

        final DestinationStatistics destinationStatistics = localBroker.getDestination(included).getDestinationStatistics();

        //Make sure that the NC durable is created because of the compositeTopic
        waitForConsumerCount(destinationStatistics, 1);
        assertNCDurableSubsCount(localBroker, included, 1);

        //Send message and make sure it is dispatched across the bridge
        includedProducer.send(test);
        waitForDispatchFromLocalBroker(destinationStatistics, 1);
        assertLocalBrokerStatistics(destinationStatistics, 1);

        //Stop the network connector and send messages to the local broker so they build
        //up on the durable
        this.localBroker.getNetworkConnectorByName("networkConnector").stop();

        for (int i = 0; i < 500; i++) {
            includedProducer.send(test);
        }

        //restart the local broker and bridge
        this.stopLocalBroker();
        this.restartLocalBroker(true);

        //We now need to verify that the 500 messages on the NC durable are dispatched
        //on bridge sync which shows that the durable wasn't destroyed/recreated
        final DestinationStatistics destinationStatistics2 =
                localBroker.getDestination(included).getDestinationStatistics();
        waitForDispatchFromLocalBroker(destinationStatistics2, 500);
        assertLocalBrokerStatistics(destinationStatistics2, 500);

    }

    protected CompositeTopic createCompositeTopic(String name, ActiveMQDestination...forwardTo) {
        CompositeTopic compositeTopic = new CompositeTopic();
        compositeTopic.setName(name);
        compositeTopic.setForwardOnly(true);
        compositeTopic.setForwardTo( Lists.newArrayList(forwardTo));

        return compositeTopic;
    }

    protected void restartBroker(BrokerService broker, boolean startNetworkConnector) throws Exception {
        if (broker.getBrokerName().equals("localBroker")) {
            restartLocalBroker(startNetworkConnector);
        } else  {
            restartRemoteBroker();
        }
    }

    protected void restartBrokers(boolean startNetworkConnector) throws Exception {
        doTearDown();
        doSetUp(false, startNetworkConnector, localBroker.getDataDirectoryFile(),
                remoteBroker.getDataDirectoryFile());
    }

    protected void doSetUp(boolean deleteAllMessages, boolean startNetworkConnector, File localDataDir,
            File remoteDataDir) throws Exception {
        included = new ActiveMQTopic(testTopicName);
        doSetUpRemoteBroker(deleteAllMessages, remoteDataDir, 0);
        doSetUpLocalBroker(deleteAllMessages, startNetworkConnector, localDataDir);
        //Give time for advisories to propagate
        Thread.sleep(1000);
    }

    protected void restartLocalBroker(boolean startNetworkConnector) throws Exception {
        stopLocalBroker();
        doSetUpLocalBroker(false, startNetworkConnector, localBroker.getDataDirectoryFile());
    }

    protected void restartRemoteBroker() throws Exception {
        int port = 0;
        if (remoteBroker != null) {
            List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
            port = transportConnectors.get(0).getConnectUri().getPort();
        }
        stopRemoteBroker();
        doSetUpRemoteBroker(false, remoteBroker.getDataDirectoryFile(), port);
    }

    protected void doSetUpLocalBroker(boolean deleteAllMessages, boolean startNetworkConnector,
            File dataDir) throws Exception {
        localBroker = createLocalBroker(dataDir, startNetworkConnector);
        localBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        localBroker.start();
        localBroker.waitUntilStarted();
        URI localURI = localBroker.getVmConnectorURI();
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        fac.setAlwaysSyncSend(true);
        fac.setDispatchAsync(false);
        localConnection = fac.createConnection();
        localConnection.setClientID("clientId");
        localConnection.start();

        if (startNetworkConnector) {
            Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1;
                }
            }, 5000, 500);
        }
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (flow.equals(FLOW.FORWARD)) {
            broker1 = localBroker;
            session1 = localSession;
        } else {
            broker2 = localBroker;
            session2 = localSession;
        }
    }

    protected void doSetUpRemoteBroker(boolean deleteAllMessages, File dataDir, int port) throws Exception {
        remoteBroker = createRemoteBroker(dataDir, port);
        remoteBroker.setDeleteAllMessagesOnStartup(deleteAllMessages);
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
            remoteRuntimeBroker = (JavaRuntimeConfigurationBroker)
                    remoteBroker.getBroker().getAdaptor(JavaRuntimeConfigurationBroker.class);
        } else {
            broker1 = remoteBroker;
            session1 = remoteSession;
        }
    }


    protected BrokerService createLocalBroker(File dataDir, boolean startNetworkConnector) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setMonitorConnectionSplits(true);
        brokerService.setBrokerName("localBroker");
        brokerService.setDataDirectoryFile(dataDir);
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(dataDir);
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
        brokerService.setPersistenceAdapter(adapter);
        brokerService.setUseVirtualDestSubs(useVirtualDestSubs);
        brokerService.setUseVirtualDestSubsOnCreation(useVirtualDestSubs);

        if (startNetworkConnector) {
            brokerService.addNetworkConnector(configureLocalNetworkConnector());
        }

        //Use auto+nio+ssl to test out the transport works with bridging
        brokerService.addConnector("auto+nio+ssl://localhost:0");

        return brokerService;
    }

    protected NetworkConnector configureLocalNetworkConnector() throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI = transportConnectors.get(0).getConnectUri();
        String uri = "static:(" + remoteURI + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("networkConnector");
        connector.setDynamicOnly(dynamicOnly);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setDuplex(true);
        connector.setStaticBridge(false);
        connector.setSyncDurableSubs(true);
        connector.setUseVirtualDestSubs(useVirtualDestSubs);
        connector.setStaticallyIncludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic(staticIncludeTopics + "?forceDurable=" + forceDurable)));
        connector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic(includedTopics + "?forceDurable=" + forceDurable)));
        connector.setExcludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic(excludeTopicName)));
        return connector;
    }

    protected AdvisoryBroker remoteAdvisoryBroker;

    protected BrokerService createRemoteBroker(File dataDir, int port) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(dataDir);
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(dataDir);
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
        brokerService.setPersistenceAdapter(adapter);
        brokerService.setUseVirtualDestSubs(useVirtualDestSubs);
        brokerService.setUseVirtualDestSubsOnCreation(useVirtualDestSubs);

        if (useVirtualDestSubs) {
            brokerService.setPlugins(new BrokerPlugin[] {new JavaRuntimeConfigurationPlugin()});
        }

        remoteAdvisoryBroker = (AdvisoryBroker) brokerService.getBroker().getAdaptor(AdvisoryBroker.class);

        //Need a larger cache size in order to handle all of the durables
        //Use auto+nio+ssl to test out the transport works with bridging
        brokerService.addConnector("auto+nio+ssl://localhost:" + port + "?wireFormat.cacheSize=2048&wireFormat.version=" + remoteBrokerWireFormatVersion);

        return brokerService;
    }

}
