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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class DurableSyncNetworkBridgeTest {
    protected static final Logger LOG = LoggerFactory.getLogger(DurableSyncNetworkBridgeTest.class);

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String includeTopicName = "include.test.bar";
    protected String includeTopicName2 = "include.test.bar2";
    protected String excludeTopicName = "exclude.test.bar";
    protected String clientId = "clientId";
    private boolean dynamicOnly = false;
    private byte remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION;
    public static enum FLOW {FORWARD, REVERSE};

    private BrokerService broker1;
    private BrokerService broker2;
    private Session session1;
    private final FLOW flow;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {FLOW.FORWARD},
                {FLOW.REVERSE}
        });
    }


    public DurableSyncNetworkBridgeTest(final FLOW flow) {
        this.flow = flow;
    }

    @Before
    public void setUp() throws Exception {
        dynamicOnly = false;
        remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION;
        doSetUp(true, true, tempFolder.newFolder(), tempFolder.newFolder());
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    protected void doTearDown() throws Exception {
        stopLocalBroker();
        stopRemoteBroker();
    }

    protected void stopLocalBroker() throws Exception {
        if (localConnection != null) {
            localConnection.close();
        }
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }
    }

    protected void stopRemoteBroker() throws Exception {
        if (remoteConnection != null) {
            remoteConnection.close();
        }
        if (remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }

    @Test
    public void testRemoveSubscriptionPropagate() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

    }

    @Test
    public void testRemoveSubscriptionPropegateAfterRestart() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

        restartBrokers(true);

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

    }

    @Test
    public void testRemoveSubscriptionWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

        doTearDown();
        restartBroker(broker1, false);
        assertSubscriptionsCount(broker1, topic, 1);
        removeSubscription(broker1, topic, subName);
        assertSubscriptionsCount(broker1, topic, 0);

        //Test that on successful reconnection of the bridge that
        //the NC sub will be removed
        restartBroker(broker2, true);
        assertNCSubscriptionsCount(broker2, topic, 1);
        restartBroker(broker1, true);
        assertNCSubscriptionsCount(broker2, topic, 0);

    }

    @Test
    public void testAddAndRemoveSubscriptionWithBridgeOfflineMultiTopics() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final ActiveMQTopic topic2 = new ActiveMQTopic(includeTopicName2);
        final String subName = "sub1";
        MessageConsumer sub1 = session1.createDurableSubscriber(topic, subName);
        sub1.close();

        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

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
        assertNCSubscriptionsCount(broker2, topic, 1);
        assertNCSubscriptionsCount(broker2, topic2, 0);

        //After sync, remove old NC and create one for topic 2
        restartBroker(broker1, true);
        assertNCSubscriptionsCount(broker2, topic, 0);
        assertNCSubscriptionsCount(broker2, topic2, 1);

    }

    @Test
    public void testAddSubscriptionsWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final ActiveMQTopic topic2 = new ActiveMQTopic(includeTopicName2);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);
        final String subName = "sub1";

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

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
        assertNCSubscriptionsCount(broker2, topic, 1);
        assertNCSubscriptionsCount(broker2, topic2, 1);
        assertNCSubscriptionsCount(broker2, excludeTopic, 0);

    }

    /**
     * Using an older version of openwire should not sync but the network bridge
     * should still start without error
     */
    @Test
    public void testAddSubscriptionsWithBridgeOfflineOpenWire11() throws Exception {
        this.remoteBrokerWireFormatVersion = CommandTypes.PROTOCOL_VERSION_DURABLE_SYNC - 1;
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        //Since we are using an old version of openwire, the NC should
        //not be added
        restartBrokers(true);
        assertNCSubscriptionsCount(broker2, topic, 0);

    }

    @Test
    public void testAddOfflineSubscriptionWithBridgeOfflineDynamicTrue() throws Exception {
        //set dynamicOnly to true
        this.dynamicOnly = true;
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCSubscriptionsCount(broker2, topic, 0);
    }

    @Test
    public void testAddOnlineSubscriptionWithBridgeOfflineDynamicTrue() throws Exception {
        //set dynamicOnly to true
        this.dynamicOnly = true;
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final String subName = "sub1";

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 0);
        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCSubscriptionsCount(broker2, topic, 0);
        //bring online again
        session1.createDurableSubscriber(topic, subName);
        assertNCSubscriptionsCount(broker2, topic, 1);

    }

    @Test
    public void testAddAndRemoveSubscriptionsWithBridgeOffline() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);
        final String subName = "sub1";

        session1.createDurableSubscriber(topic, subName).close();
        assertSubscriptionsCount(broker1, topic, 1);
        assertNCSubscriptionsCount(broker2, topic, 1);

        doTearDown();
        restartBroker(broker1, false);

        assertSubscriptionsCount(broker1, topic, 1);
        removeSubscription(broker1, topic, subName);
        session1.createDurableSubscriber(topic, "sub2").close();
        assertSubscriptionsCount(broker1, topic, 1);

        restartBrokers(true);
        assertNCSubscriptionsCount(broker2, topic, 1);
        assertNCSubscriptionsCount(broker2, excludeTopic, 0);

    }

    @Test
    public void testAddOnlineSubscriptionsWithBridgeOffline() throws Exception {
        Assume.assumeTrue(flow == FLOW.FORWARD);

        final ActiveMQTopic topic = new ActiveMQTopic(includeTopicName);
        final ActiveMQTopic excludeTopic = new ActiveMQTopic(excludeTopicName);
        final String subName = "sub1";

        assertSubscriptionsCount(broker1, topic, 0);
        assertNCSubscriptionsCount(broker2, topic, 0);

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

        //Re-create
        session1.createDurableSubscriber(topic, subName);
        session1.createDurableSubscriber(topic, "sub2");
        session1.createDurableSubscriber(topic, "sub3");
        session1.createDurableSubscriber(excludeTopic, "sub-exclude");

        Thread.sleep(1000);
        assertNCSubscriptionsCount(broker2, topic, 1);
        assertNCSubscriptionsCount(broker2, excludeTopic, 0);

    }

    protected void removeSubscription(final BrokerService brokerService, final ActiveMQTopic topic,
            final String subName) throws Exception {
        final RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subName);

        final ConnectionContext context = new ConnectionContext();
        context.setBroker(brokerService.getBroker());
        context.setClientId(clientId);

        brokerService.getBroker().removeSubscription(context, info);
    }

    protected void assertSubscriptionsCount(final BrokerService brokerService,
            final ActiveMQTopic dest, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getSubscriptions(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected void assertNCSubscriptionsCount(final BrokerService brokerService,
            final ActiveMQTopic dest, final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getNCSubscriptions(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected List<DurableTopicSubscription> getSubscriptions(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        List<DurableTopicSubscription> subs = new ArrayList<>();
        Topic destination = (Topic) brokerService.getDestination(dest);
        for (SubscriptionKey key : destination.getDurableTopicSubs().keySet()) {
            if (!key.getSubscriptionName().startsWith(DemandForwardingBridge.DURABLE_SUB_PREFIX)) {
                DurableTopicSubscription sub = destination.getDurableTopicSubs().get(key);
                if (sub != null) {
                    subs.add(sub);
                }
            }
        }
        return subs;
    }

    protected List<DurableTopicSubscription> getNCSubscriptions(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        List<DurableTopicSubscription> subs = new ArrayList<>();
        Topic destination = (Topic) brokerService.getDestination(dest);
        for (SubscriptionKey key : destination.getDurableTopicSubs().keySet()) {
            if (key.getSubscriptionName().startsWith(DemandForwardingBridge.DURABLE_SUB_PREFIX)) {
                DurableTopicSubscription sub = destination.getDurableTopicSubs().get(key);
                if (sub != null) {
                    subs.add(sub);
                }
            }
        }
        return subs;
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
        included = new ActiveMQTopic(includeTopicName);
        doSetUpRemoteBroker(deleteAllMessages, remoteDataDir);
        doSetUpLocalBroker(deleteAllMessages, startNetworkConnector, localDataDir);
        //Give time for advisories to propagate
        Thread.sleep(1000);
    }

    protected void restartLocalBroker(boolean startNetworkConnector) throws Exception {
        stopLocalBroker();
        doSetUpLocalBroker(false, startNetworkConnector, localBroker.getDataDirectoryFile());
    }

    protected void restartRemoteBroker() throws Exception {
        stopRemoteBroker();
        doSetUpRemoteBroker(false, remoteBroker.getDataDirectoryFile());
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

        if (startNetworkConnector) {        // brokerService.setPlugins(new BrokerPlugin[] {new
            // JavaRuntimeConfigurationPlugin()});
            // brokerService.setUseVirtualDestSubs(true);
            // brokerService.setUseVirtualDestSubsOnCreation(isUsevirtualDestinationSubscriptionsOnCreation);
            Wait.waitFor(new Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1;
                }
            }, 10000, 500);
        }
        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        if (flow.equals(FLOW.FORWARD)) {
            broker1 = localBroker;
            session1 = localSession;
        } else {
            broker2 = localBroker;
        }
    }

    protected void doSetUpRemoteBroker(boolean deleteAllMessages, File dataDir) throws Exception {
        remoteBroker = createRemoteBroker(dataDir);
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
        } else {
            broker1 = remoteBroker;
            session1 = remoteSession;
        }
    }

    protected BrokerService createLocalBroker(File dataDir, boolean startNetworkConnector) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setMonitorConnectionSplits(true);
        brokerService.setDataDirectoryFile(dataDir);
        brokerService.setBrokerName("localBroker");

        if (startNetworkConnector) {
            brokerService.addNetworkConnector(configureLocalNetworkConnector());
        }

        brokerService.addConnector("tcp://localhost:0");

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
        connector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic("include.test.>")));
        connector.setExcludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic(excludeTopicName)));
        return connector;
    }

    protected AdvisoryBroker remoteAdvisoryBroker;

    protected BrokerService createRemoteBroker(File dataDir) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(dataDir);

        remoteAdvisoryBroker = (AdvisoryBroker) brokerService.getBroker().getAdaptor(AdvisoryBroker.class);

        brokerService.addConnector("tcp://localhost:0?wireFormat.version=" + remoteBrokerWireFormatVersion);

        return brokerService;
    }

}
