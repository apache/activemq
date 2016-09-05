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

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class ForceDurableNetworkBridgeTest extends DynamicNetworkTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(ForceDurableNetworkBridgeTest.class);

    protected String testTopicName2 = "include.nonforced.bar";
    protected String staticTopic = "include.static.bar";
    protected String staticTopic2 = "include.static.nonforced.bar";
    public static enum FLOW {FORWARD, REVERSE};
    private BrokerService broker1;
    private BrokerService broker2;
    private Session session1;
    private final FLOW flow;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {FLOW.FORWARD},
                {FLOW.REVERSE}
        });
    }

    public ForceDurableNetworkBridgeTest(final FLOW flow) {
        this.flow = flow;
    }

    @Before
    public void setUp() throws Exception {
        doSetUp(true, tempFolder.newFolder(), tempFolder.newFolder());
    }

    @After
    public void tearDown() throws Exception {
        doTearDown();
    }

    @Test
    public void testForceDurableSubscriptionStatic() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(staticTopic);

        assertNCDurableSubsCount(broker2, topic, 1);
        assertConsumersCount(broker2, topic, 1);

        //Static so consumers stick around
        assertNCDurableSubsCount(broker2, topic, 1);
        assertConsumersCount(broker2, topic, 1);
    }

    @Test
    public void testConsumerNotForceDurableSubscriptionStatic() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(staticTopic2);

        assertConsumersCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 0);
    }

    @Test
    public void testConsumerNotForceDurableSubscription() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName2);
        MessageConsumer sub1 = session1.createConsumer(topic);

        assertConsumersCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 0);
        sub1.close();

        assertNCDurableSubsCount(broker2, topic, 0);
        assertConsumersCount(broker2, topic, 0);
    }

    @Test
    public void testConsumerNotForceDurableWithAnotherDurable() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName2);
        TopicSubscriber durSub = session1.createDurableSubscriber(topic, subName);
        session1.createConsumer(topic);

        //1 consumer because of conduit
        //1 durable sub
        assertConsumersCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        //Remove the sub
        durSub.close();
        Thread.sleep(1000);
        removeSubscription(broker1, topic, subName);

        //The durable should be gone even though there is a consumer left
        //since we are not forcing durable subs
        assertNCDurableSubsCount(broker2, topic, 0);
        //consumers count ends up being 0 here, even though there is a non-durable consumer left,
        //because the durable sub is destroyed and it is a conduit subscription
        //this is another good reason to want to enable forcing of durables
        assertConsumersCount(broker2, topic, 0);
    }

    @Test
    public void testForceDurableSubscription() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createConsumer(topic);

        assertNCDurableSubsCount(broker2, topic, 1);
        assertConsumersCount(broker2, topic, 1);
        sub1.close();

        assertNCDurableSubsCount(broker2, topic, 0);
        assertConsumersCount(broker2, topic, 0);
    }

    @Test
    public void testForceDurableMultiSubscriptions() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createConsumer(topic);
        MessageConsumer sub2 = session1.createConsumer(topic);
        MessageConsumer sub3 = session1.createConsumer(topic);

        assertNCDurableSubsCount(broker2, topic, 1);
        assertConsumersCount(broker2, topic, 1);
        sub1.close();
        sub2.close();

        assertNCDurableSubsCount(broker2, topic, 1);
        assertConsumersCount(broker2, topic, 1);

        sub3.close();

        assertNCDurableSubsCount(broker2, topic, 0);
        assertConsumersCount(broker2, topic, 0);
    }

    @Test
    public void testForceDurableSubWithDurableCreatedFirst() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        TopicSubscriber durSub = session1.createDurableSubscriber(topic, subName);
        durSub.close();
        assertNCDurableSubsCount(broker2, topic, 1);

        MessageConsumer sub1 = session1.createConsumer(topic);
        Thread.sleep(1000);
        assertNCDurableSubsCount(broker2, topic, 1);
        sub1.close();

        Thread.sleep(1000);
        assertNCDurableSubsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);
        assertNCDurableSubsCount(broker2, topic, 0);
    }

    @Test
    public void testForceDurableSubWithNonDurableCreatedFirst() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        MessageConsumer sub1 = session1.createConsumer(topic);
        assertNCDurableSubsCount(broker2, topic, 1);

        TopicSubscriber durSub = session1.createDurableSubscriber(topic, subName);
        durSub.close();
        Thread.sleep(1000);
        assertNCDurableSubsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);
        Thread.sleep(1000);
        assertConsumersCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        sub1.close();
        assertNCDurableSubsCount(broker2, topic, 0);
    }

    @Test
    public void testDurableSticksAroundOnConsumerClose() throws Exception {
        final ActiveMQTopic topic = new ActiveMQTopic(testTopicName);
        //Create the normal consumer first
        MessageConsumer sub1 = session1.createConsumer(topic);
        assertNCDurableSubsCount(broker2, topic, 1);

        TopicSubscriber durSub = session1.createDurableSubscriber(topic, subName);
        durSub.close();
        sub1.close();
        Thread.sleep(1000);
        //Both consumer and durable are closed but the durable should stick around
        assertConsumersCount(broker2, topic, 1);
        assertNCDurableSubsCount(broker2, topic, 1);

        removeSubscription(broker1, topic, subName);
        assertConsumersCount(broker2, topic, 0);
        assertNCDurableSubsCount(broker2, topic, 0);
    }

    protected void restartBrokers() throws Exception {
        doTearDown();
        doSetUp(false, localBroker.getDataDirectoryFile(), remoteBroker.getDataDirectoryFile());
    }

    protected void doSetUp(boolean deleteAllMessages, File localDataDir,
            File remoteDataDir) throws Exception {
        included = new ActiveMQTopic(testTopicName);
        doSetUpRemoteBroker(deleteAllMessages, remoteDataDir);
        doSetUpLocalBroker(deleteAllMessages, localDataDir);
        //Give time for advisories to propagate
        Thread.sleep(1000);
    }

    protected void doSetUpLocalBroker(boolean deleteAllMessages, File dataDir) throws Exception {
        localBroker = createLocalBroker(dataDir);
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

        Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return localBroker.getNetworkConnectors().get(0).activeBridges().size() == 1;
            }
        }, 10000, 500);

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

    protected BrokerService createLocalBroker(File dataDir) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setMonitorConnectionSplits(true);
        brokerService.setDataDirectoryFile(dataDir);
        brokerService.setBrokerName("localBroker");
        brokerService.addNetworkConnector(configureLocalNetworkConnector());
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setDestinations(new ActiveMQDestination[] {
                new ActiveMQTopic(testTopicName),
                new ActiveMQTopic(testTopicName2),
                new ActiveMQTopic(excludeTopicName)});

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
        connector.setStaticallyIncludedDestinations(Lists.<ActiveMQDestination>newArrayList(
                new ActiveMQTopic(staticTopic + "?forceDurable=true"),
                new ActiveMQTopic(staticTopic2)));
        connector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(
                        new ActiveMQTopic("include.test.>?forceDurable=true"),
                        new ActiveMQTopic(testTopicName2)));
        connector.setExcludedDestinations(
                Lists.<ActiveMQDestination>newArrayList(new ActiveMQTopic(excludeTopicName)));
        return connector;
    }


    protected BrokerService createRemoteBroker(File dataDir) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("remoteBroker");
        brokerService.setUseJmx(false);
        brokerService.setDataDirectoryFile(dataDir);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setDestinations(new ActiveMQDestination[] {
                new ActiveMQTopic(testTopicName),
                new ActiveMQTopic(testTopicName2),
                new ActiveMQTopic(excludeTopicName)});

        return brokerService;
    }

}
