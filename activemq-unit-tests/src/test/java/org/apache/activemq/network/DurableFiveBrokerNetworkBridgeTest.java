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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;

import com.google.common.collect.Lists;

import junit.framework.Test;

/**
 * Test to make sure durable subscriptions propagate properly throughout network bridges
 * and that conduit subscriptions work properly
 */
public class DurableFiveBrokerNetworkBridgeTest extends JmsMultipleBrokersTestSupport {

    private boolean duplex = true;

    @Override
    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName);
        connector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO?forceDurable=true")));
        connector.setDuplex(duplex);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setSyncDurableSubs(true);
        connector.setNetworkTTL(-1);
        return connector;
    }

    public void testDurablePropagationDuplex() throws Exception {
        duplex = true;
        testDurablePropagation();
    }

    public void testDurablePropagationOneWay() throws Exception {
        duplex = false;
        testDurablePropagation();
    }

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    protected void testDurablePropagation() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");
        if (!duplex) {
            bridgeBrokers("BrokerB", "BrokerA");
            bridgeBrokers("BrokerC", "BrokerB");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientB = ses.createDurableSubscriber(dest, "subB");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        sendMessages("BrokerC", dest, 1);
        assertNotNull(clientA.receive(1000));
        assertNotNull(clientB.receive(1000));

        //bring online a consumer on the other side
        Session ses2 = createSession("BrokerC");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientB.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses.unsubscribe("subB");
        ses2.unsubscribe("subC");

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);

    }

    public void testDurablePropagationConsumerAllBrokersDuplex() throws Exception {
        duplex = true;
        testDurablePropagationConsumerAllBrokers();
    }

    public void testDurablePropagationConsumerAllBrokersOneWay() throws Exception {
        duplex = false;
        testDurablePropagationConsumerAllBrokers();
    }

    protected void testDurablePropagationConsumerAllBrokers() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");
        if (!duplex) {
            bridgeBrokers("BrokerB", "BrokerA");
            bridgeBrokers("BrokerC", "BrokerB");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        //bring online a consumer on the other side
        Session ses2 = createSession("BrokerB");
        MessageConsumer clientB = ses2.createDurableSubscriber(dest, "subB");

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        Session ses3 = createSession("BrokerC");
        MessageConsumer clientC = ses3.createDurableSubscriber(dest, "subC");

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);


        clientA.close();
        clientB.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subB");
        ses3.unsubscribe("subC");


        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);

    }

    public void testDurablePropagation5BrokerDuplex() throws Exception {
        duplex = true;
        testDurablePropagation5Broker();
    }

    public void testDurablePropagation5BrokerOneWay() throws Exception {
        duplex = false;
        testDurablePropagation5Broker();
    }

    protected void testDurablePropagation5Broker() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");
        bridgeBrokers("BrokerC", "BrokerD");
        bridgeBrokers("BrokerD", "BrokerE");
        if (!duplex) {
            bridgeBrokers("BrokerB", "BrokerA");
            bridgeBrokers("BrokerC", "BrokerB");
            bridgeBrokers("BrokerD", "BrokerC");
            bridgeBrokers("BrokerE", "BrokerD");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerE").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        sendMessages("BrokerE", dest, 1);
        assertNotNull(clientA.receive(1000));

        //bring online a consumer on the other side
        Session ses2 = createSession("BrokerE");
        MessageConsumer clientE = ses2.createDurableSubscriber(dest, "subE");
        Thread.sleep(1000);

        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerE").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientE.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subE");

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerE").broker, dest, 0);

    }

    public void testDurablePropagationSpokeDuplex() throws Exception {
        duplex = true;
        testDurablePropagationSpoke();
    }

    public void testDurablePropagationSpokeOneWay() throws Exception {
        duplex = false;
        testDurablePropagationSpoke();
    }

    protected void testDurablePropagationSpoke() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");
        bridgeBrokers("BrokerB", "BrokerD");
        if (!duplex) {
            bridgeBrokers("BrokerB", "BrokerA");
            bridgeBrokers("BrokerC", "BrokerB");
            bridgeBrokers("BrokerD", "BrokerB");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        Session ses2 = createSession("BrokerB");
        Session ses3 = createSession("BrokerC");
        Session ses4 = createSession("BrokerD");

        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientAB = ses.createDurableSubscriber(dest, "subAB");
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        MessageConsumer clientD = ses4.createDurableSubscriber(dest, "subD");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        sendMessages("BrokerA", dest, 1);
        assertNotNull(clientD.receive(1000));
        sendMessages("BrokerC", dest, 1);
        assertNotNull(clientD.receive(1000));

        MessageConsumer clientB = ses2.createDurableSubscriber(dest, "subB");
        MessageConsumer clientC = ses3.createDurableSubscriber(dest, "subC");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 3);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientAB.close();
        clientB.close();
        clientC.close();
        clientD.close();

        ses.unsubscribe("subA");
        ses.unsubscribe("subAB");
        ses2.unsubscribe("subB");
        ses3.unsubscribe("subC");
        ses4.unsubscribe("subD");

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerD").broker, dest, 0);
    }

    public void testForceDurablePropagationDuplex() throws Exception {
        duplex = true;
        testForceDurablePropagation();
    }

    public void testForceDurablePropagationOneWay() throws Exception {
        duplex = false;
        testForceDurablePropagation();
    }

    protected void testForceDurablePropagation() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");
        if (!duplex) {
            bridgeBrokers("BrokerB", "BrokerA");
            bridgeBrokers("BrokerC", "BrokerB");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        MessageConsumer clientA = ses.createConsumer(dest);
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        sendMessages("BrokerC", dest, 1);
        assertNotNull(clientA.receive(1000));

        Session ses2 = createSession("BrokerC");
        MessageConsumer clientC = ses2.createConsumer(dest);
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientC.close();

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);
    }

    public void testDurablePropagationSyncDuplex() throws Exception {
        duplex = true;
        testDurablePropagationSync();
    }

    public void testDurablePropagationSyncOneWay() throws Exception {
        duplex = false;
        testDurablePropagationSync();
    }

    protected void testDurablePropagationSync() throws Exception {
        // Setup broker networks
        NetworkConnector nc1 = bridgeBrokers("BrokerA", "BrokerB");
        NetworkConnector nc2 = bridgeBrokers("BrokerB", "BrokerC");

        NetworkConnector nc3 = null;
        NetworkConnector nc4 = null;
        if (!duplex) {
            nc3 = bridgeBrokers("BrokerB", "BrokerA");
            nc4 = bridgeBrokers("BrokerC", "BrokerB");
        }

        startAllBrokers();

        nc1.stop();
        nc2.stop();

        if (!duplex) {
            nc3.stop();
            nc4.stop();
        }

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        Session ses2 = createSession("BrokerC");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientB = ses.createDurableSubscriber(dest, "subB");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);

        nc1.start();
        nc2.start();
        if (!duplex) {
            nc3.start();
            nc4.start();
        }

        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientB.close();
        clientC.close();
    }

    public void testDurablePropagationMultipleBridgesDifferentDestinations() throws Exception {
        duplex = true;

        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

        //Duplicate the bridges with different included destinations - valid use case
        NetworkConnector nc3 = bridgeBrokers("BrokerA", "BrokerB");
        NetworkConnector nc4 = bridgeBrokers("BrokerB", "BrokerC");
        nc3.setName("nc3");
        nc4.setName("nc4");
        nc3.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO2?forceDurable=true")));
        nc4.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO2?forceDurable=true")));

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);
        ActiveMQTopic dest2 = (ActiveMQTopic) createDestination("TEST.FOO2", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        Session ses2 = createSession("BrokerC");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientAa = ses.createDurableSubscriber(dest2, "subAa");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        MessageConsumer clientCc = ses2.createDurableSubscriber(dest2, "subCc");
        Thread.sleep(1000);

        //make sure network durables are online
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest2, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest2, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest2, 1);

        clientA.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subC");

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest2, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest2, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest2, 1);

        clientAa.close();
        clientCc.close();
        ses.unsubscribe("subAa");
        ses2.unsubscribe("subCc");

        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest2, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest2, 0);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest2, 0);
    }

    protected void assertNCDurableSubsCount(final BrokerService brokerService, final ActiveMQTopic dest,
            final int count) throws Exception {
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return count == getNCDurableSubs(brokerService, dest).size();
            }
        }, 10000, 500));
    }

    protected List<DurableTopicSubscription> getNCDurableSubs(final BrokerService brokerService,
            final ActiveMQTopic dest) throws Exception {
        List<DurableTopicSubscription> subs = new ArrayList<>();
        Destination d = brokerService.getDestination(dest);
        org.apache.activemq.broker.region.Topic destination = null;
        if (d instanceof DestinationFilter) {
            destination = ((DestinationFilter) d).getAdaptor(org.apache.activemq.broker.region.Topic.class);
        } else {
            destination = (org.apache.activemq.broker.region.Topic) d;
        }

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

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?persistent=false&useJmx=false");
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC" + options));
        createBroker(new URI("broker:(tcp://localhost:61619)/BrokerD" + options));
        createBroker(new URI("broker:(tcp://localhost:61620)/BrokerE" + options));
    }

    @Override
    protected void configureBroker(BrokerService broker) {
        broker.setBrokerId(broker.getBrokerName());
    }

    protected Session createSession(String broker) throws Exception {
        Connection con = createConnection(broker);
        con.start();
        return con.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static Test suite() {
        return suite(DurableFiveBrokerNetworkBridgeTest.class);
    }
}
