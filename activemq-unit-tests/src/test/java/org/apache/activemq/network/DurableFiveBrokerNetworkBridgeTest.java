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
import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import junit.framework.AssertionFailedError;
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

import junit.framework.Test;

/**
 * Test to make sure durable subscriptions propagate properly throughout network bridges
 * and that conduit subscriptions work properly
 */
public class DurableFiveBrokerNetworkBridgeTest extends JmsMultipleBrokersTestSupport {

    private boolean duplex = true;
    private boolean deletePersistentMessagesOnStartup = true;

    @Override
    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName) throws Exception {
        return bridgeBrokers(localBrokerName, remoteBrokerName, false, -1);
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName,
                                             boolean dynamicOnly, int networkTTL) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName);
        ArrayList<ActiveMQDestination> includedDestinations = new ArrayList<>();
        includedDestinations.add(new ActiveMQTopic("TEST.FOO?forceDurable=true"));
        connector.setDynamicallyIncludedDestinations(includedDestinations);
        connector.setDuplex(duplex);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setSyncDurableSubs(true);
        connector.setDynamicOnly(dynamicOnly);
        connector.setNetworkTTL(networkTTL);
        connector.setClientIdToken("|");
        return connector;
    }

    public void testDurablePropagationBrokerRestart() throws Exception {
        duplex = true;

        // Setup broker networks
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        bridgeBrokers("Broker_C_C", "Broker_D_D");
        bridgeBrokers("Broker_D_D", "Broker_E_E");

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Connection conn = brokers.get("Broker_A_A").factory.createConnection();
        conn.setClientID("clientId1");
        conn.start();
        Session ses = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientA2 = ses.createDurableSubscriber(dest, "subA2");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        //bring online a consumer on the other side
        Connection conn2 = brokers.get("Broker_E_E").factory.createConnection();
        conn2.setClientID("clientId2");
        conn2.start();
        Session ses2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer clientE = ses2.createDurableSubscriber(dest, "subE");
        MessageConsumer clientE2 = ses2.createDurableSubscriber(dest, "subE2");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        clientA.close();
        clientA2.close();
        clientE.close();
        clientE2.close();

        this.destroyAllBrokers();
        deletePersistentMessagesOnStartup = false;
        String options = new String("?persistent=true&useJmx=false");
        createBroker(new URI("broker:(tcp://localhost:61616)/Broker_A_A" + options));
        createBroker(new URI("broker:(tcp://localhost:61617)/Broker_B_B" + options));
        createBroker(new URI("broker:(tcp://localhost:61618)/Broker_C_C" + options));
        createBroker(new URI("broker:(tcp://localhost:61619)/Broker_D_D" + options));
        createBroker(new URI("broker:(tcp://localhost:61620)/Broker_E_E" + options));
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        bridgeBrokers("Broker_C_C", "Broker_D_D");
        bridgeBrokers("Broker_D_D", "Broker_E_E");

        startAllBrokers();

        conn = brokers.get("Broker_A_A").factory.createConnection();
        conn.setClientID("clientId1");
        conn.start();
        ses = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn2 = brokers.get("Broker_E_E").factory.createConnection();
        conn2.setClientID("clientId2");
        conn2.start();
        ses2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //bring one online and leave others offline to test mixed
        clientE = ses2.createDurableSubscriber(dest, "subE");
        clientE.close();

        ses.unsubscribe("subA");
        ses.unsubscribe("subA2");
        ses2.unsubscribe("subE");
        ses2.unsubscribe("subE2");

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 0);
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
     * Broker_A_A -> Broker_B_B -> Broker_C_C
     */
    protected void testDurablePropagation() throws Exception {
        // Setup broker networks
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        if (!duplex) {
            bridgeBrokers("Broker_B_B", "Broker_A_A");
            bridgeBrokers("Broker_C_C", "Broker_B_B");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientB = ses.createDurableSubscriber(dest, "subB");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        sendMessages("Broker_C_C", dest, 1);
        assertNotNull(clientA.receive(1000));
        assertNotNull(clientB.receive(1000));

        //bring online a consumer on the other side
        Session ses2 = createSession("Broker_C_C");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        clientA.close();
        clientB.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses.unsubscribe("subB");
        ses2.unsubscribe("subC");

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);

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
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        if (!duplex) {
            bridgeBrokers("Broker_B_B", "Broker_A_A");
            bridgeBrokers("Broker_C_C", "Broker_B_B");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        //bring online a consumer on the other side
        Session ses2 = createSession("Broker_B_B");
        MessageConsumer clientB = ses2.createDurableSubscriber(dest, "subB");

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        Session ses3 = createSession("Broker_C_C");
        MessageConsumer clientC = ses3.createDurableSubscriber(dest, "subC");

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);


        clientA.close();
        clientB.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subB");
        ses3.unsubscribe("subC");


        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);

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
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        bridgeBrokers("Broker_C_C", "Broker_D_D");
        bridgeBrokers("Broker_D_D", "Broker_E_E");
        if (!duplex) {
            bridgeBrokers("Broker_B_B", "Broker_A_A");
            bridgeBrokers("Broker_C_C", "Broker_B_B");
            bridgeBrokers("Broker_D_D", "Broker_C_C");
            bridgeBrokers("Broker_E_E", "Broker_D_D");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        sendMessages("Broker_E_E", dest, 1);
        assertNotNull(clientA.receive(1000));

        //bring online a consumer on the other side
        Session ses2 = createSession("Broker_E_E");
        MessageConsumer clientE = ses2.createDurableSubscriber(dest, "subE");
        Thread.sleep(1000);

        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        clientA.close();
        clientE.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subE");

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 0);

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
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        bridgeBrokers("Broker_B_B", "Broker_D_D");
        if (!duplex) {
            bridgeBrokers("Broker_B_B", "Broker_A_A");
            bridgeBrokers("Broker_C_C", "Broker_B_B");
            bridgeBrokers("Broker_D_D", "Broker_B_B");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        Session ses2 = createSession("Broker_B_B");
        Session ses3 = createSession("Broker_C_C");
        Session ses4 = createSession("Broker_D_D");

        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientAB = ses.createDurableSubscriber(dest, "subAB");
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        MessageConsumer clientD = ses4.createDurableSubscriber(dest, "subD");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        sendMessages("Broker_A_A", dest, 1);
        assertNotNull(clientD.receive(1000));
        sendMessages("Broker_C_C", dest, 1);
        assertNotNull(clientD.receive(1000));

        MessageConsumer clientB = ses2.createDurableSubscriber(dest, "subB");
        MessageConsumer clientC = ses3.createDurableSubscriber(dest, "subC");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 3);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

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

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 0);
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
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");
        if (!duplex) {
            bridgeBrokers("Broker_B_B", "Broker_A_A");
            bridgeBrokers("Broker_C_C", "Broker_B_B");
        }

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        MessageConsumer clientA = ses.createConsumer(dest);
        Thread.sleep(1000);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);

        sendMessages("Broker_C_C", dest, 1);
        assertNotNull(clientA.receive(1000));

        Session ses2 = createSession("Broker_C_C");
        MessageConsumer clientC = ses2.createConsumer(dest);
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        clientA.close();
        clientC.close();

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
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
        NetworkConnector nc1 = bridgeBrokers("Broker_A_A", "Broker_B_B");
        NetworkConnector nc2 = bridgeBrokers("Broker_B_B", "Broker_C_C");

        NetworkConnector nc3 = null;
        NetworkConnector nc4 = null;
        if (!duplex) {
            nc3 = bridgeBrokers("Broker_B_B", "Broker_A_A");
            nc4 = bridgeBrokers("Broker_C_C", "Broker_B_B");
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
        Session ses = createSession("Broker_A_A");
        Session ses2 = createSession("Broker_C_C");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientB = ses.createDurableSubscriber(dest, "subB");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);

        nc1.start();
        nc2.start();
        if (!duplex) {
            nc3.start();
            nc4.start();
        }

        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);

        clientA.close();
        clientB.close();
        clientC.close();
    }

    public void testDurablePropagationMultipleBridgesDifferentDestinations() throws Exception {
        duplex = true;

        // Setup broker networks
        bridgeBrokers("Broker_A_A", "Broker_B_B");
        bridgeBrokers("Broker_B_B", "Broker_C_C");

        //Duplicate the bridges with different included destinations - valid use case
        NetworkConnector nc3 = bridgeBrokers("Broker_A_A", "Broker_B_B");
        NetworkConnector nc4 = bridgeBrokers("Broker_B_B", "Broker_C_C");
        nc3.setName("nc_3_3");
        nc4.setName("nc_4_4");
        ArrayList<ActiveMQDestination> includedDestinations = new ArrayList<>();
        includedDestinations.add(new ActiveMQTopic("TEST.FOO2?forceDurable=true"));
        nc3.setDynamicallyIncludedDestinations(includedDestinations);
        nc4.setDynamicallyIncludedDestinations(includedDestinations);

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);
        ActiveMQTopic dest2 = (ActiveMQTopic) createDestination("TEST.FOO2", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        Session ses2 = createSession("Broker_C_C");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientAa = ses.createDurableSubscriber(dest2, "subAa");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");
        MessageConsumer clientCc = ses2.createDurableSubscriber(dest2, "subCc");
        Thread.sleep(1000);

        //make sure network durables are online
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest2, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest2, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest2, 1);

        clientA.close();
        clientC.close();
        ses.unsubscribe("subA");
        ses2.unsubscribe("subC");

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest2, 2);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest2, 1);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest2, 1);

        clientAa.close();
        clientCc.close();
        ses.unsubscribe("subAa");
        ses2.unsubscribe("subCc");

        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest2, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest2, 0);
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest2, 0);
    }

    /*
     * The following tests should work for correct sync/propagation of durables even if
     * TTL is missing on broker restart. This is for TTL: 0, -1, 1, or 4 (same number
     * of network hops in the network)
     */
    public void testDurablePropagationSyncTtl0BrokerRestart() throws Exception {
        testDurablePropagation(0, false, true, List.of(0, 0, 0, 0, 0));
    }

    public void testDurablePropagationSyncTtl1BrokerRestart() throws Exception {
        testDurablePropagation(1, false, true, List.of(0, 1, 0, 1, 0));
    }

    public void testDurablePropagationSyncTtl4BrokerRestart() throws Exception {
        testDurablePropagation(4, false, true, List.of(1, 2, 2, 2, 1));
    }

    // This test also tests the fix in NetworkBridgeUtils.getBrokerSubscriptionInfo()
    // where the clientId was missing for the offline durable which could cause demand to be created
    // by mistake and create a loop. Without the clientId the sync could not tell the
    // network durable was for its direct bridge (was created because of a local consumer
    // on its broker) so a loop could be created on restart if the sync happened before the
    // real consumer connected first.
    public void testDurablePropagationSyncTtlNotSetBrokerRestart() throws Exception {
        testDurablePropagation(-1, false, true, List.of(1, 2, 2, 2, 1));
    }

    /*
     * The following test demonstrates the problem with missing TTL and is only solved
     * by making dynamicOnly true, on restart consumers have yet to come back online
     * for offline durables so we end up missing TTL and create extra demand on sync.
     * TTL is missing on broker restart. This is for TTL > 1 but less than same number
     * of network hops in the network
     */
    public void testDurablePropagationDynamicFalseTtl2BrokerRestartFail() throws Exception {
        try {
            testDurablePropagation(2, false, true, List.of(0, 1, 2, 1, 0));
            fail("Exepected to fail");
        } catch (AssertionFailedError e) {
            // expected
        }
    }

    public void testDurablePropagationDynamicFalseTtl3BrokerRestartFail() throws Exception {
        try {
            testDurablePropagation(2, false, true, List.of(0, 2, 2, 2, 0));
            fail("Exepected to fail");
        } catch (AssertionFailedError e) {
            // expected
        }
    }

    /*
     * The following tests make sure propagation works correctly with TTL set
     * when dynamicOnly is false or true, and durable sync enabled.
     * When false, durables get reactivated and a sync is done and TTL can be
     * missing for offline durables so this works correctly ONLY if the consumers
     * are online and have correct TTL info. When true, consumers drive reactivation
     * entirely and the sync is skipped.
     *
     * For dynamicOnly being false, these tests for TTL > 1 demonstrate the improvement in
     * createDemandSubscription() inside of DemandForwardingBridgeSupport
     * where we now include the full broker path (all TTL info) if the
     * consumer is already online that created the subscription. Before,
     * we just included the remote broker.
     *
     * If brokers are restarted/consumers offline, TTL can be missing so it's recommended
     * to set dynamicOnly to true if TTL is > 1 (TTL 1 and TTL -1 can still be handled)
     * to prevent propagation of durables that shouldn't exist and let consumers drive
     * the reactivation.
     *
     * The tests keep the consumers online and only restart the connectors and not
     * the brokers so the consumer info and TTL are preserved for the tests.
     */
    public void testDurablePropagationDynamicFalseTtl0() throws Exception {
        testDurablePropagation(0, false, List.of(0, 0, 0, 0, 0));
    }

    public void testDurablePropagationDynamicFalseTtl1() throws Exception {
        testDurablePropagation(1, false, List.of(0, 1, 0, 1, 0));
    }

    public void testDurablePropagationDynamicFalseTtl2() throws Exception {
        testDurablePropagation(2, false, List.of(0, 1, 2, 1, 0));
    }

    public void testDurablePropagationDynamicFalseTtl3() throws Exception {
        testDurablePropagation(3, false, List.of(0, 2, 2, 2, 0));
    }

    public void testDurablePropagationDynamicFalseTtl4() throws Exception {
        testDurablePropagation(4, false, List.of(1, 2, 2, 2, 1));
    }

    public void testDurablePropagationDynamicFalseTtlNotSet() throws Exception {
        testDurablePropagation(-1, false, List.of(1, 2, 2, 2, 1));
    }

    public void testDurablePropagationDynamicTrueTtl0() throws Exception {
        testDurablePropagation(0, true, List.of(0, 0, 0, 0, 0));
    }

    public void testDurablePropagationDynamicTrueTtl1() throws Exception {
        testDurablePropagation(1, true, List.of(0, 1, 0, 1, 0));
    }

    public void testDurablePropagationDynamicTrueTtl2() throws Exception {
        testDurablePropagation(2, true, List.of(0, 1, 2, 1, 0));
    }

    public void testDurablePropagationDynamicTrueTtl3() throws Exception {
        testDurablePropagation(3, true, List.of(0, 2, 2, 2, 0));
    }

    public void testDurablePropagationDynamicTrueTtl4() throws Exception {
        testDurablePropagation(4, true, List.of(1, 2, 2, 2, 1));
    }

    public void testDurablePropagationDynamicTrueTtlNotSet() throws Exception {
        testDurablePropagation(-1, true, List.of(1, 2, 2, 2, 1));
    }

    private void testDurablePropagation(int ttl, boolean dynamicOnly,
                                        List<Integer> expected) throws Exception {
        testDurablePropagation(ttl, dynamicOnly, false, expected);
    }

    private void testDurablePropagation(int ttl, boolean dynamicOnly, boolean restartBrokers,
                                         List<Integer> expected) throws Exception {
        duplex = true;

        // Setup broker networks
        NetworkConnector nc1 = bridgeBrokers("Broker_A_A", "Broker_B_B", dynamicOnly, ttl);
        NetworkConnector nc2 = bridgeBrokers("Broker_B_B", "Broker_C_C", dynamicOnly, ttl);
        NetworkConnector nc3 = bridgeBrokers("Broker_C_C", "Broker_D_D", dynamicOnly, ttl);
        NetworkConnector nc4 = bridgeBrokers("Broker_D_D", "Broker_E_E", dynamicOnly, ttl);

        startAllBrokers();
        stopNetworkConnectors(nc1, nc2, nc3, nc4);

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("Broker_A_A");
        Session ses2 = createSession("Broker_E_E");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientE = ses2.createDurableSubscriber(dest, "subE");
        Thread.sleep(1000);

        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, 0);

        startNetworkConnectors(nc1, nc2, nc3, nc4);
        Thread.sleep(1000);

        // Check that the correct network durables exist
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, expected.get(0));
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, expected.get(1));
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, expected.get(2));
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, expected.get(3));
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, expected.get(4));

        if (restartBrokers) {
            // go offline and restart to make sure sync works to re-enable and doesn't
            // propagate wrong demand
            clientA.close();
            clientE.close();
            destroyAllBrokers();
            setUp();
            brokers.values().forEach(bi -> bi.broker.setDeleteAllMessagesOnStartup(false));
            bridgeBrokers("Broker_A_A", "Broker_B_B", dynamicOnly, ttl);
            bridgeBrokers("Broker_B_B", "Broker_C_C", dynamicOnly, ttl);
            bridgeBrokers("Broker_C_C", "Broker_D_D", dynamicOnly, ttl);
            bridgeBrokers("Broker_D_D", "Broker_E_E", dynamicOnly, ttl);
            startAllBrokers();
        } else {
            // restart just the network connectors but leave the consumers online
            // to test sync works ok. Things should work for all cases both dynamicOnly
            // false and true because TTL info still exits and consumers are online
            stopNetworkConnectors(nc1, nc2, nc3, nc4);
            Thread.sleep(1000);
            startNetworkConnectors(nc1, nc2, nc3, nc4);
            Thread.sleep(1000);
        }

        // after restarting the bridges, check sync/demand are correct
        assertNCDurableSubsCount(brokers.get("Broker_A_A").broker, dest, expected.get(0));
        assertNCDurableSubsCount(brokers.get("Broker_B_B").broker, dest, expected.get(1));
        assertNCDurableSubsCount(brokers.get("Broker_C_C").broker, dest, expected.get(2));
        assertNCDurableSubsCount(brokers.get("Broker_D_D").broker, dest, expected.get(3));
        assertNCDurableSubsCount(brokers.get("Broker_E_E").broker, dest, expected.get(4));
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
                if (sub != null && sub.isActive()) {
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
        deletePersistentMessagesOnStartup = true;
        String options = new String("?persistent=true&useJmx=false");
        createBroker(new URI("broker:(tcp://localhost:61616)/Broker_A_A" + options));
        createBroker(new URI("broker:(tcp://localhost:61617)/Broker_B_B" + options));
        createBroker(new URI("broker:(tcp://localhost:61618)/Broker_C_C" + options));
        createBroker(new URI("broker:(tcp://localhost:61619)/Broker_D_D" + options));
        createBroker(new URI("broker:(tcp://localhost:61620)/Broker_E_E" + options));
    }

    @Override
    protected void configureBroker(BrokerService broker) {
        broker.setBrokerId(broker.getBrokerName());
        broker.setDeleteAllMessagesOnStartup(deletePersistentMessagesOnStartup);
        broker.setDataDirectory("target" + File.separator + "test-data" + File.separator + "DurableFiveBrokerNetworkBridgeTest");
    }

    protected void startNetworkConnectors(NetworkConnector... connectors) throws Exception {
        for (NetworkConnector connector : connectors) {
            connector.start();
        }
    }

    protected void stopNetworkConnectors(NetworkConnector... connectors) throws Exception {
        for (NetworkConnector connector : connectors) {
            connector.stop();
        }
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
