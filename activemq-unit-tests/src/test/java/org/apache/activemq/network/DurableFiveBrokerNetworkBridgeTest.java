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
    private boolean deletePersistentMessagesOnStartup = true;

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
        nc3.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO2?forceDurable=true")));
        nc4.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO2?forceDurable=true")));

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

    protected Session createSession(String broker) throws Exception {
        Connection con = createConnection(broker);
        con.start();
        return con.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static Test suite() {
        return suite(DurableFiveBrokerNetworkBridgeTest.class);
    }
}
