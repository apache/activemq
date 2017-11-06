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
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;

import com.google.common.collect.Lists;

import junit.framework.Test;

/**
 * Test to make sure durable subscriptions propagate properly throughout network bridges
 * and that conduit subscriptions work properly
 */
public class DurableThreeBrokerNetworkBridgeTest extends JmsMultipleBrokersTestSupport {

    @Override
    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName) throws Exception {
        NetworkConnector connector = super.bridgeBrokers(localBrokerName, remoteBrokerName);
        connector.setDynamicallyIncludedDestinations(
                Lists.<ActiveMQDestination> newArrayList(new ActiveMQTopic("TEST.FOO?forceDurable=true")));
        connector.setDuplex(true);
        connector.setDecreaseNetworkConsumerPriority(false);
        connector.setConduitSubscriptions(true);
        connector.setSyncDurableSubs(true);
        connector.setNetworkTTL(-1);
        return connector;
    }

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void testDurablePropagation() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

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

    public void testForceDurablePropagation() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        MessageConsumer clientA = ses.createConsumer(dest);

        // let consumers propagate around the network
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);

        sendMessages("BrokerC", dest, 1);
        assertNotNull(clientA.receive(1000));

        Session ses2 = createSession("BrokerC");
        MessageConsumer clientC = ses2.createConsumer(dest);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientC.close();

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);
    }

    public void testDurablePropagationSync() throws Exception {
        // Setup broker networks
        NetworkConnector nc1 = bridgeBrokers("BrokerA", "BrokerB");
        NetworkConnector nc2 = bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        nc1.stop();
        nc2.stop();

        // Setup destination
        ActiveMQTopic dest = (ActiveMQTopic) createDestination("TEST.FOO", true);

        // Setup consumers
        Session ses = createSession("BrokerA");
        Session ses2 = createSession("BrokerC");
        MessageConsumer clientA = ses.createDurableSubscriber(dest, "subA");
        MessageConsumer clientB = ses.createDurableSubscriber(dest, "subB");
        MessageConsumer clientC = ses2.createDurableSubscriber(dest, "subC");

        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 0);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 0);

        nc1.start();
        nc2.start();

        //there will be 2 network durables, 1 for each direction of the bridge
        assertNCDurableSubsCount(brokers.get("BrokerB").broker, dest, 2);
        assertNCDurableSubsCount(brokers.get("BrokerC").broker, dest, 1);
        assertNCDurableSubsCount(brokers.get("BrokerA").broker, dest, 1);

        clientA.close();
        clientB.close();
        clientC.close();
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
        return suite(DurableThreeBrokerNetworkBridgeTest.class);
    }
}
