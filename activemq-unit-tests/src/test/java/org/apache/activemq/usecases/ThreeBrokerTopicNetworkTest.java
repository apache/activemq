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

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Topic;

import junit.framework.Test;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.JmsMultipleBrokersTestSupport.BrokerItem;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.util.MessageIdList;

/**
 * 
 */
public class ThreeBrokerTopicNetworkTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 100;
    public boolean dynamicOnly;

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void testABandBCbrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

//      let consumers propogate around the network
        Thread.sleep(2000);
        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 2);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT * 2);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT * 2, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT * 2, msgsC.getMessageCount());
    }
    
    public void initCombosForTestABandBCbrokerNetworkWithSelectors() {
    	addCombinationValues("dynamicOnly", new Object[] {true, false});
    }
    
    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void testABandBCbrokerNetworkWithSelectors() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, 2, true);
        bridgeBrokers("BrokerB", "BrokerC", dynamicOnly, 2, true);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerC", dest, "dummy = 33");
        MessageConsumer clientB = createConsumer("BrokerC", dest, "dummy > 30");
        MessageConsumer clientC = createConsumer("BrokerC", dest, "dummy = 34");

        // let consumers propogate around the network
        Thread.sleep(2000);
        // Send messages
        // Send messages for broker A
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put("dummy", 33);
        sendMessages("BrokerA", dest, MESSAGE_COUNT, props);
        props.put("dummy", 34);
        sendMessages("BrokerA", dest, MESSAGE_COUNT * 2, props);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerC", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerC", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT * 2) ;

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT *2, msgsC.getMessageCount());
    }

    /**
     * BrokerA <- BrokerB -> BrokerC
     */
    public void testBAandBCbrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerB", "BrokerA");
        bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

//      let consumers propogate around the network
        Thread.sleep(2000);
        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT * 2);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT * 2);

        assertEquals(MESSAGE_COUNT * 2, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT * 2, msgsC.getMessageCount());
    }

    /**
     * BrokerA -> BrokerB <- BrokerC
     */
    public void testABandCBbrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerC", "BrokerB");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

//      let consumers propogate around the network
        Thread.sleep(2000);
        
        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsC.getMessageCount());
    }

    /**
     * BrokerA <-> BrokerB <-> BrokerC
     */
    public void testAllConnectedBrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerA");
        bridgeBrokers("BrokerB", "BrokerC");
        bridgeBrokers("BrokerC", "BrokerB");
        bridgeBrokers("BrokerA", "BrokerC");
        bridgeBrokers("BrokerC", "BrokerA");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);
        //let consumers propogate around the network
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT * 3);

        assertEquals(MESSAGE_COUNT * 3, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsC.getMessageCount());
    }

    public void testAllConnectedBrokerNetworkSingleProducerTTL() throws Exception {
        
        // duplicates are expected with ttl of 2 as each broker is connected to the next
        // but the dups are suppressed by the store and now also by the topic sub when enableAudit
        // default (true) is present in a matching destination policy entry
        int networkTTL = 2;
        boolean conduitSubs = true;
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerB", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerC", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerA", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerC", "BrokerA", dynamicOnly, networkTTL, conduitSubs);

        PolicyMap policyMap = new PolicyMap();
        // enable audit is on by default just need to give it matching policy entry
        // so it will be applied to the topic subscription
        policyMap.setDefaultEntry(new PolicyEntry());
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            broker.setDestinationPolicy(policyMap);
            broker.setDeleteAllMessagesOnStartup(true);
        }
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);
        //let consumers propogate around the network
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerA", dest, 1);
        
        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(1);
        msgsC.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        Thread.sleep(2000);
        
        assertEquals(1, msgsA.getMessageCount());
        assertEquals(1, msgsB.getMessageCount());
        assertEquals(1, msgsC.getMessageCount());
    }

    public void testAllConnectedBrokerNetworkDurableSubTTL() throws Exception {
        int networkTTL = 2;
        boolean conduitSubs = true;
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerB", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerC", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerA", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeBrokers("BrokerC", "BrokerA", dynamicOnly, networkTTL, conduitSubs);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createDurableSubscriber("BrokerA", (Topic)dest, "clientA");
        MessageConsumer clientB = createDurableSubscriber("BrokerB", (Topic)dest, "clientB");
        MessageConsumer clientC = createDurableSubscriber("BrokerC", (Topic)dest, "clientC");
        //let consumers propogate around the network
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerA", dest, 1);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(1);
        msgsC.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        Thread.sleep(2000);
        
        assertEquals(1, msgsA.getMessageCount());
        assertEquals(1, msgsB.getMessageCount());
        assertEquals(1, msgsC.getMessageCount());
    }
    
    /**
     * BrokerA <-> BrokerB <-> BrokerC
     */
    public void testAllConnectedUsingMulticast() throws Exception {
        // Setup broker networks
        bridgeAllBrokers();

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);
        
        //let consumers propogate around the network
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 3);
        msgsC.waitForMessagesToArrive(MESSAGE_COUNT * 3);

        assertEquals(MESSAGE_COUNT * 3, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsB.getMessageCount());
        assertEquals(MESSAGE_COUNT * 3, msgsC.getMessageCount());
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?persistent=false&useJmx=false"); 
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC" + options));
    }
    
    public static Test suite() {
    	return suite(ThreeBrokerTopicNetworkTest.class);
    }
}
