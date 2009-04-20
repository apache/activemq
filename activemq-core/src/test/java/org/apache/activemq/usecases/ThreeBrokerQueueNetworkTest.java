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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ThreeBrokerQueueNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(ThreeBrokerQueueNetworkTest.class);
    protected static final int MESSAGE_COUNT = 100;

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void testABandBCbrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientC = createConsumer("BrokerC", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Let's try to wait for any messages. Should be none.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);
        assertEquals(0, msgsC.getMessageCount());
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
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);
        Thread.sleep(2000); //et subscriptions get propagated
        // Send messages
        sendMessages("BrokerB", dest, MESSAGE_COUNT);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        // Total received should be 100
        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount() + msgsC.getMessageCount());
    }
    
    /**
     * BrokerA <- BrokerB -> BrokerC
     */
    public void testBAandBCbrokerNetworkWithSelectorsSendFirst() throws Exception {
    	// Setup broker networks
        bridgeBrokers("BrokerB", "BrokerA", true, 1, false);
        bridgeBrokers("BrokerB", "BrokerC", true, 1, false);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        
        // Send messages for broker A
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put("broker", "BROKER_A");
        sendMessages("BrokerB", dest, MESSAGE_COUNT, props);

        //Send messages for broker C
        props.clear();
        props.put("broker", "BROKER_C");
        sendMessages("BrokerB", dest, MESSAGE_COUNT, props);
        
        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest, "broker = 'BROKER_A'");
        MessageConsumer clientC = createConsumer("BrokerC", dest, "broker = 'BROKER_C'");
        Thread.sleep(2000); //et subscriptions get propagated
        
        // Let's try to wait for any messages.
        //Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);
        
        // Total received should be 100
        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsC.getMessageCount());
    }
    
    /**
     * BrokerA <- BrokerB -> BrokerC
     */
    public void testBAandBCbrokerNetworkWithSelectorsSubscribeFirst() throws Exception {
    	// Setup broker networks
        bridgeBrokers("BrokerB", "BrokerA", true, 1, false);
        bridgeBrokers("BrokerB", "BrokerC", true, 1, false);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest, "broker = 'BROKER_A'");
        MessageConsumer clientC = createConsumer("BrokerC", dest, "broker = 'BROKER_C'");
        Thread.sleep(2000); //et subscriptions get propagated
        
        
        // Send messages for broker A
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put("broker", "BROKER_A");
        sendMessages("BrokerB", dest, MESSAGE_COUNT, props);

        //Send messages for broker C
        props.clear();
        props.put("broker", "BROKER_C");
        sendMessages("BrokerB", dest, MESSAGE_COUNT, props);
        
        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);
        
        // Total received should be 100
        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsC.getMessageCount());
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
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsB.waitForMessagesToArrive(MESSAGE_COUNT * 2);

        assertEquals(MESSAGE_COUNT * 2, msgsB.getMessageCount());
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
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        assertEquals(MESSAGE_COUNT * 3, msgsA.getMessageCount() + msgsB.getMessageCount() + msgsC.getMessageCount());
    }

    public void testAllConnectedUsingMulticast() throws Exception {
        // Setup broker networks
        bridgeAllBrokers();

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);
        sendMessages("BrokerB", dest, MESSAGE_COUNT);
        sendMessages("BrokerC", dest, MESSAGE_COUNT);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        assertEquals(MESSAGE_COUNT * 3, msgsA.getMessageCount() + msgsB.getMessageCount() + msgsC.getMessageCount());
    }

    
    public void testAllConnectedUsingMulticastProducerConsumerOnA() throws Exception {
        bridgeAllBrokers("default", 3, false);
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        int messageCount = 2000;
        CountDownLatch messagesReceived = new CountDownLatch(messageCount);
        MessageConsumer clientA = createConsumer("BrokerA", dest, messagesReceived);

        // Let's try to wait for advisory percolation.
        Thread.sleep(1000);

        // Send messages
        sendMessages("BrokerA", dest, messageCount);
     
        assertTrue(messagesReceived.await(30, TimeUnit.SECONDS));
        
        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        assertEquals(messageCount, msgsA.getMessageCount());
    }

    public void testAllConnectedWithSpare() throws Exception {
        bridgeAllBrokers("default", 3, false);
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        int messageCount = 2000;
        CountDownLatch messagesReceived = new CountDownLatch(messageCount);
        MessageConsumer clientA = createConsumer("BrokerA", dest, messagesReceived);

        // ensure advisory percolation.
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerB", dest, messageCount);

        assertTrue(messagesReceived.await(30, TimeUnit.SECONDS));
        
        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        assertEquals(messageCount, msgsA.getMessageCount());
    }
    
    public void testMigrateConsumerStuckMessages() throws Exception {
        boolean suppressQueueDuplicateSubscriptions = false;
        bridgeAllBrokers("default", 3, suppressQueueDuplicateSubscriptions);
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);    
        
        // Setup consumers
        LOG.info("Consumer on A");
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        
        // ensure advisors have percolated
        Thread.sleep(2000);
        
        LOG.info("Consumer on B");
        int messageCount = 2000;
        
        // will only get half of the messages
        CountDownLatch messagesReceived = new CountDownLatch(messageCount/2);
        MessageConsumer clientB = createConsumer("BrokerB", dest, messagesReceived);
          
        // ensure advisors have percolated
        Thread.sleep(2000);

        LOG.info("Close consumer on A");
        clientA.close();

        // ensure advisors have percolated
        Thread.sleep(2000);
       
        LOG.info("Send to B"); 
        sendMessages("BrokerB", dest, messageCount);

        // Let's try to wait for any messages.
        assertTrue(messagesReceived.await(30, TimeUnit.SECONDS));

        // Get message count
        MessageIdList msgs = getConsumerMessages("BrokerB", clientB);
        
        // see will any more arrive
        Thread.sleep(500);        
        assertEquals(messageCount/2, msgs.getMessageCount());
        
        // pick up the stuck messages
        messagesReceived = new CountDownLatch(messageCount/2);
        clientA = createConsumer("BrokerA", dest, messagesReceived);
        // Let's try to wait for any messages.
        assertTrue(messagesReceived.await(30, TimeUnit.SECONDS));
        
        msgs = getConsumerMessages("BrokerA", clientA);
        assertEquals(messageCount/2, msgs.getMessageCount());
    }

    // use case: for maintenance, migrate consumers and producers from one
    // node in the network to another so node can be replaced/updated
    public void testMigrateConsumer() throws Exception {
        boolean suppressQueueDuplicateSubscriptions = true;
        boolean decreaseNetworkConsumerPriority = true;
        bridgeAllBrokers("default", 3, suppressQueueDuplicateSubscriptions, decreaseNetworkConsumerPriority);
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);    
        
        // Setup consumers
        LOG.info("Consumer on A");
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        
        // ensure advisors have percolated
        Thread.sleep(2000);
        
        LOG.info("Consumer on B");
        int messageCount = 2000;
        CountDownLatch messagesReceived = new CountDownLatch(messageCount);
        MessageConsumer clientB = createConsumer("BrokerB", dest, messagesReceived);
       
        // make the consumer slow so that any network consumer has a chance, even
        // if it has a lower priority
        MessageIdList msgs = getConsumerMessages("BrokerB", clientB);
        msgs.setProcessingDelay(10);
        
        // ensure advisors have percolated
        Thread.sleep(2000);

        LOG.info("Close consumer on A");
        clientA.close();

        // ensure advisors have percolated
        Thread.sleep(2000);
       
        LOG.info("Send to B"); 
        sendMessages("BrokerB", dest, messageCount);

        // Let's try to wait for any messages.
        assertTrue(messagesReceived.await(60, TimeUnit.SECONDS));
        assertEquals(messageCount, msgs.getMessageCount());      
    }

    public void testNoDuplicateQueueSubs() throws Exception {
        
        bridgeAllBrokers("default", 3, true);
        
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        String brokerName = "BrokerA";
        createConsumer(brokerName, dest);
        
        // wait for advisories
        Thread.sleep(2000);
        
        // verify there is one consumer on each broker, no cycles
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            verifyConsumerCount(broker, 1, dest);
        }
    }
    

    public void testDuplicateQueueSubs() throws Exception {
        
        bridgeAllBrokers("default", 3, false);
        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        String brokerName = "BrokerA";
        createConsumer(brokerName, dest);
        
        // wait for advisories
        Thread.sleep(2000);
        
        verifyConsumerCount(brokers.get(brokerName).broker, 1, dest);
        
        // in a cyclic network, other brokers will get second order consumer
        // an alternative route to A via each other
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            if (!brokerName.equals(broker.getBrokerName())) {
                verifyConsumerCount(broker, 2, dest);
            }
        }
    }

    private void verifyConsumerCount(BrokerService broker, int count, Destination dest) throws Exception {
        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        Queue internalQueue = (Queue) regionBroker.getDestinations(ActiveMQDestination.transform(dest)).iterator().next(); 
        assertEquals("consumer count on " + broker.getBrokerName() + " matches for q: " + internalQueue, count, internalQueue.getConsumers().size());      
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false"));
    }
}
