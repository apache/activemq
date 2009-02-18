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
import java.util.HashMap;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ThreeBrokerQueueNetworkTest extends JmsMultipleBrokersTestSupport {
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

    /**
     * BrokerA <-> BrokerB <-> BrokerC
     */
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

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false"));
    }
}
