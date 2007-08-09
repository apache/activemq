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

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ThreeBrokerTopicNetworkTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 100;

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void test_AB_BC_BrokerNetwork() throws Exception {
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

    /**
     * BrokerA <- BrokerB -> BrokerC
     */
    public void test_BA_BC_BrokerNetwork() throws Exception {
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
    public void test_AB_CB_BrokerNetwork() throws Exception {
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
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false"));
    }
}
