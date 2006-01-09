/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.JmsMultipleBrokersTestSupport;

import javax.jms.MessageConsumer;
import javax.jms.Destination;
import java.net.URI;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ThreeBrokerTopicNetworkTest extends JmsMultipleBrokersTestSupport {

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

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(10);
        msgsB.waitForMessagesToArrive(20);
        msgsC.waitForMessagesToArrive(20);

        assertEquals(10, msgsA.getMessageCount());
        assertEquals(20, msgsB.getMessageCount());
        assertEquals(20, msgsC.getMessageCount());
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

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(20);
        msgsB.waitForMessagesToArrive(10);
        msgsC.waitForMessagesToArrive(20);

        assertEquals(20, msgsA.getMessageCount());
        assertEquals(10, msgsB.getMessageCount());
        assertEquals(20, msgsC.getMessageCount());
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

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(10);
        msgsB.waitForMessagesToArrive(30);
        msgsC.waitForMessagesToArrive(10);

        assertEquals(10, msgsA.getMessageCount());
        assertEquals(30, msgsB.getMessageCount());
        assertEquals(10, msgsC.getMessageCount());
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

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(30);
        msgsB.waitForMessagesToArrive(30);
        msgsC.waitForMessagesToArrive(30);

        assertEquals(30, msgsA.getMessageCount());
        assertEquals(30, msgsB.getMessageCount());
        assertEquals(30, msgsC.getMessageCount());
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

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(30);
        msgsB.waitForMessagesToArrive(30);
        msgsC.waitForMessagesToArrive(30);

        assertEquals(30, msgsA.getMessageCount());
        assertEquals(30, msgsB.getMessageCount());
        assertEquals(30, msgsC.getMessageCount());
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false"));
    }
}
