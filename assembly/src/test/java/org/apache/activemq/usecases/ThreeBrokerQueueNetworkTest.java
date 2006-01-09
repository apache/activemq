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

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.util.MessageIdList;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import java.net.URI;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ThreeBrokerQueueNetworkTest extends JmsMultipleBrokersTestSupport {

    /**
     * BrokerA -> BrokerB -> BrokerC
     */
    public void test_AB_BC_BrokerNetwork() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");
        bridgeBrokers("BrokerB", "BrokerC");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientC = createConsumer("BrokerC", dest);

        // Send messages
        sendMessages("BrokerA", dest, 10);

        // Let's try to wait for any messages. Should be none.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);
        assertEquals(0, msgsC.getMessageCount());
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
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientC = createConsumer("BrokerC", dest);

        // Send messages
        sendMessages("BrokerB", dest, 10);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        // Total received should be 10
        assertEquals(10, msgsA.getMessageCount() + msgsC.getMessageCount());
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
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        // Send messages
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Get message count
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsB.waitForMessagesToArrive(20);

        assertEquals(20, msgsB.getMessageCount());
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
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        assertEquals(30, msgsA.getMessageCount() + msgsB.getMessageCount() + msgsC.getMessageCount());
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
        sendMessages("BrokerA", dest, 10);
        sendMessages("BrokerB", dest, 10);
        sendMessages("BrokerC", dest, 10);

        // Let's try to wait for any messages.
        Thread.sleep(1000);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        assertEquals(30, msgsA.getMessageCount() + msgsB.getMessageCount() + msgsC.getMessageCount());
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61618)/BrokerC?persistent=false&useJmx=false"));
    }
}
