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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.Command;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.MessageIdList;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TwoBrokerMessageNotSentToRemoteWhenNoConsumerTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 100;

    /**
     * BrokerA -> BrokerB
     */
    public void testRemoteBrokerHasConsumer() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        Thread.sleep(2000);
        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());
        assertEquals(MESSAGE_COUNT, msgsB.getMessageCount());

    }

    /**
     * BrokerA -> BrokerB
     */
    public void testRemoteBrokerHasNoConsumer() throws Exception {
        // Setup broker networks
        bridgeBrokers("BrokerA", "BrokerB");

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);

        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);

        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());

    }
    
    /**
     * BrokerA -> BrokerB && BrokerB -> BrokerA
     */
    public void testDuplexStaticRemoteBrokerHasNoConsumer() throws Exception {
        // Setup broker networks
        boolean dynamicOnly = true;
        int networkTTL = 2;
        boolean conduit = true;
        bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduit);
        bridgeBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduit);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);

        Thread.sleep(2*1000);
        
        int messageCount = 2000;
        // Send messages
        sendMessages("BrokerA", dest, messageCount);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);

        msgsA.waitForMessagesToArrive(messageCount);

        assertEquals(messageCount, msgsA.getMessageCount());

    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI(
                "broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI(
                "broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));
    }
}
