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

import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoBrokerNetworkLoadBalanceTest extends JmsMultipleBrokersTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(TwoBrokerNetworkLoadBalanceTest.class);
    public void testLoadBalancing() throws Exception {
        // network consumers get a lower priority by default, so a local consumer that is not
        // full would take every message. Give both sides the same priority so load balances.
        bridgeBrokers("BrokerA", "BrokerB").setDecreaseNetworkConsumerPriority(false);
        bridgeBrokers("BrokerB", "BrokerA").setDecreaseNetworkConsumerPriority(false);

        startAllBrokers();
        waitForBridgeFormation();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        // Wait for each broker to see the other side's network consumer before producing
        waitForNetworkConsumer("BrokerA", (ActiveMQDestination) dest);
        waitForNetworkConsumer("BrokerB", (ActiveMQDestination) dest);

        // Send messages
        sendMessages("BrokerA", dest, 5000);

        // Send messages
        sendMessages("BrokerB", dest, 1000);

        // Get message count
        final MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        final MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        assertTrue("all messages should be consumed", Wait.waitFor(
                () -> msgsA.getMessageCount() + msgsB.getMessageCount() == 6000, Wait.MAX_WAIT_MILLIS, 10));

        LOG.info("A got: " +  msgsA.getMessageCount());
        LOG.info("B got: " +  msgsB.getMessageCount());
         
        assertTrue("B got is fair share: " + msgsB.getMessageCount(), msgsB.getMessageCount() > 2000);
    }

    private void waitForNetworkConsumer(String brokerName, ActiveMQDestination dest) throws Exception {
        final BrokerService broker = brokers.get(brokerName).broker;
        assertTrue(brokerName + " should have a local and a network consumer", Wait.waitFor(
                () -> broker.getDestination(dest).getConsumers().size() == 2, 10000, 10));
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
