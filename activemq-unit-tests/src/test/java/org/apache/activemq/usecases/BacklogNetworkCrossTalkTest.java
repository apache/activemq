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
import javax.jms.MessageConsumer;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BacklogNetworkCrossTalkTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(BacklogNetworkCrossTalkTest.class);

    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
        brokers.put(brokerName, new BrokerItem(broker));

        return broker;
    }

    public void testProduceConsume() throws Exception {
        createBroker("A");
        createBroker("B");

        NetworkConnector nc = bridgeBrokers("A", "B");
        nc.setDuplex(true);
        nc.setDispatchAsync(false);
        startAllBrokers();

        waitForBridgeFormation();

        final int numMessages = 1000;
        // Create queue
        ActiveMQDestination destA = createDestination("AAA", false);
        sendMessages("A", destA, numMessages);

        ActiveMQDestination destB = createDestination("BBB", false);
        sendMessages("B", destB, numMessages);

        // consume across network
        LOG.info("starting consumers..");

        // Setup consumers
        MessageConsumer clientA = createConsumer("A", destB);
        // Setup consumers
        MessageConsumer clientB = createConsumer("B", destA);


        final long maxWait = 5 * 60 * 1000l;
        MessageIdList listA = getConsumerMessages("A", clientA);
        listA.setMaximumDuration(maxWait);
        listA.waitForMessagesToArrive(numMessages);

        MessageIdList listB = getConsumerMessages("B", clientB);
        listB.setMaximumDuration(maxWait);
        listB.waitForMessagesToArrive(numMessages);

        assertEquals("got all on A" + listA.getMessageCount(),
                numMessages, listA.getMessageCount());

        assertEquals("got all on B" + listB.getMessageCount(),
                numMessages, listB.getMessageCount());

    }

    @Override
    public void setUp() throws Exception {
        messageSize = 5000;
        super.setMaxTestTime(10*60*1000);
        super.setAutoFail(true);
        super.setUp();
    }
}
