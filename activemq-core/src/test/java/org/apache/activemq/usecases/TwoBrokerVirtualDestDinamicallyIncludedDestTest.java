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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;

public class TwoBrokerVirtualDestDinamicallyIncludedDestTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 10;
    boolean dynamicOnly = true;
    int networkTTL = 1;
    boolean conduit = true;
    boolean suppressDuplicateQueueSubscriptions = true;
    boolean decreaseNetworkConsumerPriority = true;

    /**
     * BrokerA -> BrokerB && BrokerB -> BrokerA
     */
    public void testTopicDinamicallyIncludedBehavior() throws Exception {

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("test", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", dest);
        MessageConsumer clientB = createConsumer("BrokerB", dest);

        Thread.sleep(2*1000);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        msgsA.waitForMessagesToArrive(MESSAGE_COUNT);
        assertEquals(MESSAGE_COUNT, msgsA.getMessageCount());

        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        msgsB.waitForMessagesToArrive(MESSAGE_COUNT);
        assertEquals(0, msgsB.getMessageCount());

    }

    /**
     * BrokerA -> BrokerB && BrokerB -> BrokerA
     */
    public void testVirtualDestinationsDinamicallyIncludedBehavior1() throws Exception {

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("global.test", true);

        // Setup consumers
        MessageConsumer clientB1 = createConsumer("BrokerB", dest);
        MessageConsumer clientB2 = createConsumer("BrokerB", createDestination("Consumer.foo-bar.global.test", false));

        Thread.sleep(2*1000);

        int messageCount = MESSAGE_COUNT;
        // Send messages
        sendMessages("BrokerA", dest, messageCount);

        // Get message count
        MessageIdList msgsB1 = getConsumerMessages("BrokerB", clientB1);
        msgsB1.waitForMessagesToArrive(messageCount);
        assertEquals(messageCount, msgsB1.getMessageCount());

        MessageIdList msgsB2 = getConsumerMessages("BrokerB", clientB2);
        msgsB2.waitForMessagesToArrive(messageCount);
        assertEquals(messageCount, msgsB2.getMessageCount());

    }

    /**
     * BrokerA -> BrokerB && BrokerB -> BrokerA
     */
    public void testVirtualDestinationsDinamicallyIncludedBehavior2() throws Exception {

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("global.test", true);

        // Setup consumers
        //MessageConsumer clientB1 = createConsumer("BrokerB", dest);
        MessageConsumer clientB2 = createConsumer("BrokerB", createDestination("Consumer.foo-bar.global.test", false));

        Thread.sleep(2*1000);

        // Send messages
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Get message count
        MessageIdList msgsB2 = getConsumerMessages("BrokerB", clientB2);
        msgsB2.waitForMessagesToArrive(MESSAGE_COUNT);
        assertEquals(MESSAGE_COUNT, msgsB2.getMessageCount());

    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?useJmx=false&deleteAllMessagesOnStartup=true");
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));

        // Setup broker networks
        NetworkConnector nc1 = bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduit);
        nc1.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
        nc1.setSuppressDuplicateQueueSubscriptions(suppressDuplicateQueueSubscriptions);
        nc1.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.QUEUE_TYPE));
        nc1.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.TOPIC_TYPE));
        nc1.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("Consumer.>", ActiveMQDestination.QUEUE_TYPE));

        NetworkConnector nc2 = bridgeBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduit);
        nc2.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
        nc2.setSuppressDuplicateQueueSubscriptions(suppressDuplicateQueueSubscriptions);
        nc2.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.QUEUE_TYPE));
        nc2.addDynamicallyIncludedDestination(ActiveMQDestination.createDestination("global.>", ActiveMQDestination.TOPIC_TYPE));
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);

        configurePersistenceAdapter(broker);

        // make all topics virtual and consumers use the default prefix
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(new VirtualDestination[]{new VirtualTopic()});
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{virtualDestinationInterceptor};
        broker.setDestinationInterceptors(destinationInterceptors);
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }
}