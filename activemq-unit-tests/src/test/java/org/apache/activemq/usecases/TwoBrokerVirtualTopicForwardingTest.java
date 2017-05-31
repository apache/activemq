/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;

import javax.jms.MessageConsumer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

import static org.apache.activemq.TestSupport.*;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class TwoBrokerVirtualTopicForwardingTest extends JmsMultipleBrokersTestSupport {

    public void testBridgeVirtualTopicQueues() throws Exception {

        bridgeAndConfigureBrokers("BrokerA", "BrokerB");
        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer clientA = createConsumer("BrokerA", createDestination("Consumer.A.VirtualTopic.tempTopic", false));
        MessageConsumer clientB = createConsumer("BrokerB", createDestination("Consumer.B.VirtualTopic.tempTopic", false));


        // give a sec to let advisories propogate
        Thread.sleep(500);

        ActiveMQQueue queueA = new ActiveMQQueue("Consumer.A.VirtualTopic.tempTopic");
        Destination destination = getDestination(brokers.get("BrokerA").broker, queueA);
        assertEquals(1, destination.getConsumers().size());

        ActiveMQQueue queueB = new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic");
        destination = getDestination(brokers.get("BrokerA").broker, queueB);
        assertEquals(1, destination.getConsumers().size());

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        assertNull(getDestination(brokers.get("BrokerA").broker, virtualTopic));
        assertNull(getDestination(brokers.get("BrokerB").broker, virtualTopic));

        // send some messages
        sendMessages("BrokerA", virtualTopic, 1);

        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        Thread.sleep(2000);

        assertEquals(1, msgsA.getMessageCount());
        assertEquals(1, msgsB.getMessageCount());

    }

    public void testDontBridgeQueuesWithOnlyQueueConsumers() throws Exception{
        dontBridgeVirtualTopicConsumerQueues("BrokerA", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer clientA = createConsumer("BrokerA", createDestination("Consumer.A.VirtualTopic.tempTopic", false));
        MessageConsumer clientB = createConsumer("BrokerB", createDestination("Consumer.B.VirtualTopic.tempTopic", false));


        // give a sec to let advisories propogate
        Thread.sleep(500);

        ActiveMQQueue queueA = new ActiveMQQueue("Consumer.A.VirtualTopic.tempTopic");
        Destination destination = getDestination(brokers.get("BrokerA").broker, queueA);
        assertEquals(1, destination.getConsumers().size());

        ActiveMQQueue queueB = new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic");
        destination = getDestination(brokers.get("BrokerA").broker, queueB);
        assertNull(destination);

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        assertNull(getDestination(brokers.get("BrokerA").broker, virtualTopic));
        assertNull(getDestination(brokers.get("BrokerB").broker, virtualTopic));

        // send some messages
        sendMessages("BrokerA", virtualTopic, 1);

        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(0);

        // ensure we don't get any more messages
        Thread.sleep(2000);

        assertEquals(1, msgsA.getMessageCount());
        assertEquals(0, msgsB.getMessageCount());
    }

    public void testDontBridgeQueuesWithBothTypesConsumers() throws Exception{
        dontBridgeVirtualTopicConsumerQueues("BrokerA", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        MessageConsumer clientA = createConsumer("BrokerA", createDestination("Consumer.A.VirtualTopic.tempTopic", false));
        MessageConsumer clientB = createConsumer("BrokerB", createDestination("Consumer.B.VirtualTopic.tempTopic", false));
        MessageConsumer clientC = createConsumer("BrokerB", createDestination("VirtualTopic.tempTopic", true));


        // give a sec to let advisories propogate
        Thread.sleep(500);

        ActiveMQQueue queueA = new ActiveMQQueue("Consumer.A.VirtualTopic.tempTopic");
        Destination destination = getDestination(brokers.get("BrokerA").broker, queueA);
        assertEquals(1, destination.getConsumers().size());

        ActiveMQQueue queueB = new ActiveMQQueue("Consumer.B.VirtualTopic.tempTopic");
        destination = getDestination(brokers.get("BrokerA").broker, queueB);
        assertNull(destination);

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.tempTopic");
        assertNotNull(getDestination(brokers.get("BrokerA").broker, virtualTopic));
        assertNotNull(getDestination(brokers.get("BrokerB").broker, virtualTopic));

        // send some messages
        sendMessages("BrokerA", virtualTopic, 1);

        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        Thread.sleep(2000);

        assertEquals(1, msgsA.getMessageCount());
        assertEquals(1, msgsB.getMessageCount());
    }

    private void bridgeAndConfigureBrokers(String local, String remote) throws Exception {
        NetworkConnector bridge = bridgeBrokers(local, remote);
        bridge.setDecreaseNetworkConsumerPriority(true);
    }

    private void dontBridgeVirtualTopicConsumerQueues(String local, String remote) throws Exception {
        NetworkConnector bridge = bridgeBrokers(local, remote);
        bridge.setDecreaseNetworkConsumerPriority(true);

        LinkedList<ActiveMQDestination> excludedDestinations = new LinkedList<ActiveMQDestination>();
        excludedDestinations.add(new ActiveMQQueue("Consumer.*.VirtualTopic.>"));

        bridge.setExcludedDestinations(excludedDestinations);

    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?useJmx=false&deleteAllMessagesOnStartup=true");
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));
    }

    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        configurePersistenceAdapter(broker);
        return broker;
    }

    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }
}
