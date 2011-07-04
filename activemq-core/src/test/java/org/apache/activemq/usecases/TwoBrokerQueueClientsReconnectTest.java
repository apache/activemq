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
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.apache.activemq.network.NetworkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class TwoBrokerQueueClientsReconnectTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 100; // Best if a factor of 100
    protected static final int PREFETCH_COUNT = 1;
    protected static final int NETWORK_PREFETCH = 1;
    private static final Logger LOG = LoggerFactory.getLogger(TwoBrokerQueueClientsReconnectTest.class);


    protected int msgsClient1;
    protected int msgsClient2;
    protected String broker1;
    protected String broker2;

    public void testClientAReceivesOnly() throws Exception {
        broker1 = "BrokerA";
        broker2 = "BrokerB";

        doOneClientReceivesOnly();
    }

    public void testClientBReceivesOnly() throws Exception {
        broker1 = "BrokerB";
        broker2 = "BrokerA";

        doOneClientReceivesOnly();
    }

    public void doOneClientReceivesOnly() throws Exception {
        // allow immediate replay back to origin
        applyRateLimitNetworkFilter(0);

        // Bridge brokers
        bridgeBrokers(broker1, broker2);
        bridgeBrokers(broker2, broker1);

        // Run brokers
        startAllBrokers();

        // Create queue
        Destination dest = createDestination("TEST.FOO", false);

        // Create consumers
        MessageConsumer client1 = createConsumer(broker1, dest);
        MessageConsumer client2 = createConsumer(broker2, dest);

        // Give clients time to register with broker
        Thread.sleep(500);

        // Always send messages to broker A
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Close the second client, messages should be sent to the first client
        client2.close();

        // Let the first client receive all messages
        msgsClient1 += receiveAllMessages(client1);
        client1.close();

        // First client should have received 100 messages
        assertEquals("Client for " + broker1 + " should have receive all messages.", MESSAGE_COUNT, msgsClient1);
    }

    public void testClientAReceivesOnlyAfterReconnect() throws Exception {
        broker1 = "BrokerA";
        broker2 = "BrokerB";

        doOneClientReceivesOnlyAfterReconnect();
    }

    public void testClientBReceivesOnlyAfterReconnect() throws Exception {
        broker1 = "BrokerB";
        broker2 = "BrokerA";

        doOneClientReceivesOnlyAfterReconnect();
    }

    public void doOneClientReceivesOnlyAfterReconnect() throws Exception {
        // allow immediate replay back to origin
        applyRateLimitNetworkFilter(0);

        // Bridge brokers
        bridgeBrokers(broker1, broker2);
        bridgeBrokers(broker2, broker1);

        // Run brokers
        startAllBrokers();

        // Create queue
        Destination dest = createDestination("TEST.FOO", false);

        // Create first consumer
        MessageConsumer client1 = createConsumer(broker1, dest);
        MessageConsumer client2 = createConsumer(broker2, dest);

        // Give clients time to register with broker
        Thread.sleep(500);

        // Always send message to broker A
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Let the first client receive the first 20% of messages
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.20));

        // Disconnect the first client
        client1.close();

        // Create another client for the first broker
        client1 = createConsumer(broker1, dest);
        Thread.sleep(500);

        // Close the second client, messages should be sent to the first client
        client2.close();

        // Receive the rest of the messages
        msgsClient1 += receiveAllMessages(client1);
        client1.close();

        // The first client should have received 100 messages
        assertEquals("Client for " + broker1 + " should have received all messages.", MESSAGE_COUNT, msgsClient1);
    }

    public void testTwoClientsReceiveClientADisconnects() throws Exception {
        broker1 = "BrokerA";
        broker2 = "BrokerB";

        doTwoClientsReceiveOneClientDisconnects();
    }

    public void testTwoClientsReceiveClientBDisconnects() throws Exception {
        broker1 = "BrokerB";
        broker2 = "BrokerA";

        doTwoClientsReceiveOneClientDisconnects();
    }

    public void doTwoClientsReceiveOneClientDisconnects() throws Exception {
        // ensure all message do not flow across the network too quickly
        applyRateLimitNetworkFilter(0.8 * MESSAGE_COUNT);

        // Bridge brokers
        bridgeBrokers(broker1, broker2);
        bridgeBrokers(broker2, broker1);

        // Run brokers
        startAllBrokers();

        // Create queue
        Destination dest = createDestination("TEST.FOO", false);

        // Create first client
        MessageConsumer client1 = createConsumer(broker1, dest);
        MessageConsumer client2 = createConsumer(broker2, dest);

        // Give clients time to register with broker
        Thread.sleep(500);

        // Always send messages to broker A
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        LOG.info("Let each client receive 20% of the messages - 40% total");
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.20));
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.20));

        // Disconnect the first client
        client1.close();

        LOG.info("Let the second client receive the rest of the messages");
        msgsClient2 += receiveAllMessages(client2);
        client2.close();

        // First client should have received 20% of the messages
        assertEquals("Client for " + broker1 + " should have received 20% of the messages.", (int)(MESSAGE_COUNT * 0.20), msgsClient1);

        // Second client should have received 80% of the messages
        assertEquals("Client for " + broker2 + " should have received 80% of the messages.", (int)(MESSAGE_COUNT * 0.80), msgsClient2);
    }

    public void testTwoClientsReceiveClientAReconnects() throws Exception {
        broker1 = "BrokerA";
        broker2 = "BrokerB";

        doTwoClientsReceiveOneClientReconnects();
    }

    public void testTwoClientsReceiveClientBReconnects() throws Exception {
        broker1 = "BrokerB";
        broker2 = "BrokerA";

        doTwoClientsReceiveOneClientReconnects();
    }

    public void doTwoClientsReceiveOneClientReconnects() throws Exception {
        // ensure all message do not flow across the network too quickly
        applyRateLimitNetworkFilter(0.2 * MESSAGE_COUNT);

        // Bridge brokers
        bridgeBrokers(broker1, broker2);
        bridgeBrokers(broker2, broker1);

        // Run brokers
        startAllBrokers();

        // Create queue
        Destination dest = createDestination("TEST.FOO", false);

        // Create the first client
        MessageConsumer client1 = createConsumer(broker1, dest);
        MessageConsumer client2 = createConsumer(broker2, dest);

        // Give clients time to register with broker
        Thread.sleep(500);

        // Always send messages to broker A
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Let each client receive 20% of the messages - 40% total
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.20));
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.20));

        LOG.info("msgsClient1=" + msgsClient1);
        LOG.info("msgsClient2=" + msgsClient2);

        Thread.sleep(1000);
        LOG.info("Disconnect the first client");
        client1.close();

        LOG.info("Let the second client receive 20% more of the total messages");
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.20));

        LOG.info("msgsClient2=" + msgsClient2);

        // Create another client for broker 1
        client1 = createConsumer(broker1, dest);
        Thread.sleep(1000);

        // Let each client receive 20% of the messages - 40% total
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.20));
        client1.close();
        LOG.info("new consumer addition, msgsClient1=" + msgsClient1);

        Thread.sleep(2000);
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.20));
        client2.close();
        LOG.info("msgsClient2=" + msgsClient2);

        // First client should have received 40 messages
        assertEquals("Client for " + broker1 + " should have received 40% of the messages.", (int)(MESSAGE_COUNT * 0.40), msgsClient1);

        // Second client should have received 60 messages
        assertEquals("Client for " + broker2 + " should have received 60% of the messages.", (int)(MESSAGE_COUNT * 0.60), msgsClient2);
    }

    private void applyRateLimitNetworkFilter(double rateLimit) {
        ConditionalNetworkBridgeFilterFactory filterFactory = new ConditionalNetworkBridgeFilterFactory();
        filterFactory.setReplayWhenNoConsumers(true);
        filterFactory.setRateLimit((int) rateLimit);
        filterFactory.setRateDuration(1000);

        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            broker.getDestinationPolicy().getDefaultEntry().setNetworkBridgeFilterFactory(filterFactory);
        }
    }

    public void testTwoClientsReceiveTwoClientReconnects() throws Exception {
        // ensure all message do not flow across the network too quickly
        applyRateLimitNetworkFilter(0.5 * MESSAGE_COUNT);

        broker1 = "BrokerA";
        broker2 = "BrokerB";

        // Bridge brokers
        bridgeBrokers(broker1, broker2);
        bridgeBrokers(broker2, broker1);

        // Run brokers
        startAllBrokers();

        // Create queue
        Destination dest = createDestination("TEST.FOO", false);

        // Create the first client
        MessageConsumer client1 = createConsumer(broker1, dest);
        MessageConsumer client2 = createConsumer(broker2, dest);

        // Give clients time to register with broker
        Thread.sleep(500);

        // Always send messages to broker A
        sendMessages("BrokerA", dest, MESSAGE_COUNT);

        // Let each client receive 20% of the messages - 40% total
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.20));
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.20));

        LOG.info("Disconnect both clients");
        client1.close();
        client2.close();

        // Let each client receive 30% more of the total messages - 60% total
        LOG.info("Serially create another two clients for each broker and consume in turn");
        client1 = createConsumer(broker1, dest);
        msgsClient1 += receiveExactMessages(client1, (int)(MESSAGE_COUNT * 0.30));
        client1.close();

        // the close will allow replay or the replay of the remaining messages
        client2 = createConsumer(broker2, dest);
        msgsClient2 += receiveExactMessages(client2, (int)(MESSAGE_COUNT * 0.30));
        client2.close();

        // First client should have received 50% of the messages
        assertEquals("Client for " + broker1 + " should have received 50% of the messages.", (int)(MESSAGE_COUNT * 0.50), msgsClient1);

        // Second client should have received 50% of the messages
        assertEquals("Client for " + broker2 + " should have received 50% of the messages.", (int)(MESSAGE_COUNT * 0.50), msgsClient2);
    }

    protected int receiveExactMessages(MessageConsumer consumer, int msgCount) throws Exception {
        Message msg;
        int i;
        for (i = 0; i < msgCount; i++) {
            msg = consumer.receive(4000);
            if (msg == null) {
                LOG.error("Consumer failed to receive exactly " + msgCount + " messages. Actual messages received is: " + i);
                break;
            }
        }

        return i;
    }

    protected int receiveAllMessages(MessageConsumer consumer) throws Exception {
        int msgsReceived = 0;

        Message msg;
        do {
            msg = consumer.receive(1000);
            if (msg != null) {
                msgsReceived++;
            }
        } while (msg != null);

        return msgsReceived;
    }

    protected MessageConsumer createConsumer(String brokerName, Destination dest) throws Exception {
        Connection conn = createConnection(brokerName);
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return sess.createConsumer(dest);
    }

    protected void configureBroker(BrokerService broker) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setEnableAudit(false);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
    }

    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL, boolean conduit, boolean failover) throws Exception {
        NetworkConnector nc = super.bridgeBrokers(localBroker,remoteBroker, dynamicOnly, networkTTL, conduit, failover);
        nc.setPrefetchSize(NETWORK_PREFETCH);
        nc.setDecreaseNetworkConsumerPriority(true);
        return nc;
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        createBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:(tcp://localhost:61617)/BrokerB?persistent=false&useJmx=false"));

        // Configure broker connection factory
        ActiveMQConnectionFactory factoryA;
        ActiveMQConnectionFactory factoryB;
        factoryA = (ActiveMQConnectionFactory)getConnectionFactory("BrokerA");
        factoryB = (ActiveMQConnectionFactory)getConnectionFactory("BrokerB");

        // Set prefetch policy
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setAll(PREFETCH_COUNT);

        factoryA.setPrefetchPolicy(policy);
        factoryB.setPrefetchPolicy(policy);

        msgsClient1 = 0;
        msgsClient2 = 0;
    }
}
