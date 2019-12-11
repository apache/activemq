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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// demonstrate the use of conduit=true/false on a network bridge for topics with selectors
// note selectors are ignored when conduit=true
public class TopicBridgeSelectorConduitOnOff {
    private static final Logger LOG = LoggerFactory.getLogger(TopicBridgeSelectorConduitOnOff.class);

    BrokerService brokerA,brokerB;
    final int numProducers = 20;
    final int numConsumers = 20;
    final int numberOfMessagesToSendPerProducer = 5000;
    final ActiveMQTopic destination = new ActiveMQTopic("TOPIC");



    @After
    public void stopBrokers() throws Exception {
        brokerA.stop();
        brokerB.stop();
    }

    @Test
    public void testForwardsWithConduitSubsTrue() throws Exception {
        doTestWithConduit(true);
    }

    @Test
    public void testForwardsWithConduitSubsFalse() throws Exception {
        doTestWithConduit(false);
    }

    private void doTestWithConduit(boolean conduitPlease)  throws Exception {

        brokerA = newBroker("A");
        brokerB = newBroker("B");
        brokerB.start();

        // bridge
        NetworkConnector networkConnector = bridgeBrokers(brokerA, brokerB, conduitPlease);
        brokerA.start();


        // wait for bridge creation
        while (networkConnector.activeBridges().size() == 0) {
            LOG.info("num bridges: " + networkConnector.activeBridges());
            TimeUnit.SECONDS.sleep(1);
        }


        // a given consumer selects half the messages
        CountDownLatch allReceived = new CountDownLatch(numConsumers/2 * (numberOfMessagesToSendPerProducer *numProducers));

        final ActiveMQConnectionFactory localConnectionFactoryForProducers = new ActiveMQConnectionFactory(brokerA.getTransportConnectorByScheme("tcp").getPublishableConnectString()
                + "?jms.watchTopicAdvisories=false");

        final ActiveMQConnectionFactory remoteConnectionFactoryForConsumers = new ActiveMQConnectionFactory(brokerB.getTransportConnectorByScheme("tcp").getPublishableConnectString()
                + "?jms.watchTopicAdvisories=false");

        ExecutorService consumersExecutor = Executors.newFixedThreadPool(numConsumers);

        final AtomicInteger receivedCount = new AtomicInteger(0);
        final Collection<Connection> connections =  Collections.synchronizedList(new LinkedList<Connection>());
        final CountDownLatch consumersRegistered = new CountDownLatch(numConsumers);
        for (int i=0; i<numConsumers; i++) {
            final int id = i;
            consumersExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection connection = remoteConnectionFactoryForConsumers.createConnection();
                        connection.start();
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                        MessageConsumer consumerWithSelector = session.createConsumer(destination, (id%2==0 ? "COLOUR = 'RED'" : "COLOUR = 'BLUE'"));
                        consumerWithSelector.setMessageListener(new MessageListener() {
                            @Override
                            public void onMessage(Message message) {
                                int messageCount = receivedCount.incrementAndGet();
                                allReceived.countDown();
                                if (messageCount % 20000 == 0) {
                                    try {
                                        LOG.info("Consumer id: " + id + ", message COLOUR:" + message.getStringProperty("COLOUR"));
                                    } catch (JMSException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        });
                        connections.add(connection);
                        consumersRegistered.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }


        // need to be sure all subs are active before we publish to have some guaentee of stats
        consumersRegistered.await(20, TimeUnit.SECONDS);

        // would really need to verify on the local broker...
        // lets do that
        Topic topic = (Topic) brokerA.getDestination(destination);
        LOG.info("Num consumers: " + topic.getConsumers().size());

        while (topic.getConsumers().size() != (conduitPlease ? 1 : numConsumers)) {
            LOG.info("Num consumers: " + topic.getConsumers().size());
            TimeUnit.SECONDS.sleep(1);
        }

        ExecutorService producersExecutor = Executors.newFixedThreadPool(numProducers);
        for (int i=0; i<numProducers; i++) {
            producersExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection connection = localConnectionFactoryForProducers.createConnection();
                        connection.start();
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                        MessageProducer producer = session.createProducer(destination);
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                        for (int id = 0; id < numberOfMessagesToSendPerProducer; id++) {
                            final BytesMessage message = session.createBytesMessage();
                            message.setStringProperty("COLOUR", id % 2 == 0 ? "RED" : "BLUE");
                            producer.send(message);
                        }
                        connection.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        producersExecutor.shutdown();
        producersExecutor.awaitTermination(5, TimeUnit.MINUTES);

        final long start = System.currentTimeMillis();

        // wait for all messages to get delivered to the consumers
        // a given consumer selects half the messages
        allReceived.await(5, TimeUnit.MINUTES);

        LOG.info("Duration to Receive after producers complete: " + (System.currentTimeMillis() - start) + "ms");

        LOG.info("Topic enqueues: " + topic.getDestinationStatistics().getEnqueues().getCount() + ", Total received: " + receivedCount.get() + ", forwards: " + topic.getDestinationStatistics().getForwards().getCount());

        TimeUnit.SECONDS.sleep(10);
        LOG.info("Topic enqueues: " + topic.getDestinationStatistics().getEnqueues().getCount() + ", Total received: " + receivedCount.get() + ", forwards: " + topic.getDestinationStatistics().getForwards().getCount());


        for (Connection connection: connections) {
            try {
                connection.close();
            } catch (Exception ignored) {}
        }
        consumersExecutor.shutdown();
        consumersExecutor.awaitTermination(5, TimeUnit.MINUTES);
    }


    private BrokerService newBroker(String name) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setBrokerName(name);
        brokerService.addConnector("tcp://0.0.0.0:0");

        PolicyMap map = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(0);
        // the audit defaults to 1k producers and lk messages, but those casn be configured via policy
        defaultEntry.setEnableAudit(true); // default is true only if there is a policy entry in force, otherwise the sub accepts the duplicates
        map.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(map);
        return brokerService;
    }


    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean conduitPlease) throws Exception {

        String uri = "static:(" + remoteBroker.getTransportConnectorByScheme("tcp").getPublishableConnectString() + ")";

        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName(localBroker.getBrokerName() + "-to-" + remoteBroker.getBrokerName());
        connector.setConduitSubscriptions(conduitPlease);

        localBroker.addNetworkConnector(connector);

        LOG.info("Bridging with conduit subs:" + conduitPlease);
        return connector;
    }
}