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
package org.apache.activemq.network;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.TopicSubscriber;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkBrokerDetachTest {

    private static final String BROKER_NAME = "broker";
    private static final String REM_BROKER_NAME = "networkedBroker";
    private static final String DESTINATION_NAME = "testQ";
    private static final int NUM_CONSUMERS = 1;

    protected static final Logger LOG = LoggerFactory.getLogger(NetworkBrokerDetachTest.class);
    protected final int numRestarts = 3;
    protected final int networkTTL = 2;
    protected final boolean dynamicOnly = false;

    protected BrokerService broker;
    protected BrokerService networkedBroker;

    protected BrokerService createBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        configureBroker(broker);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    private void addNetworkConnectorToBroker(final BrokerService source, final BrokerService target) throws Exception {
        final String targetUri = target.getTransportConnectors().get(0).getConnectUri().toString();
        final NetworkConnector networkConnector = source.addNetworkConnector(
                "static:(" + targetUri + "?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        configureNetworkConnector(networkConnector);
        source.startNetworkConnector(networkConnector, null);
    }

    protected BrokerService createNetworkedBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName(REM_BROKER_NAME);
        configureBroker(broker);
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    private void configureNetworkConnector(final NetworkConnector networkConnector) {
        networkConnector.setDuplex(false);
        networkConnector.setNetworkTTL(networkTTL);
        networkConnector.setDynamicOnly(dynamicOnly);
    }

    // variants for each store....
    protected void configureBroker(final BrokerService broker) throws Exception {
        final KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("target/activemq-data/kahadb/" + broker.getBrokerName() + "NetworBrokerDetatchTest"));
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    @Before
    public void init() throws Exception {
        broker = createBroker();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();

        networkedBroker = createNetworkedBroker();
        networkedBroker.setDeleteAllMessagesOnStartup(true);
        networkedBroker.start();

        // Add network connectors after both brokers are started and ports are assigned
        addNetworkConnectorToBroker(broker, networkedBroker);
        addNetworkConnectorToBroker(networkedBroker, broker);
    }

    @After
    public void cleanup() throws Exception {
        if (networkedBroker != null) {
            networkedBroker.stop();
            networkedBroker.waitUntilStopped();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testNetworkedBrokerDetach() throws Exception {
        LOG.info("Creating Consumer on the networked broker ...");
        // Create a consumer on the networked broker
        final ConnectionFactory consFactory = createConnectionFactory(networkedBroker);
        final Connection consConn = consFactory.createConnection();
        final Session consSession = consConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final ActiveMQDestination destination = (ActiveMQDestination) consSession.createQueue(DESTINATION_NAME);
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consSession.createConsumer(destination);
        }

        assertTrue("got expected consumer count from mbean within time limit",
                   verifyConsumerCount(1, destination, broker));

        LOG.info("Stopping Consumer on the networked broker ...");
        // Closing the connection will also close the consumer
        consConn.close();

        // We should have 0 consumer for the queue on the local broker
        assertTrue("got expected 0 count from mbean within time limit", verifyConsumerCount(0, destination, broker));
    }

    @Test
    public void testNetworkedBrokerDurableSubAfterRestart() throws Exception {

        final AtomicInteger count = new AtomicInteger(0);
        final MessageListener counter = message -> count.incrementAndGet();

        LOG.info("Creating durable consumer on each broker ...");
        final ActiveMQTopic destination = registerDurableConsumer(networkedBroker, counter);
        registerDurableConsumer(broker, counter);

        assertTrue("got expected consumer count from local broker mbean within time limit",
                verifyConsumerCount(2, destination, broker));

        assertTrue("got expected consumer count from network broker mbean within time limit",
                verifyConsumerCount(2, destination, networkedBroker));

        sendMessageTo(destination, broker);

        assertTrue("Got one message on each", verifyMessageCount(2, count));

        LOG.info("Stopping brokerTwo...");
        networkedBroker.stop();
        networkedBroker.waitUntilStopped();

        LOG.info("restarting  broker Two...");
        networkedBroker = createNetworkedBroker();
        networkedBroker.start();

        // Add network connector after restart (ports are dynamically assigned)
        addNetworkConnectorToBroker(networkedBroker, broker);

        LOG.info("Recreating durable Consumer on the broker after restart...");
        registerDurableConsumer(networkedBroker, counter);

        sendMessageTo(destination, broker);

        // expect similar after restart
        assertTrue("got expected consumer count from local broker mbean within time limit",
                verifyConsumerCount(2, destination, broker));

        // a durable sub is auto bridged on restart unless dynamicOnly=true
        assertTrue("got expected consumer count from network broker mbean within time limit",
                verifyConsumerCount(2, destination, networkedBroker));

        assertTrue("got no inactive subs on broker", verifyDurableConsumerCount(0, broker));
        assertTrue("got no inactive subs on other broker", verifyDurableConsumerCount(0, networkedBroker));

        assertTrue("Got two more messages after restart", verifyMessageCount(4, count));
        assertTrue("still Got just two more messages", verifyMessageCount(4, count));
    }

    private boolean verifyMessageCount(final int expected, final AtomicInteger count) throws Exception {
        return Wait.waitFor(() -> expected == count.get());
    }

    private ActiveMQTopic registerDurableConsumer(
            final BrokerService brokerService, final MessageListener listener) throws Exception {
        final ConnectionFactory factory = createConnectionFactory(brokerService);
        final Connection connection = factory.createConnection();
        connection.setClientID("DurableOne");
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final ActiveMQTopic destination = (ActiveMQTopic) session.createTopic(DESTINATION_NAME);
        // unique to a broker
        final TopicSubscriber sub = session.createDurableSubscriber(destination, "SubOne" + brokerService.getBrokerName());
        sub.setMessageListener(listener);
        return destination;
    }

    private void sendMessageTo(final ActiveMQTopic destination, final BrokerService brokerService) throws Exception {
        final ConnectionFactory factory = createConnectionFactory(brokerService);
        final Connection conn = factory.createConnection();
        conn.start();
        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(destination).send(session.createTextMessage("Hi"));
        conn.close();
    }

    protected ConnectionFactory createConnectionFactory(final BrokerService broker) throws Exception {
        final String url = broker.getTransportConnectors().get(0).getServer().getConnectURI().toString();
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connectionFactory.setOptimizedMessageDispatch(true);
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setUseCompression(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setUseAsyncSend(false);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setWatchTopicAdvisories(true);
        final ActiveMQPrefetchPolicy qPrefetchPolicy = new ActiveMQPrefetchPolicy();
        qPrefetchPolicy.setQueuePrefetch(100);
        qPrefetchPolicy.setTopicPrefetch(1000);
        connectionFactory.setPrefetchPolicy(qPrefetchPolicy);
        connectionFactory.setAlwaysSyncSend(true);
        return connectionFactory;
    }

    // JMX Helper Methods
    private boolean verifyConsumerCount(final long expectedCount, final ActiveMQDestination destination, final BrokerService broker) throws Exception {
        return Wait.waitFor(() -> {
            try {
                final ObjectName[] destinations = destination.isQueue()
                        ? broker.getAdminView().getQueues()
                        : broker.getAdminView().getTopics();

                for (final ObjectName name : destinations) {
                    final DestinationViewMBean view = (DestinationViewMBean)
                        broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class, true);

                    if (view.getName().equals(destination.getPhysicalName())) {
                        LOG.info("Consumers for " + destination.getPhysicalName() + " on " + broker + " : " + view.getConsumerCount());
                        LOG.info("Subs: " + Arrays.asList(view.getSubscriptions()));
                        if (expectedCount == view.getConsumerCount()) {
                            return true;
                        }
                    }
                }
            } catch (Exception ignoreAndRetry) {
            }
            return false;
        });
    }

    private boolean verifyDurableConsumerCount(final long expectedCount, final BrokerService broker) throws Exception {
        return Wait.waitFor(() -> {
            final BrokerView view = broker.getAdminView();
            if (view != null) {
                final ObjectName[] subs = broker.getAdminView().getInactiveDurableTopicSubscribers();
                if (subs != null) {
                    LOG.info("inactive durable subs on " + broker + " : " + Arrays.asList(subs));
                    if (expectedCount == subs.length) {
                        return true;
                    }
                }
            }
            return false;
        });
    }
}
