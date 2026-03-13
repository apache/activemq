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
package org.apache.activemq.bugs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import javax.management.ObjectName;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  A Test case for AMQ-1479
 */
public class DurableConsumerTest extends CombinationTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(DurableConsumerTest.class);
    private static final int COUNT = 1024;
    private static final String CONSUMER_NAME = "DURABLE_TEST";
    protected BrokerService broker;

    protected final String bindAddress = "tcp://localhost:0";

    protected final byte[] payload = new byte[1024 * 32];
    protected ConnectionFactory factory;
    protected final Vector<Exception> exceptions = new Vector<>();

    private static final String TOPIC_NAME = "failoverTopic";
    public boolean useDedicatedTaskRunner = false;

    private class SimpleTopicSubscriber implements MessageListener, ExceptionListener {

        private TopicConnection topicConnection = null;

        public SimpleTopicSubscriber(final String connectionURL, final String clientId, final String topicName) {

            final ActiveMQConnectionFactory topicConnectionFactory = new ActiveMQConnectionFactory(connectionURL);
            try {
                final Topic topic = new ActiveMQTopic(topicName);
                topicConnection = topicConnectionFactory.createTopicConnection();
                topicConnection.setClientID(clientId);
                topicConnection.start();

                final TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                final TopicSubscriber topicSubscriber = topicSession.createDurableSubscriber(topic, clientId);
                topicSubscriber.setMessageListener(this);

            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public void onMessage(final Message arg0) {
        }

        public void closeConnection() {
            if (topicConnection != null) {
                try {
                    topicConnection.close();
                } catch (JMSException e) {
                }
            }
        }

        public void onException(final JMSException exception) {
            exceptions.add(exception);
        }
    }

    private class MessagePublisher implements Runnable {
        private final boolean shouldPublish = true;
        private final String connectionUrl;

        MessagePublisher(final String connectionUrl) {
            this.connectionUrl = connectionUrl;
        }

        public void run() {
            final TopicConnectionFactory topicConnectionFactory = new ActiveMQConnectionFactory(connectionUrl);
            TopicPublisher topicPublisher = null;
            Message message = null;
            try {
                final Topic topic = new ActiveMQTopic(TOPIC_NAME);
                final TopicConnection topicConnection = topicConnectionFactory.createTopicConnection();
                final TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                topicPublisher = topicSession.createPublisher(topic);
                message = topicSession.createMessage();
            } catch (Exception ex) {
                exceptions.add(ex);
            }
            while (shouldPublish) {
                try {
                    topicPublisher.publish(message, DeliveryMode.PERSISTENT, 1, 2 * 60 * 60 * 1000);
                } catch (JMSException ex) {
                    exceptions.add(ex);
                }
                try {
                    Thread.sleep(1);
                } catch (Exception ex) {
                }
            }
        }
    }

    private void configurePersistence(final BrokerService broker) throws Exception {
        final File dataDirFile = new File("target/" + getName());
        final KahaDBPersistenceAdapter kahaDBAdapter = new KahaDBPersistenceAdapter();
        kahaDBAdapter.setDirectory(dataDirFile);
        broker.setPersistenceAdapter(kahaDBAdapter);
    }

    public void testFailover() throws Exception {

        configurePersistence(broker);
        broker.start();

        final String brokerUri = broker.getTransportConnectors().get(0).getConnectUri().toString();
        final String failoverUrl = "failover:(" + brokerUri + ")";

        final Thread publisherThread = new Thread(new MessagePublisher(failoverUrl));
        publisherThread.start();
        final int numSubs = 100;
        final List<SimpleTopicSubscriber> list = new ArrayList<>(numSubs);
        for (int i = 0; i < numSubs; i++) {

            final int id = i;
            final Thread thread = new Thread(() -> {
                final SimpleTopicSubscriber s = new SimpleTopicSubscriber(failoverUrl, System.currentTimeMillis() + "-" + id, TOPIC_NAME);
                list.add(s);
            });
            thread.start();

        }

        Wait.waitFor(() -> numSubs == list.size());

        broker.stop();
        broker = createBroker(false);
        configurePersistence(broker);
        broker.start();

        assertTrue("broker restarted and durable subs recovered",
                Wait.waitFor(() -> broker.getAdminView() != null
                        && broker.getAdminView().getDurableTopicSubscribers().length
                         + broker.getAdminView().getInactiveDurableTopicSubscribers().length > 0,
                        15000, 500));

        for (final SimpleTopicSubscriber s : list) {
            s.closeConnection();
        }
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    // makes heavy use of threads and can demonstrate https://issues.apache.org/activemq/browse/AMQ-2028
    // with use dedicatedTaskRunner=true and produce OOM
    public void initCombosForTestConcurrentDurableConsumer() {
        addCombinationValues("useDedicatedTaskRunner", new Object[] { Boolean.TRUE, Boolean.FALSE });
    }

    public void testConcurrentDurableConsumer() throws Exception {

        broker.start();
        broker.waitUntilStarted();

        factory = createConnectionFactory();
        final String topicName = getName();
        final int numMessages = 500;
        final int numConsumers = 1;
        final CountDownLatch counsumerStarted = new CountDownLatch(numConsumers);
        final AtomicInteger receivedCount = new AtomicInteger();
        final Runnable consumer = () -> {
            final String consumerName = Thread.currentThread().getName();
            int acked = 0;
            int received = 0;

            try {
                while (acked < numMessages / 2) {
                    // take one message and close, ack on occasion
                    final Connection consumerConnection = factory.createConnection();
                    ((ActiveMQConnection) consumerConnection).setWatchTopicAdvisories(false);
                    consumerConnection.setClientID(consumerName);
                    final Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    final Topic topic = consumerSession.createTopic(topicName);
                    consumerConnection.start();

                    final MessageConsumer mc = consumerSession.createDurableSubscriber(topic, consumerName);

                    counsumerStarted.countDown();
                    Message msg = null;
                    do {
                        msg = mc.receive(5000);
                        if (msg != null) {
                            receivedCount.incrementAndGet();
                            if (received != 0 && received % 100 == 0) {
                                LOG.info("Received msg: " + msg.getJMSMessageID());
                            }
                            if (++received % 2 == 0) {
                                msg.acknowledge();
                                acked++;
                            }
                        }
                    } while (msg == null);

                    consumerConnection.close();
                }
                assertTrue(received >= acked);
            } catch (Exception e) {
                e.printStackTrace();
                exceptions.add(e);
            }
        };

        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        for (int i = 0; i < numConsumers; i++) {
            executor.execute(consumer);
        }

        assertTrue(counsumerStarted.await(30, TimeUnit.SECONDS));

        final Connection producerConnection = factory.createConnection();
        ((ActiveMQConnection) producerConnection).setWatchTopicAdvisories(false);
        final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = producerSession.createTopic(topicName);
        final MessageProducer producer = producerSession.createProducer(topic);
        producerConnection.start();
        for (int i = 0; i < numMessages; i++) {
            final BytesMessage msg = producerSession.createBytesMessage();
            msg.writeBytes(payload);
            producer.send(msg);
            if (i != 0 && i % 100 == 0) {
                LOG.info("Sent msg " + i);
            }
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        Wait.waitFor(() -> {
            LOG.info("receivedCount: " + receivedCount.get());
            return receivedCount.get() == numMessages;
        }, 360 * 1000);
        assertEquals("got required some messages", numMessages, receivedCount.get());
        assertTrue("no exceptions, but: " + exceptions, exceptions.isEmpty());
    }

    public void testConsumerRecover() throws Exception {
        doTestConsumer(true);
    }

    public void testConsumer() throws Exception {
        doTestConsumer(false);
    }

    public void testPrefetchViaBrokerConfig() throws Exception {

        final Integer prefetchVal = 150;
        final PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setDurableTopicPrefetch(prefetchVal);
        policyEntry.setPrioritizedMessages(true);
        final PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
        broker.start();

        factory = createConnectionFactory();
        final Connection consumerConnection = factory.createConnection();
        consumerConnection.setClientID(CONSUMER_NAME);
        final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = consumerSession.createTopic(getClass().getName());
        final MessageConsumer consumer = consumerSession.createDurableSubscriber(topic, CONSUMER_NAME);
        consumerConnection.start();

        final ObjectName activeSubscriptionObjectName = broker.getAdminView().getDurableTopicSubscribers()[0];
        final Object prefetchFromSubView = broker.getManagementContext().getAttribute(activeSubscriptionObjectName, "PrefetchSize");
        assertEquals(prefetchVal, prefetchFromSubView);
    }

    public void doTestConsumer(final boolean forceRecover) throws Exception {

        if (forceRecover) {
            configurePersistence(broker);
        }
        broker.start();

        factory = createConnectionFactory();
        Connection consumerConnection = factory.createConnection();
        consumerConnection.setClientID(CONSUMER_NAME);
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = consumerSession.createTopic(getClass().getName());
        MessageConsumer consumer = consumerSession.createDurableSubscriber(topic, CONSUMER_NAME);
        consumerConnection.start();
        consumerConnection.close();
        broker.stop();
        broker = createBroker(false);
        if (forceRecover) {
            configurePersistence(broker);
        }
        broker.start();

        // Re-create factory after broker restart (port may have changed)
        factory = createConnectionFactory();
        final Connection producerConnection = factory.createConnection();

        final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final MessageProducer producer = producerSession.createProducer(topic);
        producerConnection.start();
        for (int i = 0; i < COUNT; i++) {
            final BytesMessage msg = producerSession.createBytesMessage();
            msg.writeBytes(payload);
            producer.send(msg);
            if (i != 0 && i % 1000 == 0) {
                LOG.info("Sent msg " + i);
            }
        }
        producerConnection.close();
        broker.stop();
        broker = createBroker(false);
        if (forceRecover) {
            configurePersistence(broker);
        }
        broker.start();

        // Re-create factory after broker restart (port may have changed)
        factory = createConnectionFactory();
        consumerConnection = factory.createConnection();
        consumerConnection.setClientID(CONSUMER_NAME);
        consumerConnection.start();
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        consumer = consumerSession.createDurableSubscriber(topic, CONSUMER_NAME);
        for (int i = 0; i < COUNT; i++) {
            final Message msg = consumer.receive(10000);
            assertNotNull("Missing message: " + i, msg);
            if (i != 0 && i % 1000 == 0) {
                LOG.info("Received msg " + i);
            }

        }
        consumerConnection.close();

    }

    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker(true);
        }

        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    protected Topic creatTopic(final Session s, final String destinationName) throws JMSException {
        return s.createTopic(destinationName);
    }

    /**
     * Factory method to create a new broker
     *
     * @throws Exception
     */
    protected BrokerService createBroker(final boolean deleteStore) throws Exception {
        final BrokerService answer = new BrokerService();
        configureBroker(answer, deleteStore);
        return answer;
    }

    protected void configureBroker(final BrokerService answer, final boolean deleteStore) throws Exception {
        answer.setDeleteAllMessagesOnStartup(deleteStore);
        final KahaDBStore kaha = new KahaDBStore();
        final File directory = new File("target/activemq-data/kahadb");
        if (deleteStore) {
            IOHelper.deleteChildren(directory);
        }
        kaha.setDirectory(directory);

        answer.setPersistenceAdapter(kaha);
        answer.addConnector(bindAddress);
        answer.setUseShutdownHook(false);
        answer.setAdvisorySupport(false);
        answer.setDedicatedTaskRunner(useDedicatedTaskRunner);
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        final String connectUri = broker.getTransportConnectors().get(0).getConnectUri().toString();
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectUri);
        factory.setUseDedicatedTaskRunner(useDedicatedTaskRunner);
        return factory;
    }

    public static Test suite() {
        return suite(DurableConsumerTest.class);
    }

    public static void main(final String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
