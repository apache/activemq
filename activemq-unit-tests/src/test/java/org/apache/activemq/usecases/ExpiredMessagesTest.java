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

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.activemq.TestSupport.getDestination;
import static org.apache.activemq.TestSupport.getDestinationConsumers;
import static org.apache.activemq.TestSupport.getDestinationStatistics;

public class ExpiredMessagesTest extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ExpiredMessagesTest.class);

    BrokerService broker;
    Connection connection;
    Session session;
    MessageProducer producer;
    MessageConsumer consumer;
    private ActiveMQDestination dlqDestination = new ActiveMQQueue("ActiveMQ.DLQ");
    private boolean useTextMessage = true;
    private boolean useVMCursor = true;
    private boolean deleteAllMessages = true;
    private boolean usePrefetchExtension = true;
    private String brokerUri;

    public static Test suite() {
        return suite(ExpiredMessagesTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected void setUp() throws Exception {

    }

    @Override
    protected void tearDown() throws Exception {
        if (null != producer) {
            producer.close();
        }
        if (null != consumer) {
            consumer.close();
        }
        session.close();
        connection.stop();
        broker.stop();
        broker.waitUntilStopped();
    }

    public void testExpiredMessages() throws Exception {
        final ActiveMQDestination destination = new ActiveMQQueue("test");
        final int numMessagesToSend = 10000;

        buildBroker(destination);

        final DestinationStatistics view = verifyMessageExpirationOnDestination(destination, numMessagesToSend);

        verifyDestinationDlq(destination, numMessagesToSend, view);
    }

    public void testClientAckInflight_onTopic_withPrefetchExtension() throws Exception {
        usePrefetchExtension = true;
        doTestClientAckInflight_onTopic_checkPrefetchExtension();
    }

    public void testClientAckInflight_onTopic_withOutPrefetchExtension() throws Exception {
        usePrefetchExtension = false;
        doTestClientAckInflight_onTopic_checkPrefetchExtension();
    }

    public void doTestClientAckInflight_onTopic_checkPrefetchExtension() throws Exception {
        final ActiveMQDestination destination = new ActiveMQTopic("test");
        buildBroker(destination);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "failover://"+brokerUri);
        ActiveMQPrefetchPolicy prefetchTwo = new ActiveMQPrefetchPolicy();
        prefetchTwo.setAll(6);
        factory.setPrefetchPolicy(prefetchTwo);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        produce(10, destination);

        Message m = null;
        for (int i=0; i<5; i++) {
            m = consumer.receive(4000);
        }
        assertNotNull(m);

        final List<Subscription> subscriptions = getDestinationConsumers(broker, destination);

        assertTrue("prefetch extension was not incremented",
                subscriptions.stream().
                        filter(s -> s instanceof TopicSubscription).
                        mapToInt(s -> ((TopicSubscription)s).getPrefetchExtension().get()).
                        allMatch(e -> usePrefetchExtension ? e > 1 : e == 0));

        m.acknowledge();

        assertTrue("prefetch extension was not incremented",
                subscriptions.stream().
                        filter(s -> s instanceof TopicSubscription).
                        mapToInt(s -> ((TopicSubscription)s).getPrefetchExtension().get()).
                        allMatch(e -> e == 0));

    }

    private void produce(int num, ActiveMQDestination destination) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "failover://"+brokerUri);
        Connection connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        int i = 0;
        while (i++ < num) {
            Message message = useTextMessage ? session
                    .createTextMessage("test") : session
                    .createObjectMessage("test");
            producer.send(message);
        }
        connection.close();
    }


    private void buildBroker(ActiveMQDestination destination) throws Exception {
        broker = createBroker(deleteAllMessages, usePrefetchExtension, 100, destination);
        brokerUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    public void testRecoverExpiredMessages() throws Exception {
        final ActiveMQDestination destination = new ActiveMQQueue("test");

        buildBroker(destination);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "failover://"+brokerUri);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        producer.setTimeToLive(2000);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Thread producingThread = new Thread("Producing Thread") {
            @Override
            public void run() {
                try {
                    int i = 0;
                    while (i++ < 1000) {
                        Message message = useTextMessage ? session
                                .createTextMessage("test") : session
                                .createObjectMessage("test");
                        producer.send(message);
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };

        producingThread.start();
        producingThread.join();

        DestinationStatistics view = getDestinationStatistics(broker, destination);
        LOG.info("Stats: size: " + view.getMessages().getCount() + ", enqueues: "
                + view.getEnqueues().getCount() + ", dequeues: "
                + view.getDequeues().getCount() + ", dispatched: "
                + view.getDispatched().getCount() + ", inflight: "
                + view.getInflight().getCount() + ", expiries: "
                + view.getExpired().getCount());

        LOG.info("stopping broker");
        broker.stop();
        broker.waitUntilStopped();

        Thread.sleep(5000);

        LOG.info("recovering broker");
        final boolean deleteAllMessages = false;
        final boolean usePrefetchExtension = true;
        broker = createBroker(deleteAllMessages, usePrefetchExtension, 5000, destination);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                DestinationStatistics view = getDestinationStatistics(broker, destination);
                LOG.info("Stats: size: " + view.getMessages().getCount() + ", enqueues: "
                        + view.getEnqueues().getCount() + ", dequeues: "
                        + view.getDequeues().getCount() + ", dispatched: "
                        + view.getDispatched().getCount() + ", inflight: "
                        + view.getInflight().getCount() + ", expiries: "
                        + view.getExpired().getCount());

                return view.getMessages().getCount() == 0;
            }
        });

        view = getDestinationStatistics(broker, destination);
        assertEquals("Expect empty queue, QueueSize: ", 0, view.getMessages().getCount());
        assertEquals("all dequeues were expired", view.getDequeues().getCount(), view.getExpired().getCount());
    }

    private DestinationStatistics verifyMessageExpirationOnDestination(ActiveMQDestination destination, final int numMessagesToSend) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        producer.setTimeToLive(100);
        consumer = session.createConsumer(destination);
        connection.start();
        final AtomicLong received = new AtomicLong();

        Thread consumerThread = new Thread("Consumer Thread") {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                try {
                    long end = System.currentTimeMillis();
                    while (end - start < 3000) {
                        if (consumer.receive(1000) != null) {
                            received.incrementAndGet();
                        }
                        Thread.sleep(100);
                        end = System.currentTimeMillis();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };

        consumerThread.start();

        Thread producingThread = new Thread("Producing Thread") {
            @Override
            public void run() {
                try {
                    int i = 0;
                    while (i++ < numMessagesToSend) {
                        producer.send(session.createTextMessage("test"));
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };

        producingThread.start();

        consumerThread.join();
        producingThread.join();

        final DestinationStatistics view = getDestinationStatistics(broker, destination);

        // wait for all to inflight to expire
        assertTrue("all inflight messages expired ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return view.getInflight().getCount() == 0;
            }
        }));
        assertEquals("Wrong inFlightCount: ", 0, view.getInflight().getCount());

        LOG.info("Stats: received: "  + received.get() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());

        // wait for all sent to get delivered and expire
        assertTrue("all sent messages expired ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                long oldEnqueues = view.getEnqueues().getCount();
                Thread.sleep(200);
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getDequeues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                        + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
                return oldEnqueues == view.getEnqueues().getCount();
            }
        }, 60*1000));


        LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());

        assertTrue("got at least what did not expire", received.get() >= view.getDequeues().getCount() - view.getExpired().getCount());

        assertTrue("all messages expired - queue size gone to zero " + view.getMessages().getCount(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                        + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
                return view.getMessages().getCount() == 0;
            }
        }));
        return view;
    }

    private void verifyDestinationDlq(ActiveMQDestination destination, int numMessagesToSend, DestinationStatistics view) throws Exception {
        final long expiredBeforeEnqueue = numMessagesToSend - view.getEnqueues().getCount();
        final long totalExpiredCount = view.getExpired().getCount() + expiredBeforeEnqueue;

        final DestinationStatistics dlqView = getDestinationStatistics(broker, dlqDestination);
        LOG.info("DLQ stats: size= " + dlqView.getMessages().getCount() + ", enqueues: " + dlqView.getDequeues().getCount() + ", dequeues: " + dlqView.getDequeues().getCount()
                + ", dispatched: " + dlqView.getDispatched().getCount() + ", inflight: " + dlqView.getInflight().getCount() + ", expiries: " + dlqView.getExpired().getCount());

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return totalExpiredCount == dlqView.getMessages().getCount();
            }
        });
        assertEquals("dlq contains all expired", totalExpiredCount, dlqView.getMessages().getCount());

        // memory check
        assertEquals("memory usage is back to duck egg", 0, getDestination(broker, destination).getMemoryUsage().getPercentUsage());
        assertTrue("memory usage is increased ", 0 < getDestination(broker, dlqDestination).getMemoryUsage().getUsage());

        // verify DLQ
        MessageConsumer dlqConsumer = createDlqConsumer(connection);
        final DLQListener dlqListener = new DLQListener();
        dlqConsumer.setMessageListener(dlqListener);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return totalExpiredCount == dlqListener.count;
            }
        }, 60 * 1000);

        assertEquals("dlq returned all expired", dlqListener.count, totalExpiredCount);
    }

    class DLQListener implements MessageListener {

        int count = 0;

        @Override
        public void onMessage(Message message) {
            count++;
        }

    };

    private MessageConsumer createDlqConsumer(Connection connection) throws Exception {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(dlqDestination);
    }

    public void initCombosForTestRecoverExpiredMessages() {
        addCombinationValues("useVMCursor", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    private BrokerService createBroker(boolean deleteAllMessages, boolean usePrefetchExtension, long expireMessagesPeriod, ActiveMQDestination destination) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setDestinations(new ActiveMQDestination[]{destination});
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());

        PolicyEntry defaultPolicy = new PolicyEntry();
        if (useVMCursor) {
            defaultPolicy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        }
        defaultPolicy.setExpireMessagesPeriod(expireMessagesPeriod);
        defaultPolicy.setMaxExpirePageSize(1200);
        defaultPolicy.setUsePrefetchExtension(usePrefetchExtension);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(policyMap);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

}
