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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentProducerQueueConsumerTest extends TestSupport
{
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentProducerQueueConsumerTest.class);

    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());
    protected Map<MessageConsumer, TimedMessageListener> consumers =
        new HashMap<MessageConsumer, TimedMessageListener>();
    protected MessageIdList allMessagesList = new MessageIdList();

    private BrokerService broker;
    private final int consumerCount = 5;
    private final int messageSize = 1024;
    private final int NUM_MESSAGES = 500;
    private final int ITERATIONS = 10;

    private int expectedQueueDeliveries = 0;

    public void initCombosForTestSendRateWithActivatingConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB,
                             /* too slow for hudson - PersistenceAdapterChoice.JDBC,*/
                             PersistenceAdapterChoice.MEM});
    }

    public void testSendRateWithActivatingConsumers() throws Exception {
        final Destination destination = createDestination();
        final ConnectionFactory factory = createConnectionFactory();

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = createMessageProducer(session, destination);

        // preload the queue before adding any consumers
        double[] noConsumerStats = produceMessages(destination, NUM_MESSAGES, ITERATIONS, session, producer, null);
        LOG.info("With no consumers: ave: " + noConsumerStats[1] + ", max: " +
                 noConsumerStats[0] + ", multiplier: " + (noConsumerStats[0]/noConsumerStats[1]));
        expectedQueueDeliveries = NUM_MESSAGES * ITERATIONS;

        // periodically start a queue consumer
        final int consumersToActivate = 5;
        final Object addConsumerSignal = new Object();
        Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ActivateConsumer" + this);
            }
        }).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    MessageConsumer consumer = null;
                    for (int i = 0; i < consumersToActivate; i++) {
                        LOG.info("Waiting for add signal from producer...");
                        synchronized (addConsumerSignal) {
                            addConsumerSignal.wait(30 * 60 * 1000);
                        }
                        TimedMessageListener listener = new TimedMessageListener();
                        consumer = createConsumer(factory.createConnection(), destination);
                        LOG.info("Created consumer " + consumer);
                        consumer.setMessageListener(listener);
                        consumers.put(consumer, listener);
                    }
                } catch (Exception e) {
                    LOG.error("failed to start consumer", e);
                }
            }
        });

        // Collect statistics when there are active consumers.
        double[] statsWithActive =
            produceMessages(destination, NUM_MESSAGES, ITERATIONS, session, producer, addConsumerSignal);
        expectedQueueDeliveries += NUM_MESSAGES * ITERATIONS;

        LOG.info(" with concurrent activate, ave: " + statsWithActive[1] + ", max: " +
                 statsWithActive[0] + ", multiplier: " + (statsWithActive[0]/ statsWithActive[1]));

        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return consumers.size() == consumersToActivate;
            }
        }));

        long timeToFirstAccumulator = 0;
        for (TimedMessageListener listener : consumers.values()) {
            long time = listener.getFirstReceipt();
            timeToFirstAccumulator += time;
            LOG.info("Time to first " + time);
        }
        LOG.info("Ave time to first message =" + timeToFirstAccumulator/consumers.size());

        for (TimedMessageListener listener : consumers.values()) {
            LOG.info("Ave batch receipt time: " + listener.waitForReceivedLimit(expectedQueueDeliveries) +
                     " max receipt: " + listener.maxReceiptTime);
        }

        // compare no active to active
        LOG.info("Ave send time with active: " + statsWithActive[1]
                + " as multiplier of ave with none active: " + noConsumerStats[1]
                + ", multiplier=" + (statsWithActive[1]/noConsumerStats[1]));

        assertTrue("Ave send time with active: " + statsWithActive[1]
                + " within reasonable multpler of ave with none active: " + noConsumerStats[1]
                + ", multiplier " + (statsWithActive[1]/noConsumerStats[1]),
                statsWithActive[1] < 15 * noConsumerStats[1]);
    }

    public void x_initCombosForTestSendWithInactiveAndActiveConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.LevelDB,
                             /* too slow for hudson - PersistenceAdapterChoice.JDBC,*/
                             PersistenceAdapterChoice.MEM});
    }

    public void x_testSendWithInactiveAndActiveConsumers() throws Exception {
        Destination destination = createDestination();
        ConnectionFactory factory = createConnectionFactory();

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        final int toSend = 100;
        final int numIterations = 5;

        double[] noConsumerStats = produceMessages(destination, toSend, numIterations, session, producer, null);

        startConsumers(factory, destination);
        LOG.info("Activated consumer");

        double[] withConsumerStats = produceMessages(destination, toSend, numIterations, session, producer, null);

        LOG.info("With consumer: " + withConsumerStats[1] + " , with noConsumer: " + noConsumerStats[1]
                + ", multiplier: " + (withConsumerStats[1]/noConsumerStats[1]));
        final int reasonableMultiplier = 15; // not so reasonable but improving
        assertTrue("max X times as slow with consumer: " + withConsumerStats[1] + ", with no Consumer: "
                + noConsumerStats[1] + ", multiplier: " + (withConsumerStats[1]/noConsumerStats[1]),
                withConsumerStats[1] < noConsumerStats[1] * reasonableMultiplier);

        final int toReceive = toSend * numIterations * consumerCount * 2;
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("count: " + allMessagesList.getMessageCount());
                return toReceive == allMessagesList.getMessageCount();
            }
        }, 60 * 1000);

        assertEquals("got all messages", toReceive, allMessagesList.getMessageCount());
    }

    private MessageProducer createMessageProducer(Session session, Destination destination) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        return producer;
    }

    protected void startConsumers(ConnectionFactory factory, Destination dest) throws Exception {
        MessageConsumer consumer;
        for (int i = 0; i < consumerCount; i++) {
            TimedMessageListener list = new TimedMessageListener();
            consumer = createConsumer(factory.createConnection(), dest);
            consumer.setMessageListener(list);
            consumers.put(consumer, list);
        }
    }

    protected MessageConsumer createConsumer(Connection conn, Destination dest) throws Exception {
        connections.add(conn);
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(dest);

        return consumer;
    }

    /**
     * @return max and average send time
     * @throws Exception
     */
    private double[] produceMessages(Destination destination,
                                     final int toSend,
                                     final int numIterations,
                                     Session session,
                                     MessageProducer producer,
                                     Object addConsumerSignal) throws Exception {
        long start;
        long count = 0;
        double batchMax = 0, max = 0, sum = 0;

        for (int i=0; i<numIterations; i++) {
            start = System.currentTimeMillis();
            for (int j=0; j < toSend; j++) {
                long singleSendstart = System.currentTimeMillis();
                TextMessage msg = createTextMessage(session, "" + j);
                // rotate
                int priority = ((int)count%10);
                producer.send(msg, DeliveryMode.PERSISTENT, priority, 0);
                max = Math.max(max, (System.currentTimeMillis() - singleSendstart));
                if (++count % 500 == 0) {
                    if (addConsumerSignal != null) {
                        synchronized (addConsumerSignal) {
                            addConsumerSignal.notifyAll();
                            LOG.info("Signalled add consumer");
                        }
                    }
                }
                ;
                if (count % 5000 == 0) {
                    LOG.info("Sent " + count + ", singleSendMax:" + max);
                }

            }
            long duration = System.currentTimeMillis() - start;
            batchMax = Math.max(batchMax, duration);
            sum += duration;
            LOG.info("Iteration " + i + ", sent " + toSend + ", time: "
                    + duration + ", batchMax:" + batchMax + ", singleSendMax:" + max);
        }

        LOG.info("Sent: " + toSend * numIterations + ", batchMax: " + batchMax + " singleSendMax: " + max);
        return new double[]{batchMax, sum/numIterations};
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        TextMessage msg = session.createTextMessage();

        // Pad message text
        if (initText.length() < messageSize) {
            char[] data = new char[messageSize - initText.length()];
            Arrays.fill(data, '*');
            String str = new String(data);
            msg.setText(initText + str);

            // Do not pad message text
        } else {
            msg.setText(initText);
        }

        return msg;
    }

    @Override
    protected void setUp() throws Exception {
        topic = false;
        super.setUp();
        broker = createBroker();
        broker.start();
    }

    @Override
    protected void tearDown() throws Exception {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection conn = iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
        broker.stop();
        allMessagesList.flushMessages();
        consumers.clear();
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setEnableStatistics(false);
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.setDeleteAllMessagesOnStartup(true);

        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(true);
        policy.setMaxPageSize(500);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        brokerService.setDestinationPolicy(policyMap);
        setDefaultPersistenceAdapter(brokerService);

        return brokerService;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            broker.getTransportConnectors().get(0).getPublishableConnectString());
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(1);
        factory.setPrefetchPolicy(prefetchPolicy);

        factory.setDispatchAsync(true);
        return factory;
    }

    public static Test suite() {
        return suite(ConcurrentProducerQueueConsumerTest.class);
    }

    static class TimedMessageListener implements MessageListener {

        static final AtomicLong count = new AtomicLong(0);

        final int batchSize = 1000;
        final CountDownLatch firstReceiptLatch = new CountDownLatch(1);

        long mark = System.currentTimeMillis();
        long firstReceipt = 0l;
        long receiptAccumulator = 0;
        long batchReceiptAccumulator = 0;
        long maxReceiptTime = 0;

        final Map<Integer, MessageIdList> messageLists =
            new ConcurrentHashMap<Integer, MessageIdList>(new HashMap<Integer, MessageIdList>());

        @Override
        public void onMessage(Message message) {
            final long current = System.currentTimeMillis();
            final long duration = current - mark;
            receiptAccumulator += duration;
            int priority = 0;

            try {
                priority = message.getJMSPriority();
            } catch (JMSException ignored) {}

            if (!messageLists.containsKey(priority)) {
                messageLists.put(priority, new MessageIdList());
            }
            messageLists.get(priority).onMessage(message);

            if (count.incrementAndGet() == 1) {
                firstReceipt = duration;
                firstReceiptLatch.countDown();
                LOG.info("First receipt in " + firstReceipt + "ms");
            } else if (count.get() % batchSize == 0) {
                LOG.info("Consumed " + count.get() + " in " + batchReceiptAccumulator + "ms" + ", priority:" + priority);
                batchReceiptAccumulator=0;
            }

            maxReceiptTime = Math.max(maxReceiptTime, duration);
            receiptAccumulator += duration;
            batchReceiptAccumulator += duration;
            mark = current;
        }

        long getMessageCount() {
            return count.get();
        }

        long getFirstReceipt() throws Exception {
            firstReceiptLatch.await(30, TimeUnit.SECONDS);
            return firstReceipt;
        }

        public long waitForReceivedLimit(long limit) throws Exception {
            final long expiry = System.currentTimeMillis() + 30*60*1000;
            while (count.get() < limit) {
                if (System.currentTimeMillis() > expiry) {
                    throw new RuntimeException("Expired waiting for X messages, " + limit);
                }
                TimeUnit.SECONDS.sleep(2);
                String missing = findFirstMissingMessage();
                if (missing != null) {
                    LOG.info("first missing = " + missing);
                    throw new RuntimeException("We have a missing message. " + missing);
                }

            }
            return receiptAccumulator/(limit/batchSize);
        }

        private String findFirstMissingMessage() {
            return null;
        }
    }

}
