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
import javax.jms.TopicSubscriber;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
//import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.Wait;
//import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentProducerDurableConsumerTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentProducerDurableConsumerTest.class);
    private int consumerCount = 5;
    BrokerService broker;
    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());
    protected Map<MessageConsumer, TimedMessageListener> consumers = new HashMap<MessageConsumer, TimedMessageListener>();
    protected MessageIdList allMessagesList = new MessageIdList();
    private int messageSize = 1024;

    public void initCombosForTestSendRateWithActivatingConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, /* too slow for hudson - PersistenceAdapterChoice.JDBC,*/  PersistenceAdapterChoice.MEM});
    }

    public void testSendRateWithActivatingConsumers() throws Exception {
        final Destination destination = createDestination();
        final ConnectionFactory factory = createConnectionFactory();
        startInactiveConsumers(factory, destination);

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = createMessageProducer(session, destination);

        // preload the durable consumers
        double[] inactiveConsumerStats = produceMessages(destination, 500, 10, session, producer, null);
        LOG.info("With inactive consumers: ave: " + inactiveConsumerStats[1]
                + ", max: " + inactiveConsumerStats[0] + ", multiplier: " + (inactiveConsumerStats[0]/inactiveConsumerStats[1]));

        // periodically start a durable sub that has a backlog
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
                        consumer = createDurableSubscriber(factory.createConnection(), destination, "consumer" + (i + 1));
                        LOG.info("Created consumer " + consumer);
                        consumer.setMessageListener(listener);
                        consumers.put(consumer, listener);
                    }
                } catch (Exception e) {
                    LOG.error("failed to start consumer", e);
                }
            }
        });


        double[] statsWithActive = produceMessages(destination, 500, 10, session, producer, addConsumerSignal);

        LOG.info(" with concurrent activate, ave: " + statsWithActive[1] + ", max: " + statsWithActive[0] + ", multiplier: " + (statsWithActive[0]/ statsWithActive[1]));

        while(consumers.size() < consumersToActivate) {
            TimeUnit.SECONDS.sleep(2);
        }

        long timeToFirstAccumulator = 0;
        for (TimedMessageListener listener : consumers.values()) {
            long time = listener.getFirstReceipt();
            timeToFirstAccumulator += time;
            LOG.info("Time to first " + time);
        }
        LOG.info("Ave time to first message =" + timeToFirstAccumulator/consumers.size());

        for (TimedMessageListener listener : consumers.values()) {
            LOG.info("Ave batch receipt time: " + listener.waitForReceivedLimit(10000) + " max receipt: " + listener.maxReceiptTime);
        }

        //assertTrue("max (" + statsWithActive[0] + ") within reasonable multiplier of ave (" + statsWithActive[1] + ")",
        //        statsWithActive[0] < 5 * statsWithActive[1]);

        // compare no active to active
        LOG.info("Ave send time with active: " + statsWithActive[1]
                + " as multiplier of ave with none active: " + inactiveConsumerStats[1]
                + ", multiplier=" + (statsWithActive[1]/inactiveConsumerStats[1]));

        assertTrue("Ave send time with active: " + statsWithActive[1]
                + " within reasonable multpler of ave with none active: " + inactiveConsumerStats[1]
                + ", multiplier " + (statsWithActive[1]/inactiveConsumerStats[1]),
                statsWithActive[1] < 15 * inactiveConsumerStats[1]);
    }


    public void x_initCombosForTestSendWithInactiveAndActiveConsumers() throws Exception {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC});
    }

    public void x_testSendWithInactiveAndActiveConsumers() throws Exception {
        Destination destination = createDestination();
        ConnectionFactory factory = createConnectionFactory();
        startInactiveConsumers(factory, destination);

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


    private void startInactiveConsumers(ConnectionFactory factory, Destination destination) throws Exception {
        // create off line consumers
        startConsumers(factory, destination);
        for (Connection connection: connections) {
            connection.close();
        }
        connections.clear();
        consumers.clear();
    }


    protected void startConsumers(ConnectionFactory factory, Destination dest) throws Exception {
        MessageConsumer consumer;
        for (int i = 0; i < consumerCount; i++) {
            TimedMessageListener list = new TimedMessageListener();
            consumer = createDurableSubscriber(factory.createConnection(), dest, "consumer" + (i + 1));
            consumer.setMessageListener(list);
            consumers.put(consumer, list);
        }
    }

    protected TopicSubscriber createDurableSubscriber(Connection conn, Destination dest, String name) throws Exception {
        conn.setClientID(name);
        connections.add(conn);
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TopicSubscriber consumer = sess.createDurableSubscriber((javax.jms.Topic)dest, name);

        return consumer;
    }

    /**
     * @return max and ave send time
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
        topic = true;
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
        brokerService.addConnector("tcp://0.0.0.0:61616");
        brokerService.setDeleteAllMessagesOnStartup(true);

        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(true);
        policy.setMaxPageSize(500);

        StorePendingDurableSubscriberMessageStoragePolicy durableSubPending =
                new StorePendingDurableSubscriberMessageStoragePolicy();
        durableSubPending.setImmediatePriorityDispatch(true);
        durableSubPending.setUseCache(true);
        policy.setPendingDurableSubscriberPolicy(durableSubPending);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        brokerService.setDestinationPolicy(policyMap);

        if (false) {
              // external mysql works a lot faster
              //
//            JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
//            BasicDataSource ds = new BasicDataSource();
//            com.mysql.jdbc.Driver d = new com.mysql.jdbc.Driver();
//            ds.setDriverClassName("com.mysql.jdbc.Driver");
//            ds.setUrl("jdbc:mysql://localhost/activemq?relaxAutoCommit=true");
//            ds.setMaxActive(200);
//            ds.setUsername("root");
//            ds.setPassword("");
//            ds.setPoolPreparedStatements(true);
//            jdbc.setDataSource(ds);
//            brokerService.setPersistenceAdapter(jdbc);

/* add mysql bits to the pom in the testing dependencies
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.10</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>commons-dbcp</groupId>
    <artifactId>commons-dbcp</artifactId>
    <version>1.2.2</version>
    <scope>test</scope>
</dependency>
             */
        } else {
            setDefaultPersistenceAdapter(brokerService);
        }
        return brokerService;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(1);
        factory.setPrefetchPolicy(prefetchPolicy);

        factory.setDispatchAsync(true);
        return factory;
    }

    public static Test suite() {
        return suite(ConcurrentProducerDurableConsumerTest.class);
    }

    class TimedMessageListener implements MessageListener {
        final int batchSize = 1000;
        CountDownLatch firstReceiptLatch = new CountDownLatch(1);
        long mark = System.currentTimeMillis();
        long firstReceipt = 0l;
        long receiptAccumulator = 0;
        long batchReceiptAccumulator = 0;
        long maxReceiptTime = 0;
        AtomicLong count = new AtomicLong(0);
        Map<Integer, MessageIdList> messageLists = new ConcurrentHashMap<Integer, MessageIdList>(new HashMap<Integer, MessageIdList>());

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
            MessageId current = new MessageId();
            for (MessageIdList priorityList : messageLists.values()) {
                MessageId previous = null;
                for (String id : priorityList.getMessageIds()) {
                    current.setValue(id);
                    if (previous == null) {
                        previous = current.copy();
                    } else {
                        if (current.getProducerSequenceId() - 1 != previous.getProducerSequenceId() &&
                            current.getProducerSequenceId() - 10 !=  previous.getProducerSequenceId()) {
                                return "Missing next after: " + previous + ", got: " + current;
                        } else {
                            previous = current.copy();
                        }
                    }
                }
            }
            return null;
        }
    }

}
