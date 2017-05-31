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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// https://issues.apache.org/jira/browse/AMQ-4262
public class TransactedStoreUsageSuspendResumeTest {
    private static final Logger LOG = LoggerFactory.getLogger(TransactedStoreUsageSuspendResumeTest.class);

    private static final int MAX_MESSAGES = 10000;

    private static final String QUEUE_NAME = "test.queue";

    private BrokerService broker;

    private final CountDownLatch messagesReceivedCountDown = new CountDownLatch(MAX_MESSAGES);
    private final CountDownLatch messagesSentCountDown = new CountDownLatch(MAX_MESSAGES);
    private final CountDownLatch consumerStartLatch = new CountDownLatch(1);

    private class ConsumerThread extends Thread {

        @Override
        public void run() {
            try {

                consumerStartLatch.await(30, TimeUnit.SECONDS);

                ConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                Connection connection = factory.createConnection();
                connection.start();
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

                // wait for producer to stop
                long currentSendCount;
                do {
                    currentSendCount = messagesSentCountDown.getCount();
                    TimeUnit.SECONDS.sleep(5);
                } while (currentSendCount != messagesSentCountDown.getCount());

                LOG.info("Starting consumer at: " + currentSendCount);

                MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

                do {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        session.commit();
                        messagesReceivedCountDown.countDown();
                    }
                    if (messagesReceivedCountDown.getCount() % 500 == 0) {
                        LOG.info("remaining to receive: " + messagesReceivedCountDown.getCount());
                    }
                } while (messagesReceivedCountDown.getCount() != 0);
                session.commit();
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @Before
    public void setup() throws Exception {

        // investigate liner gc issue - store usage not getting released
        org.apache.log4j.Logger.getLogger(MessageDatabase.class).setLevel(Level.TRACE);

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);

        KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        kahaDB.setJournalMaxFileLength(256 * 1024);
        kahaDB.setCleanupInterval(10*1000);
        kahaDB.setCompactAcksAfterNoGC(5);
        broker.setPersistenceAdapter(kahaDB);

        broker.getSystemUsage().getStoreUsage().setLimit(7*1024*1024);

        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testTransactedStoreUsageSuspendResume() throws Exception {

        ConsumerThread thread = new ConsumerThread();
        thread.start();
        ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
        sendExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendMessages();
                } catch (Exception ignored) {
                }
            }
        });
        sendExecutor.shutdown();
        sendExecutor.awaitTermination(10, TimeUnit.MINUTES);

        boolean allMessagesReceived = messagesReceivedCountDown.await(10, TimeUnit.MINUTES);
        if (!allMessagesReceived) {
            LOG.info("Giving up - not all received on time...");
            LOG.info("System Mem Usage: " + broker.getSystemUsage().getMemoryUsage());
            LOG.info("System Store Usage: " +broker.getSystemUsage().getStoreUsage());
            LOG.info("Producer sent: " + messagesSentCountDown.getCount());
            LOG.info("Consumer remaining to receive: " + messagesReceivedCountDown.getCount());
            TestSupport.dumpAllThreads("StuckConsumer!");
        }
        assertTrue("Got all messages: " + messagesReceivedCountDown, allMessagesReceived);

        // give consumers a chance to exit gracefully
        TimeUnit.SECONDS.sleep(5);
    }

    private void sendMessages() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setAlwaysSyncSend(true);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination queue = session.createQueue(QUEUE_NAME);
        Destination retainQueue = session.createQueue(QUEUE_NAME + "-retain");
        MessageProducer producer = session.createProducer(null);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(new byte[10]);

        for (int i=0; i<1240; i++) {
            // mostly fill the store with retained messages
            // so consumer only has a small bit of store usage to work with
            producer.send(retainQueue, message);
            session.commit();
        }

        // issue with gc and linear store usage
        // some daylight in needed between retainQ and regularQ to free up the store
        // log4j.logger.org.apache.activemq.store.kahadb.MessageDatabase=TRACE
        Destination shortRetainQueue = session.createQueue(QUEUE_NAME + "-retain-short");
        for (int i=0; i<1240; i++) {
            producer.send(shortRetainQueue, message);
            session.commit();
        }

        MessageConsumer consumer = session.createConsumer(shortRetainQueue);
        for (int i=0; i<1240; i++) {
            consumer.receive(4000);
            session.commit();
        }

        LOG.info("Done with retain q. Mem Usage: " + broker.getSystemUsage().getMemoryUsage());
        LOG.info("Done with retain q. Store Usage: " +broker.getSystemUsage().getStoreUsage());
        consumerStartLatch.countDown();
        for (int i = 0; i < MAX_MESSAGES; i++) {
            producer.send(queue,  message);
            if (i>0 && i%20 == 0) {
                session.commit();
            }
            messagesSentCountDown.countDown();
            if (i>0 && i%500 == 0) {
                LOG.info("Sent : " + i);
            }

        }
        session.commit();
        producer.close();
        session.close();
        connection.close();
    }

}
