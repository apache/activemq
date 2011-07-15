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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AMQ2982Test {

    private static final int MAX_MESSAGES = 500;

    private static final String QUEUE_NAME = "test.queue";

    private BrokerService broker;

    private final CountDownLatch messageCountDown = new CountDownLatch(MAX_MESSAGES);

    private CleanableKahaDBStore kahaDB;

    private static class CleanableKahaDBStore extends KahaDBStore {
        // make checkpoint cleanup accessible
        public void forceCleanup() throws IOException {
            checkpointCleanup(true);
        }

        public int getFileMapSize() throws IOException {
            // ensure save memory publishing, use the right lock
            indexLock.readLock().lock();
            try {
                return getJournalManager().getFileMap().size();
            } finally {
                indexLock.readLock().unlock();
            }
        }
    }

    @Before
    public void setup() throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);

        kahaDB = new CleanableKahaDBStore();
        kahaDB.setJournalMaxFileLength(256 * 1024);
        broker.setPersistenceAdapter(kahaDB);

        broker.start();
        broker.waitUntilStarted();
    }

    private Connection registerDLQMessageListener() throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session
                .createQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                messageCountDown.countDown();
            }
        });

        return connection;
    }

    class ConsumerThread extends Thread {

        @Override
        public void run() {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");

                RedeliveryPolicy policy = new RedeliveryPolicy();
                policy.setMaximumRedeliveries(0);
                policy.setInitialRedeliveryDelay(100);
                policy.setUseExponentialBackOff(false);

                factory.setRedeliveryPolicy(policy);

                Connection connection = factory.createConnection();
                connection.start();
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
                do {
                    Message message = consumer.receive(300);
                    if (message != null) {
                        session.rollback();
                    }
                } while (messageCountDown.getCount() != 0);
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    private void sendMessages() throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < MAX_MESSAGES; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(new byte[1000]);
            producer.send(message);
        }
        producer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testNoStickyKahaDbLogFilesOnLocalTransactionRollback() throws Exception {

        Connection dlqConnection = registerDLQMessageListener();

        ConsumerThread thread = new ConsumerThread();
        thread.start();

        sendMessages();

        thread.join(60 * 1000);
        assertFalse(thread.isAlive());

        dlqConnection.close();

        kahaDB.forceCleanup();

        assertEquals("only one active KahaDB log file after cleanup is expected", 1, kahaDB.getFileMapSize());
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

}
