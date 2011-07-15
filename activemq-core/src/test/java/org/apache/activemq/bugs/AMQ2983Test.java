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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class AMQ2983Test {

    private static final int MAX_CONSUMER = 10;

    private static final int MAX_MESSAGES = 2000;

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

    private class ConsumerThread extends Thread {

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
                Connection connection = factory.createConnection();
                connection.start();
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
                do {
                    Message message = consumer.receive(200);
                    if (message != null) {
                        session.commit();
                        messageCountDown.countDown();
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

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testNoStickyKahaDbLogFilesOnConcurrentTransactionalConsumer() throws Exception {

        List<Thread> consumerThreads = new ArrayList<Thread>();
        for (int i = 0; i < MAX_CONSUMER; i++) {
            ConsumerThread thread = new ConsumerThread();
            thread.start();
            consumerThreads.add(thread);
        }
        sendMessages();

        boolean allMessagesReceived = messageCountDown.await(60, TimeUnit.SECONDS);
        assertTrue(allMessagesReceived);

        for (Thread thread : consumerThreads) {
            thread.join(TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS));
            assertFalse(thread.isAlive());
        }
        kahaDB.forceCleanup();
        assertEquals("Expect only one active KahaDB log file after cleanup", 1, kahaDB.getFileMapSize());
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
            message.writeBytes(new byte[200]);
            producer.send(message);
        }
        producer.close();
        session.close();
        connection.close();
    }

}
