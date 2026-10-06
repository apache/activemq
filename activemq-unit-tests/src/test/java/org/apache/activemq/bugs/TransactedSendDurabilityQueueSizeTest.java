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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Confirms a transacted producer's durability is governed by the synchronous
 * commit, not by the individual send. Individual transacted sends go to the
 * broker one-way (ActiveMQSession routes a send with a transaction id through
 * asyncSendPacket), so there is no per-send response. But the commit is a
 * synchronous request that returns success only after the transaction is
 * durably stored and throws on failure, so the producer always learns the
 * outcome and can retry the whole transaction.
 *
 * This is why the fire-and-forget loss window that applies to a non-transacted
 * useAsyncSend producer (a send whose deferred store add fails is not reported
 * back) does not apply to a transacted producer: commit is the checkpoint.
 *
 * The cursor cache is left enabled (concurrentStoreAndDispatch), matching the
 * configuration the duplicate-send fix targets, so the transacted path runs
 * with the async store machinery in play.
 */
@Category(ParallelTest.class)
public class TransactedSendDurabilityQueueSizeTest {

    private static final Logger LOG = LoggerFactory.getLogger(TransactedSendDurabilityQueueSizeTest.class);
    private static final String QUEUE_NAME = "TEST.TX.SEND.DURABILITY";

    private BrokerService broker;
    private Connection connection;
    private File dataDir;

    @Before
    public void setUp() throws Exception {
        var baseDir = new File(IOHelper.getDefaultDataDirectory());
        Files.createDirectories(baseDir.toPath());
        dataDir = Files.createTempDirectory(baseDir.toPath(), "TxSendDurability-").toFile();
        dataDir.deleteOnExit();

        broker = new BrokerService();
        broker.setDataDirectoryFile(dataDir);
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 1024 * 1024);

        var pa = new KahaDBPersistenceAdapter();
        pa.setDirectory(new File(dataDir, "kahadb"));
        broker.setPersistenceAdapter(pa);

        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        var factory = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri());
        connection = factory.createConnection();
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.deleteAllMessages();
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60_000)
    public void testCommittedTransactedSendIsDurableAndDeliveredOnce() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();

        try (var txSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             var producer = txSession.createProducer(dest)) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.send(txSession.createTextMessage("tx-msg"));

            // Before commit the message is held in the transaction: not counted,
            // not stored, not deliverable.
            assertEquals("uncommitted transacted send must not be counted", 0, stats.getMessages().getCount());
            assertEquals("uncommitted transacted send must not be in the store",
                    0, queue.getMessageStore().getMessageCount());

            txSession.commit();
        }

        // commit() returned, so the message is durable now.
        assertTrue("committed message should be counted",
                Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));
        assertEquals("committed message should be in the store", 1, queue.getMessageStore().getMessageCount());

        // Delivered exactly once, store and counter drain to zero.
        var received = 0;
        try (var consumeSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = consumeSession.createConsumer(dest)) {
            jakarta.jms.Message m;
            while ((m = consumer.receive(2000)) != null) {
                LOG.info("received {}", ((TextMessage) m).getText());
                received++;
            }
        }
        assertEquals("the committed message must be delivered exactly once", 1, received);
        assertTrue("store should drain to empty",
                Wait.waitFor(() -> {
                    try {
                        return queue.getMessageStore().getMessageCount() == 0;
                    } catch (Exception e) {
                        return false;
                    }
                }, 5000, 100));
        assertTrue("queueSize should settle to 0",
                Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100));
    }

    @Test(timeout = 60_000)
    public void testRolledBackTransactedSendIsNotStoredOrDelivered() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();

        try (var txSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             var producer = txSession.createProducer(dest)) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.send(txSession.createTextMessage("rolled-back-msg"));
            txSession.rollback();
        }

        // A rolled-back transacted send leaves nothing: the producer controlled
        // the outcome synchronously, so there is no stranded or phantom message.
        try (var consumeSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = consumeSession.createConsumer(dest)) {
            assertNull("rolled-back message must not be delivered", consumer.receive(1500));
        }
        assertEquals("rolled-back message must not be counted", 0, stats.getMessages().getCount());
        assertEquals("rolled-back message must not be in the store",
                0, queue.getMessageStore().getMessageCount());
    }
}
