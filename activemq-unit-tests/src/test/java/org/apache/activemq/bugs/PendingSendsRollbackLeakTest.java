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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Session;

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
 * Reproduces the pendingSends counter leak on transaction rollback.
 *
 * Queue.doMessageSend() increments the pendingSends counter for every send.
 * On commit, CursorAddSync.afterCommit() -> messageSent() decrements it.
 * On rollback, CursorAddSync.afterRollback() must also decrement it —
 * without that, every rolled-back transactional send permanently inflates
 * pendingSends.
 *
 * The leaked counter makes Queue.singlePendingSend() return false forever,
 * which disables cursor caching via QueueStorePrefetch.canEnableCash().
 * The queue is then stuck in store page-in mode, degrading throughput and
 * amplifying duplicateFromStore events.
 */
@Category(ParallelTest.class)
public class PendingSendsRollbackLeakTest {

    private static final Logger LOG = LoggerFactory.getLogger(PendingSendsRollbackLeakTest.class);
    private static final String QUEUE_NAME = "TEST.PENDING.SENDS.ROLLBACK";
    private static final int ROLLBACK_SEND_COUNT = 5;

    private BrokerService broker;
    private Connection connection;
    private File dataDir;

    @Before
    public void setUp() throws Exception {
        var baseDir = new File(IOHelper.getDefaultDataDirectory());
        Files.createDirectories(baseDir.toPath());
        dataDir = Files.createTempDirectory(baseDir.toPath(), "PendingSendsRollback-").toFile();
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
    public void testRollbackDecrementsPendingSends() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);

        assertEquals("pendingSends should start at 0", 0, getPendingSends(queue));
        assertTrue("singlePendingSend should be true initially", queue.singlePendingSend());

        // Send persistent messages in a transaction, then roll back
        try(var txSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            var producer = txSession.createProducer(dest)) {

            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            for (int i = 0; i < ROLLBACK_SEND_COUNT; i++) {
                producer.send(txSession.createTextMessage("rollback-msg-" + i));
            }

            txSession.rollback();
        }

        // Rollback processing is synchronous with the rollback() call, but
        // allow a grace period in case any async completion is in flight.
        Wait.waitFor(() -> getPendingSends(queue) == 0, 5000, 10);

        int leaked = getPendingSends(queue);
        LOG.info("After rollback of {} sends: pendingSends={}, singlePendingSend={}",
                ROLLBACK_SEND_COUNT, leaked, queue.singlePendingSend());

        assertEquals("PENDING SENDS LEAK: pendingSends should return to 0 after " +
                "transaction rollback, but " + leaked + " send(s) leaked. " +
                "CursorAddSync.afterRollback() must decrement pendingSends the same " +
                "way afterCommit() -> messageSent() does. A leaked counter disables " +
                "cursor caching (singlePendingSend() false forever).",
                0, getPendingSends(queue));
        assertTrue("singlePendingSend should recover after rollback", queue.singlePendingSend());

        assertEquals("message count should be 0 after rollback",
                0, queue.getDestinationStatistics().getMessages().getCount());
    }

    @Test(timeout = 60_000)
    public void testCommitKeepsPendingSendsBalanced() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME + ".COMMIT");
        var queue = (Queue) broker.getDestination(dest);

        try(var txSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            var producer = txSession.createProducer(dest)) {

            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            for (int i = 0; i < ROLLBACK_SEND_COUNT; i++) {
                producer.send(txSession.createTextMessage("commit-msg-" + i));
            }

            txSession.commit();
        }

        assertTrue("pendingSends should return to 0 after commit",
                Wait.waitFor(() -> getPendingSends(queue) == 0, 5000, 100));
        assertTrue("Queue should have " + ROLLBACK_SEND_COUNT + " messages after commit",
                Wait.waitFor(() -> queue.getDestinationStatistics().getMessages().getCount() == ROLLBACK_SEND_COUNT,
                        5000, 100));
    }

    private int getPendingSends(Queue queue) {
        try {
            var f = Queue.class.getDeclaredField("pendingSends");
            f.setAccessible(true);
            return ((AtomicInteger) f.get(queue)).get();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to read Queue.pendingSends", e);
        }
    }
}
