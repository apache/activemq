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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
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
 * Checks the side effect of the fireAfterCommit isolation fix when a single
 * transaction both produces and consumes: after the fix, when a post-commit
 * synchronization throws, every synchronization runs afterCommit and then the
 * triggered rollback runs afterRollback. For a produce in that same
 * transaction the queue's CursorAddSync gets afterCommit (pages the message in,
 * counts it) and then afterRollback (rollbackPendingCursorAdditions) - while
 * the message is already durably committed in the store.
 *
 * The transaction here consumes M1 (its ack's afterCommit is rigged to throw)
 * and produces M2. The concern is that M2 is left durably in the store but
 * pulled back out of the in-memory cursor, stranded until a restart. The test
 * confirms this is not the case: the store must drain to empty and queueSize
 * must settle to 0, meaning M2 is still deliverable.
 */
@Category(ParallelTest.class)
public class TransactedProduceWithThrowingAckQueueSizeTest {

    private static final Logger LOG = LoggerFactory.getLogger(TransactedProduceWithThrowingAckQueueSizeTest.class);
    private static final String QUEUE_NAME = "TEST.TX.PRODUCE.THROWING.ACK";

    private BrokerService broker;
    private Connection connection;
    private File dataDir;

    /** Pending cursor that throws from reset() while armed, but only inside afterCommit. */
    static class FailingResetPendingCursor extends VMPendingMessageCursor {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger throwCount = new AtomicInteger(0);

        FailingResetPendingCursor(boolean prioritizedMessages) {
            super(prioritizedMessages);
        }

        @Override
        public synchronized void reset() {
            if (armed.get() && calledFromAfterCommit()) {
                throwCount.incrementAndGet();
                throw new RuntimeException("injected: transient cursor failure during post-commit dispatchPending");
            }
            super.reset();
        }

        private boolean calledFromAfterCommit() {
            for (var frame : Thread.currentThread().getStackTrace()) {
                if ("afterCommit".equals(frame.getMethodName())) {
                    return true;
                }
            }
            return false;
        }
    }

    @Before
    public void setUp() throws Exception {
        var baseDir = new File(IOHelper.getDefaultDataDirectory());
        Files.createDirectories(baseDir.toPath());
        dataDir = Files.createTempDirectory(baseDir.toPath(), "TxProduceThrowingAck-").toFile();
        dataDir.deleteOnExit();

        broker = new BrokerService();
        broker.setDataDirectoryFile(dataDir);
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 1024 * 1024);

        var pa = new KahaDBPersistenceAdapter();
        pa.setDirectory(new File(dataDir, "kahadb"));
        broker.setPersistenceAdapter(pa);

        // Production profile: no cursor cache, small page size
        var policyMap = new PolicyMap();
        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setUseCache(false);
        entry.setMaxPageSize(60);
        policyMap.setDefaultEntry(entry);
        broker.setDestinationPolicy(policyMap);

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
    public void testProducedMessageNotStrandedWhenAckAfterCommitThrows() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);

        // Seed M1 (committed, outside any transaction) so the transacted session
        // has a message to consume.
        try (var seedSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var seedProducer = seedSession.createProducer(dest)) {
            seedProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            seedProducer.send(seedSession.createTextMessage("M1-consumed"));
        }

        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();
        assertTrue("seed message should be counted",
                Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));

        // One transaction that produces M2 and consumes M1. Producing M2 adds a
        // CursorAddSync; committing the consume adds the ack synchronizations.
        try (var txSession = connection.createSession(true, Session.SESSION_TRANSACTED)) {
            try (var producer = txSession.createProducer(dest)) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.send(txSession.createTextMessage("M2-produced"));
            }

            var consumer = txSession.createConsumer(dest);
            var m1 = consumer.receive(5000);
            assertNotNull("M1 should be received in the transaction", m1);
            assertEquals("M1-consumed", ((TextMessage) m1).getText());

            // Rig the ack's afterCommit to throw, as in the D1 reproduction.
            var sub = (PrefetchSubscription) queue.getConsumers().get(0);
            var failingCursor = new FailingResetPendingCursor(false);
            sub.setPending(failingCursor);
            failingCursor.armed.set(true);

            try {
                txSession.commit();
                LOG.info("commit() returned normally");
            } catch (JMSException expected) {
                LOG.info("commit() threw (post-commit chain aborted, rollback triggered): {}",
                        expected.toString());
            } finally {
                failingCursor.armed.set(false);
            }

            assertTrue("injected afterCommit failure should have fired",
                    failingCursor.throwCount.get() >= 1);
            consumer.close();
        }

        // Drain whatever is deliverable with a fresh consumer.
        var received = new ArrayList<String>();
        try (var drainSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var drainConsumer = drainSession.createConsumer(dest)) {
            jakarta.jms.Message m;
            while ((m = drainConsumer.receive(2000)) != null) {
                received.add(((TextMessage) m).getText());
            }
        }

        long storeCount = queue.getMessageStore().getMessageCount();
        LOG.info("After drain: received={}, messages={}, store={}, enqueues={}, dequeues={}",
                received, stats.getMessages().getCount(), storeCount,
                stats.getEnqueues().getCount(), stats.getDequeues().getCount());

        // The produced M2 was durably committed to the store. It must not be
        // stranded there: the store must drain to empty and queueSize settle to
        // 0. A stranded M2 leaves store count and queueSize stuck at 1.
        assertTrue("LOST/STRANDED MESSAGE: the produced message is durably in the store but was " +
                "pulled out of the cursor by CursorAddSync.afterRollback and never redelivered " +
                "(store count " + queue.getMessageStore().getMessageCount() + ", received " + received + ")",
                Wait.waitFor(() -> {
                    try {
                        return queue.getMessageStore().getMessageCount() == 0;
                    } catch (Exception e) {
                        return false;
                    }
                }, 8000, 100));

        assertTrue("queueSize must settle to 0",
                Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100));
    }
}
