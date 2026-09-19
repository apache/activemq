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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

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
 * Reproduces queueSize drift candidate D1: an exception thrown from an
 * earlier transaction Synchronization skips the queue's dropMessage
 * synchronization (docs/bugs/queuesize-drift-deep-dive.md).
 *
 * For a transacted acknowledgment two synchronizations are registered in
 * order: the subscription's (PrefetchSubscription.registerRemoveSync: sub
 * dequeue counters, dispatched-list removal, then nodeDest.wakeup() and
 * dispatchPending()) followed by the queue's (Queue.removeMessage, whose
 * afterCommit calls dropMessage(), the only decrement of the queue messages
 * counter).
 *
 * Transaction.fireAfterCommit() iterates the synchronization list with no
 * per-synchronization exception isolation: the first throw aborts the loop.
 * The KahaDB store commit is already durable at that point (the index ack is
 * applied before the postCommit runnable runs), so a throw from the
 * subscription synchronization (for example dispatchPending() hitting a
 * transient cursor/store error, which with useCache=false does store work on
 * every commit) permanently loses the decrement:
 *
 *   message removed from the store and never re-deliverable,
 *   but messages counter stays +1: queue drained, browse empty, inflight 0.
 *
 * The test injects a one-shot RuntimeException from the subscription's
 * pending cursor reset() (the first call dispatchPending makes), commits,
 * and asserts the counter returns to 0.
 */
@Category(ParallelTest.class)
public class TransactedAckPostCommitDriftTest {

    private static final Logger LOG = LoggerFactory.getLogger(TransactedAckPostCommitDriftTest.class);
    private static final String QUEUE_NAME = "TEST.TX.POST.COMMIT.DRIFT";

    private BrokerService broker;
    private Connection connection;
    private File dataDir;

    /**
     * Pending cursor that throws from reset() while armed, but ONLY when
     * invoked from within a transaction Synchronization's afterCommit. The
     * ack-processing path also calls dispatchPending() and must not be
     * poisoned (that would mark the transaction rollback-only before the
     * store commit, a different scenario).
     */
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
        dataDir = Files.createTempDirectory(baseDir.toPath(), "TxPostCommitDrift-").toFile();
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
    public void testPostCommitExceptionDoesNotLoseDecrement() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);

        // Send one persistent message
        try (var producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = producerSession.createProducer(dest)) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.send(producerSession.createTextMessage("m1"));
        }

        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();
        assertTrue("message should be counted",
                Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));

        // Transacted consume (DMLC style: 1 message per commit)
        try (var txSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             var consumer = txSession.createConsumer(dest)) {
            Message received = consumer.receive(5000);
            assertNotNull("message should be received", received);

            // Swap the subscription's pending cursor for one that throws from
            // reset(), the first call dispatchPending() makes inside the
            // subscription synchronization's afterCommit.
            var sub = (PrefetchSubscription) queue.getConsumers().get(0);
            var failingCursor = new FailingResetPendingCursor(false);
            sub.setPending(failingCursor);
            failingCursor.armed.set(true);

            try {
                txSession.commit();
                LOG.info("commit() returned normally");
            } catch (JMSException expected) {
                LOG.info("commit() threw (expected while the post-commit chain aborts): {}",
                        expected.toString());
            } finally {
                failingCursor.armed.set(false);
            }

            assertTrue("injected failure should have fired during afterCommit",
                    failingCursor.throwCount.get() >= 1);
        }

        // The store ack was durable before the synchronizations ran: the
        // message must be gone from the store and not redeliverable.
        assertTrue("store should be empty after commit",
                Wait.waitFor(() -> {
                    try {
                        return queue.getMessageStore().getMessageCount() == 0;
                    } catch (Exception e) {
                        return false;
                    }
                }, 5000, 100));

        try (var verifySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var verifyConsumer = verifySession.createConsumer(dest)) {
            assertNull("no message should be redeliverable", verifyConsumer.receive(2000));
        }

        var settled = Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100);

        LOG.info("Final stats: messages={}, enqueues={}, dequeues={}, inflight={}",
                stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                stats.getDequeues().getCount(), stats.getInflight().getCount());

        assertTrue("QUEUE SIZE DRIFT (D1): message committed+consumed (store empty, nothing " +
                "redeliverable, inflight=" + stats.getInflight().getCount() + ") but messages " +
                "counter is " + stats.getMessages().getCount() + "; an exception from the " +
                "subscription's afterCommit synchronization skipped the queue's dropMessage " +
                "synchronization in Transaction.fireAfterCommit",
                settled);

        assertEquals("dequeues should reflect the consumed message",
                1, stats.getDequeues().getCount());
    }
}
