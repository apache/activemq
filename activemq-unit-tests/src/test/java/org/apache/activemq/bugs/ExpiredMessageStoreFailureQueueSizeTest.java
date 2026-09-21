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
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
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
 * Reproduces queue size defect D2: the expiry decrement lost to a
 * swallowed store exception (docs/bugs/queuesize-deep-dive.md).
 *
 * Message expiry is gated by a one-shot CAS (Message.canProcessAsExpired,
 * consumed via RegionBroker.isExpired). The CAS winner is responsible for
 * completing removal and the statistics decrement. Queue.messageExpired
 * swallows IOException from removeMessage ("Failed to remove expired
 * Message from the store") AFTER the CAS is consumed and AFTER the expired
 * counter incremented, but BEFORE dropMessage (the only decrement of the
 * queue messages counter) has run.
 *
 * With the CAS consumed, every later encounter of the message takes a
 * CAS-loser branch that discards the reference from in-memory lists WITHOUT
 * decrementing, and the expiry is never retried: the messages counter stays
 * +1 permanently while nothing is browsable, dispatchable, or inflight,
 * the production queue size fingerprint.
 *
 * The test wraps the queue's MessageStore with a proxy whose remove throws
 * IOException once during the periodic expiry sweep, then asserts the
 * counter recovers to 0 once the store is healthy again (a later sweep must
 * be able to retry the expiry).
 */
@Category(ParallelTest.class)
public class ExpiredMessageStoreFailureQueueSizeTest {

    private static final Logger LOG = LoggerFactory.getLogger(ExpiredMessageStoreFailureQueueSizeTest.class);
    private static final String QUEUE_NAME = "TEST.EXPIRY.STORE.FAIL";

    private BrokerService broker;
    private Connection connection;
    private File dataDir;

    /** MessageStore proxy that fails message removal once while armed. */
    static class FailingRemoveMessageStore extends ProxyMessageStore {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger throwCount = new AtomicInteger(0);

        FailingRemoveMessageStore(MessageStore delegate) {
            super(delegate);
        }

        private void maybeThrow() throws IOException {
            if (armed.compareAndSet(true, false)) {
                throwCount.incrementAndGet();
                throw new IOException("injected: store failure removing expired message");
            }
        }

        @Override
        public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
            maybeThrow();
            super.removeMessage(context, ack);
        }

        @Override
        public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
            maybeThrow();
            super.removeAsyncMessage(context, ack);
        }
    }

    @Before
    public void setUp() throws Exception {
        var baseDir = new File(IOHelper.getDefaultDataDirectory());
        Files.createDirectories(baseDir.toPath());
        dataDir = Files.createTempDirectory(baseDir.toPath(), "ExpiryStoreFail-").toFile();
        dataDir.deleteOnExit();

        broker = new BrokerService();
        broker.setDataDirectoryFile(dataDir);
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 1024 * 1024);

        var pa = new KahaDBPersistenceAdapter();
        pa.setDirectory(new File(dataDir, "kahadb"));
        broker.setPersistenceAdapter(pa);

        // Production profile plus a fast periodic expiry sweep
        var policyMap = new PolicyMap();
        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setUseCache(false);
        entry.setMaxPageSize(60);
        entry.setExpireMessagesPeriod(500);
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
    public void testExpiryRetriedAfterStoreFailure() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();

        // Wrap the queue's store so the expiry removal fails exactly once
        var failingStore = swapStore(queue);

        // Watch the expiry advisory so we can prove it fires exactly once, not
        // once per attempt.
        try (var advisorySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var advisoryConsumer = advisorySession.createConsumer(AdvisorySupport.getExpiredMessageTopic(dest))) {

            // One persistent message with a short TTL and no consumer, so the
            // periodic sweep owns the expiry.
            try (var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 var producer = session.createProducer(dest)) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.setTimeToLive(1000);
                producer.send(session.createTextMessage("will-expire"));
            }

            assertTrue("message should be counted",
                    Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));

            failingStore.armed.set(true);

            // The first sweep hits the injected store failure; messageExpired
            // resets processAsExpired so a later sweep can retry.
            assertTrue("injected store failure should fire during the expiry sweep",
                    Wait.waitFor(() -> failingStore.throwCount.get() >= 1, 10_000, 50));

            // Store is healthy again from here on. The retry removes the row and
            // decrements the messages counter.
            var settled = Wait.waitFor(() -> stats.getMessages().getCount() == 0, 10_000, 100);

            var storeCount = queue.getMessageStore().getMessageCount();
            LOG.info("Final stats: messages={}, enqueues={}, dequeues={}, expired={}, inflight={}, storeCount={}",
                    stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getExpired().getCount(),
                    stats.getInflight().getCount(), storeCount);

            // Nothing must be browsable/consumable either way (the message is expired)
            try (var verifySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 var verifyConsumer = verifySession.createConsumer(dest)) {
                assertNull("expired message must not be consumable", verifyConsumer.receive(1000));
            }

            assertTrue("INCORRECT QUEUE SIZE (D2): expiry store removal failed once and was never " +
                    "retried; the expiry CAS was already consumed, so the messages counter is " +
                    "stuck at " + stats.getMessages().getCount() + " (store row count " + storeCount +
                    ") with nothing consumable, browsable or inflight. Queue.messageExpired must " +
                    "reset the message's processAsExpired state when the store removal fails so a " +
                    "later sweep can retry", settled);

            assertTrue("store should be empty after the retried expiry",
                    Wait.waitFor(() -> {
                        try {
                            return queue.getMessageStore().getMessageCount() == 0;
                        } catch (Exception e) {
                            return false;
                        }
                    }, 5000, 100));

            // messageExpired advertises and counts the expiry only after the store
            // removal and messages decrement succeed, so the failed attempt
            // contributes nothing: the expiry is counted once and advertised once
            // despite the retry.
            assertEquals("expiry must be counted once despite the failed attempt and retry",
                    1, stats.getExpired().getCount());

            var advisoryCount = 0;
            while (advisoryConsumer.receive(500) != null) {
                advisoryCount++;
            }
            assertEquals("expiry advisory must fire once despite the failed attempt and retry",
                    1, advisoryCount);
        }
    }

    private FailingRemoveMessageStore swapStore(Queue queue) throws Exception {
        var storeField = BaseDestination.class.getDeclaredField("store");
        storeField.setAccessible(true);
        var original = (MessageStore) storeField.get(queue);
        var failing = new FailingRemoveMessageStore(original);
        storeField.set(queue, failing);
        return failing;
    }
}
