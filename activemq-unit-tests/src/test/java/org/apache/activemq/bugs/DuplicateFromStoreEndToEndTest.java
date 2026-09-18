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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.state.ProducerState;
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
 * End-to-end pins of queue statistics accounting when duplicate messages are
 * detected, exercised through the REAL broker paths (no direct invocation of
 * duplicateFromStore).
 *
 * Two duplicate classes exist and the broker handles them differently:
 *
 * 1. Duplicate SEND (failover-style resend via a new connection, so the
 *    per-connection producer audit cannot suppress it): the cursor cache
 *    audit traps it, gotToTheStore() observes the KahaDB index rejection of
 *    the duplicate row, and the send is fully suppressed, never counted by
 *    messageSent(), duplicateFromStore never fires. No drift.
 *
 * 2. Store RE-READ ("cursor got duplicate from store", the !cached branch in
 *    AbstractStoreCursor.recoverMessage): the cursor re-reads a row whose id
 *    the audit has already recorded, a SINGLE-counted message read twice.
 *    duplicateFromStore poison-acks the row, while the inflight copy is
 *    still consumed and acked normally. The consumer's ack performs the one
 *    and only messages-counter decrement; duplicateFromStore must NOT
 *    decrement it as well, or the counter goes negative.
 *
 * Invariant pinned by both tests: every real message is consumed exactly
 * once, and after draining, messages == 0 and enqueues - dequeues == messages.
 */
@Category(ParallelTest.class)
public class DuplicateFromStoreEndToEndTest {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicateFromStoreEndToEndTest.class);
    private static final String QUEUE_NAME = "TEST.DUP.FROM.STORE.E2E";
    private static final int MESSAGE_COUNT = 3;

    private BrokerService broker;
    private Connection connection;
    private File dataDir;
    private final List<Message> capturedSends = new CopyOnWriteArrayList<>();

    /** Captures copies of message commands as they pass through the broker. */
    class CapturingPlugin extends BrokerPluginSupport {
        @Override
        public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
            if (messageSend.getDestination() != null
                    && QUEUE_NAME.equals(messageSend.getDestination().getPhysicalName())) {
                capturedSends.add((Message) messageSend.copy());
            }
            super.send(producerExchange, messageSend);
        }
    }

    @Before
    public void setUp() throws Exception {
        var baseDir = new File(IOHelper.getDefaultDataDirectory());
        Files.createDirectories(baseDir.toPath());
        dataDir = Files.createTempDirectory(baseDir.toPath(), "DupFromStoreE2E-").toFile();
        dataDir.deleteOnExit();

        broker = new BrokerService();
        broker.setDataDirectoryFile(dataDir);
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 1024 * 1024);
        broker.setPlugins(new BrokerPlugin[] { next -> {
            var plugin = new CapturingPlugin();
            plugin.setNext(next);
            return plugin;
        } });

        var pa = new KahaDBPersistenceAdapter();
        pa.setDirectory(new File(dataDir, "kahadb"));
        broker.setPersistenceAdapter(pa);

        // The .REREAD queue pages in from the store (no cache), one row per
        // batch, so the re-read scenario can be triggered deterministically.
        var policyMap = new org.apache.activemq.broker.region.policy.PolicyMap();
        var rereadEntry = new org.apache.activemq.broker.region.policy.PolicyEntry();
        rereadEntry.setQueue(QUEUE_NAME + ".REREAD");
        rereadEntry.setUseCache(false);
        policyMap.put(new ActiveMQQueue(QUEUE_NAME + ".REREAD"), rereadEntry);
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
    public void testDuplicateSendIsSuppressedWithoutCountingDrift() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);

        try (var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            // 1. Send 3 unique persistent messages
            try (var producer = session.createProducer(dest)) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                for (var i = 0; i < MESSAGE_COUNT; i++) {
                    producer.send(session.createTextMessage("msg-" + i));
                }
            }

            var queue = (Queue) broker.getDestination(dest);
            var stats = queue.getDestinationStatistics();

            assertTrue("queue should count 3 messages after produce",
                    Wait.waitFor(() -> stats.getMessages().getCount() == MESSAGE_COUNT, 5000, 10));
            assertEquals("3 sends captured", MESSAGE_COUNT, capturedSends.size());

            // 2. Replay message #2's command with a fresh exchange, a
            // failover-style resend from a new connection. Same MessageId, so
            // the cursor audit must trap it as a duplicate send and route it to
            // duplicateFromStore().
            var duplicate = (Message) capturedSends.get(1).copy();
            LOG.info("Replaying duplicate send of {}", duplicate.getMessageId());

            var context = new ConnectionContext(new NonCachedMessageEvaluationContext());
            context.setBroker(broker.getBroker());
            context.setClientId("duplicate-resender");
            var exchange = new ProducerBrokerExchange();
            exchange.setConnectionContext(context);
            exchange.setMutable(true);
            var producerInfo = new ProducerInfo(
                    new ProducerId(new SessionId(new ConnectionId("duplicate-resender"), 1), 1));
            exchange.setProducerState(new ProducerState(producerInfo));

            broker.getBroker().send(exchange, duplicate);

            // The duplicate may be processed at cursor-add time or deferred
            // until the next page-in (dealWithDuplicates); don't fail here,
            // observe at the end.
            Wait.waitFor(() -> stats.getDuplicateFromStore().getCount() == 1, 3000, 10);

            LOG.info("After duplicate send: messages={}, enqueues={}, dequeues={}, dupFromStore={}",
                    stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getDuplicateFromStore().getCount());

            // 3. Drain the queue, all 3 real messages must still be consumable
            var received = 0;
            try (var consumer = session.createConsumer(dest)) {
                jakarta.jms.Message msg;
                while ((msg = consumer.receive(2000)) != null) {
                    LOG.info("received {} -> {}", msg.getJMSMessageID(), ((TextMessage) msg).getText());
                    received++;
                }
                assertNull("no further messages expected", consumer.receiveNoWait());
            }

            assertEquals("MESSAGE LOSS: consumer must receive all " + MESSAGE_COUNT +
                    " real messages despite the duplicate send", MESSAGE_COUNT, received);

            // 4. The metric must reflect reality: empty queue == 0
            var settled = Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100);

            LOG.info("After drain: messages={}, enqueues={}, dequeues={}, dupFromStore={}, inflight={}",
                    stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getDuplicateFromStore().getCount(),
                    stats.getInflight().getCount());

            assertTrue("COUNTING DRIFT: queue is empty (all " + MESSAGE_COUNT + " messages " +
                    "consumed, duplicate trapped) but stats show messages=" +
                    stats.getMessages().getCount() +
                    ", enqueues=" + stats.getEnqueues().getCount() +
                    ", dequeues=" + stats.getDequeues().getCount() +
                    ", duplicateFromStore=" + stats.getDuplicateFromStore().getCount() +
                    "; the counted duplicate send was removed without dequeue accounting",
                    settled);

            assertEquals("statistics invariant: enqueues - dequeues == messages",
                    stats.getMessages().getCount(),
                    stats.getEnqueues().getCount() - stats.getDequeues().getCount());
        }
    }

    /**
     * The store re-read case ("cursor got duplicate from store", !cached
     * branch): the cursor reads a row from the store whose id the audit has
     * already seen, a single counted message read twice. Triggered
     * deterministically by resetting store batching while an unacked message
     * is inflight, which is what batch-reset races do in production.
     *
     * The message was counted ONCE (its send) and the consumer's ack
     * decrements ONCE. duplicateFromStore removes the store row but must NOT
     * decrement the messages counter; the inflight copy's ack already
     * accounts for it. A decrement here drives the counter negative.
     */
    @Test(timeout = 60_000)
    public void testStoreReReadDuplicateDoesNotCorruptQueueSize() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME + ".REREAD");

        try (var session = connection.createSession(false,
                org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)) {
            try (var producer = session.createProducer(dest)) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                for (var i = 0; i < MESSAGE_COUNT; i++) {
                    producer.send(session.createTextMessage("msg-" + i));
                }
            }

            var queue = (Queue) broker.getDestination(dest);
            var stats = queue.getDestinationStatistics();
            assertTrue("queue should count 3 messages",
                    Wait.waitFor(() -> stats.getMessages().getCount() == MESSAGE_COUNT, 5000, 10));

            try (var consumer = session.createConsumer(dest)) {
                // Receive (but do not ack) all three, their rows stay in the
                // store while the cursor audit has recorded their ids.
                var toAck = new CopyOnWriteArrayList<jakarta.jms.Message>();
                for (var i = 0; i < MESSAGE_COUNT; i++) {
                    var m = consumer.receive(5000);
                    assertNotNull("message " + i, m);
                    toAck.add(m);
                }

                // Rewind the store batch position, the next fillBatch re-reads
                // the three unacked rows, which the audit traps as duplicates
                // (the "cursor got duplicate from store" branch), via the REAL
                // path.
                queue.getMessageStore().resetBatching();

                // A fourth send forces the next fillBatch.
                try (var producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                     var producer2 = producerSession.createProducer(dest)) {
                    producer2.setDeliveryMode(DeliveryMode.PERSISTENT);
                    producer2.send(producerSession.createTextMessage("msg-3"));
                }

                // Ack the three held messages. Each ack wakes the dispatch
                // task, which walks the cursor past the duplicate rows one
                // fillBatch at a time until the fourth message is recovered
                // and dispatched.
                for (var m : toAck) {
                    m.acknowledge();
                }

                var fourth = consumer.receive(10_000);
                assertNotNull("fourth message should be dispatched after the re-read", fourth);
                fourth.acknowledge();
                toAck.add(fourth);
                assertNull("no further messages expected", consumer.receive(1000));

                LOG.info("Re-read scenario: received={}, dupFromStore={}, messages={}, enqueues={}, dequeues={}",
                        toAck.size(), stats.getDuplicateFromStore().getCount(), stats.getMessages().getCount(),
                        stats.getEnqueues().getCount(), stats.getDequeues().getCount());

                assertEquals("consumer must receive exactly the 4 real messages once each",
                        MESSAGE_COUNT + 1, toAck.size());
            }

            assertTrue("store re-read should have fired duplicateFromStore via the real cursor path",
                    stats.getDuplicateFromStore().getCount() >= 1);

            var settled = Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100);
            assertTrue("NEGATIVE DRIFT: queue drained (4 sent, 4 consumed) but messages counter is " +
                    stats.getMessages().getCount() + "; duplicateFromStore must not decrement for a " +
                    "re-read of a single-counted message; the inflight copy's ack already accounts for it",
                    settled);
        }
    }
}
