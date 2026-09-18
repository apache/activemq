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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
import org.apache.activemq.broker.region.cursors.AbstractPendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
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
 * Reproduces queueSize drift candidate D3: the mixed async/sync duplicate
 * send race (docs/bugs/queuesize-drift-deep-dive.md).
 *
 * Recipe:
 * 1. Original message M sent while the cursor cache is ENABLED: the async
 *    store path (concurrentStoreAndDispatchQueues=true, the default) counts
 *    it via messageSent() and adds it to the cursor cache immediately, but
 *    the KahaDB INDEX update is parked in a deferred StoreQueueTask (this
 *    test blocks the store's queueExecutor to hold it deterministically).
 * 2. The cursor cache disables (here: forced; in production, memory
 *    pressure / load). A failover-style resend of M arrives via a different
 *    connection: the connection-level producer audit does not cover it, the
 *    SYNC store path finds NO row in messageIdIndex (the original's update
 *    is still parked), assigns a real sequence, passes the -1 duplicate
 *    gate in doPendingCursorAdditions, and, with the cache disabled so there
 *    is no cursor-audit at add time, messageSent() COUNTS IT AGAIN.
 * 3. The executor is released: the original's deferred index update now
 *    loses ("Duplicate message add attempt rejected" on the async executor
 *    thread). The store holds ONE row.
 * 4. A consumer drains the queue: exactly one delivery, one decrement.
 *
 * Accounting: +1 (original) +1 (duplicate resend) -1 (single ack)
 * = messages counter permanently +1 with the queue fully drained, browse
 * empty, inflight 0, the production drift fingerprint, with no ERROR logs.
 */
@Category(ParallelTest.class)
public class DuplicateSendStoreRaceDriftTest {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicateSendStoreRaceDriftTest.class);
    private static final String QUEUE_NAME = "TEST.DUP.SEND.RACE";

    private BrokerService broker;
    private Connection connection;
    private final List<Message> capturedSends = new CopyOnWriteArrayList<>();

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
        var dataDir = Files.createTempDirectory(baseDir.toPath(), "DupSendRace-").toFile();
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

        // Defaults: useCache=true, concurrentStoreAndDispatchQueues=true,
        // the configuration in which the original send rides the async path.
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
    public void testAsyncSyncDuplicateSendRaceDriftsQueueSize() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();

        // Park the KahaDB async store executor so the original's index
        // update is deferred (simulates the deferred StoreQueueTask under
        // load; the executor only grows past 1 thread when its 10k queue
        // fills, so one blocker holds the lane).
        var queueExecutor = getQueueExecutor();
        var release = new CountDownLatch(1);
        var blocked = new CountDownLatch(1);
        queueExecutor.execute(() -> {
            blocked.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue("executor blocker should start", blocked.await(5, TimeUnit.SECONDS));

        var asyncFactory = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri());
        asyncFactory.setUseAsyncSend(true);

        try(var producerConnection = asyncFactory.createConnection();
            var session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(dest)) {

            // 1. Original send, async producer so send() does not wait on the
            // parked store future (mirrors high-throughput async-send
            // producers). Counted + cached immediately; index update parked.
            producerConnection.start();
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.send(session.createTextMessage("original"));

            assertTrue("original should be counted",
                    Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));
            assertEquals("one send captured", 1, capturedSends.size());

            // 2. Cache disables (forced here; memory pressure in production),
            // then the failover-style resend arrives on a fresh exchange
            disableCursorCache(queue);

            var duplicate = (Message) capturedSends.get(0).copy();
            LOG.info("Replaying duplicate send of {} via sync store path", duplicate.getMessageId());

            var context = new ConnectionContext(new NonCachedMessageEvaluationContext());
            context.setBroker(broker.getBroker());
            context.setClientId("failover-resender");
            var exchange = new ProducerBrokerExchange();
            exchange.setConnectionContext(context);
            exchange.setMutable(true);
            exchange.setProducerState(new ProducerState(new ProducerInfo(
                    new ProducerId(new SessionId(new ConnectionId("failover-resender"), 1), 1))));

            broker.getBroker().send(exchange, duplicate);

            LOG.info("After duplicate send: messages={}, enqueues={}, dequeues={}, dupFromStore={}",
                    stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getDuplicateFromStore().getCount());
        } finally {
            // 3. Release the original's parked index update, it now loses
            release.countDown();
        }

        // Let the deferred task process ("Duplicate message add attempt rejected")
        Thread.sleep(1000);

        // 4. Drain, the logical message must be delivered exactly once
        try(var consumeSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var consumer = consumeSession.createConsumer(dest)) {
            var received = 0;
            jakarta.jms.Message msg;

            while ((msg = consumer.receive(3000)) != null) {
                LOG.info("received {} -> {}", msg.getJMSMessageID(), ((TextMessage) msg).getText());
                received++;
            }
            var settled = Wait.waitFor(() -> stats.getMessages().getCount() == 0, 5000, 100);

            LOG.info("After drain: received={}, messages={}, enqueues={}, dequeues={}, dupFromStore={}, inflight={}",
                    received, stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getDuplicateFromStore().getCount(),
                    stats.getInflight().getCount());
            assertEquals("the logical message must be delivered exactly once", 1, received);

            assertTrue("QUEUE SIZE DRIFT (D3): queue drained (1 logical message, delivered once) " +
                    "but stats show messages=" + stats.getMessages().getCount() +
                    ", enqueues=" + stats.getEnqueues().getCount() +
                    ", dequeues=" + stats.getDequeues().getCount() +
                    ", duplicateFromStore=" + stats.getDuplicateFromStore().getCount() +
                    "; the sync duplicate resend was counted by messageSent() because the " +
                    "original's deferred index update had not landed when the -1 duplicate " +
                    "gate checked", settled);
        }
    }

    private ExecutorService getQueueExecutor() throws Exception {
        var pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        var letterField = KahaDBPersistenceAdapter.class.getDeclaredField("letter");
        letterField.setAccessible(true);
        var store = (KahaDBStore) letterField.get(pa);
        var executorField = KahaDBStore.class.getDeclaredField("queueExecutor");
        executorField.setAccessible(true);
        return (ExecutorService) executorField.get(store);
    }

    private void disableCursorCache(Queue queue) throws Exception {
        var messagesField = Queue.class.getDeclaredField("messages");
        messagesField.setAccessible(true);
        var cursor = (AbstractPendingMessageCursor) messagesField.get(queue);
        cursor.setCacheEnabled(false);
        if (cursor instanceof StoreQueueCursor) {
            var persistentField = StoreQueueCursor.class.getDeclaredField("persistent");
            persistentField.setAccessible(true);
            var persistent =
                    (AbstractPendingMessageCursor) persistentField.get(cursor);
            if (persistent != null) {
                persistent.setCacheEnabled(false);
            }
        }
    }
}
