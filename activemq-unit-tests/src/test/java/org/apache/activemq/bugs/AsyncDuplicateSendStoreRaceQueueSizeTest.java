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
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Companion to DuplicateSendStoreRaceQueueSizeTest that confirms queue size
 * accounting for a duplicate resend under both cursor cache policies.
 *
 * The store add path a persistent send takes depends on the cursor cache
 * (Queue.doMessageSend checks asyncAddQueueMessage on messages.isCacheEnabled()):
 *
 * - useCache=true: the send takes the ASYNC store path. addQueueTask/
 *   addTopicTask do not guard against a duplicate async add, but the cache
 *   keeps the original's id in the cursor audit, so the duplicate's cursor add
 *   (messages.addMessageLast in Queue.doPendingCursorAdditions) returns false
 *   and it is never counted. No guard in addQueueTask is needed for accounting.
 *
 * - useCache=false: the send takes the SYNC store path (store.addMessage). The
 *   store rejects the duplicate row and returns -1, and the -1 gate in
 *   doPendingCursorAdditions drops the candidate before messageSent counts it.
 *
 * Either way the logical message is delivered once and the queue drains to
 * zero. The sync fix on this branch (KahaDBStore.addMessage) covers the
 * distinct case the sibling test reproduces: a sync duplicate racing a still
 * pending async add, where neither of these checks applies.
 */
@RunWith(Parameterized.class)
@Category(ParallelTest.class)
public class AsyncDuplicateSendStoreRaceQueueSizeTest {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDuplicateSendStoreRaceQueueSizeTest.class);
    private static final String QUEUE_NAME = "TEST.ASYNC.DUP.SEND.RACE";

    @Parameterized.Parameters(name = "useCache={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    private final boolean useCache;

    public AsyncDuplicateSendStoreRaceQueueSizeTest(boolean useCache) {
        this.useCache = useCache;
    }

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
        var dataDir = Files.createTempDirectory(baseDir.toPath(), "AsyncDupSendRace-").toFile();
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

        // useCache selects the store path the send takes: true keeps the cursor
        // cache and takes the async path, false takes the sync path.
        var policyMap = new PolicyMap();
        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setUseCache(useCache);
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
    public void testAsyncDuplicateSendRaceDoesNotInflateQueueSize() throws Exception {
        var dest = new ActiveMQQueue(QUEUE_NAME);
        var queue = (Queue) broker.getDestination(dest);
        var stats = queue.getDestinationStatistics();

        var asyncFactory = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri());
        asyncFactory.setUseAsyncSend(true);

        try (var producerConnection = asyncFactory.createConnection();
             var session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(dest)) {

            // 1. Original send via async producer: counted, and (with the cache
            // enabled) its message id recorded in the cursor audit.
            producerConnection.start();
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.send(session.createTextMessage("original"));

            assertTrue("original should be counted",
                    Wait.waitFor(() -> stats.getMessages().getCount() == 1, 5000, 10));
            assertEquals("one send captured", 1, capturedSends.size());

            // 2. Replay the captured command on a fresh exchange, a failover
            // style resend of the same message id. Queue.doMessageSend routes it
            // to the async store path when the cache is enabled and the sync
            // store path otherwise. Mark the replay non-response-required so the
            // broker-side send does not wait.
            var duplicate = (Message) capturedSends.get(0).copy();
            duplicate.setResponseRequired(false);
            LOG.info("Replaying duplicate send of {} via async store path", duplicate.getMessageId());

            var context = new ConnectionContext(new NonCachedMessageEvaluationContext());
            context.setBroker(broker.getBroker());
            context.setClientId("async-failover-resender");
            var exchange = new ProducerBrokerExchange();
            exchange.setConnectionContext(context);
            exchange.setMutable(true);
            exchange.setProducerState(new ProducerState(new ProducerInfo(
                    new ProducerId(new SessionId(new ConnectionId("async-failover-resender"), 1), 1))));

            broker.getBroker().send(exchange, duplicate);

            LOG.info("After duplicate send: messages={}, enqueues={}, dequeues={}, dupFromStore={}",
                    stats.getMessages().getCount(), stats.getEnqueues().getCount(),
                    stats.getDequeues().getCount(), stats.getDuplicateFromStore().getCount());
        }

        // 3. Drain: the logical message must be delivered exactly once.
        try (var consumeSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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

            assertTrue("INCORRECT QUEUE SIZE: queue drained (1 logical message, delivered once) " +
                    "but stats show messages=" + stats.getMessages().getCount() +
                    ", enqueues=" + stats.getEnqueues().getCount() +
                    ", dequeues=" + stats.getDequeues().getCount() +
                    ", duplicateFromStore=" + stats.getDuplicateFromStore().getCount() +
                    ", useCache=" + useCache + "; a duplicate resend was counted a second time " +
                    "instead of being suppressed by the store/cursor deduplication", settled);

            assertEquals("statistics invariant: enqueues - dequeues == messages",
                    stats.getMessages().getCount(),
                    stats.getEnqueues().getCount() - stats.getDequeues().getCount());
        }
    }
}
