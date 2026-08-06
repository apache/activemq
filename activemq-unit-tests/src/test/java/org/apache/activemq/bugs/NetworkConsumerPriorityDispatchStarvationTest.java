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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduces dispatch starvation caused by Queue.doPageInForDispatch()
 * gating page-in on dispatchPendingList.size() < maxPageSize.
 *
 * When the dispatchPendingList fills with messages that no active consumer
 * can accept (selector mismatch, message group ownership by a stalled
 * consumer, etc.), the page-in gate blocks permanently. Deliverable
 * messages remain stuck in the store even though a consumer has available
 * prefetch capacity.
 *
 * The fix adds an alternative gate condition:
 *   pagedInPendingSize < maxPageSize || getConsumerMessageCountBeforeFull() > 0
 * and caps toPageIn to getConsumerMessageCountBeforeFull() when the pending
 * list is full, so page-in continues at the rate consumers can absorb.
 */
public class NetworkConsumerPriorityDispatchStarvationTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkConsumerPriorityDispatchStarvationTest.class);

//    static {
//        Configurator.setLevel("org.apache.activemq.broker.region.Queue", Level.DEBUG);
//    }

    private static final String QUEUE_NAME = "TEST.STARVATION";
    private static final int MAX_PAGE_SIZE = 20;
    private static final int TOTAL_MESSAGES = 200;
    private static final int APP_PREFETCH = 5;

    private BrokerService broker;
    private String brokerURL;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("starvation-test");
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setPersistent(true);

        var policyMap = new PolicyMap();
        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setLazyDispatch(true);
        entry.setMaxPageSize(MAX_PAGE_SIZE);
        entry.setUseCache(false);
        entry.setOptimizedDispatch(true);
        entry.setMemoryLimit(10 * 1024 * 1024);
        policyMap.setDefaultEntry(entry);
        broker.setDestinationPolicy(policyMap);

        broker.getSystemUsage().getMemoryUsage().setLimit(64 * 1024 * 1024);

        var connector = broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();
        brokerURL = connector.getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /**
     * Sends a mix of messages: 1 in 5 has target='app' (matches the app
     * consumer's selector), the rest have target='other' (no consumer
     * matches). The 'other' messages accumulate in dispatchPendingList
     * because no consumer can accept them. Once the list reaches
     * maxPageSize, the pre-fix page-in gate blocks permanently, starving
     * the app consumer of deliverable messages still in the store.
     *
     * Expected behavior:
     *   Pre-fix:  app consumer receives only ~5-8 of 40 messages (FAIL)
     *   Post-fix: app consumer receives all 40 messages (PASS)
     */
    @Test(timeout = 60_000)
    public void testUndeliverableMessagesClogDispatchBufferAndBlockPageIn() throws Exception {
        var appMessageCount = TOTAL_MESSAGES / 5;

        produceInterleavedMessages(TOTAL_MESSAGES);

        var allReceived = new CountDownLatch(appMessageCount);
        var received = new AtomicInteger(0);

        var factory = new ActiveMQConnectionFactory(brokerURL);
        var prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(APP_PREFETCH);
        factory.setPrefetchPolicy(prefetchPolicy);

        try(var conn = factory.createConnection();
            var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var consumer = session.createConsumer(
                    new ActiveMQQueue(QUEUE_NAME), "target = 'app'")) {

            conn.start();

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    int count = received.incrementAndGet();
                    if (count % 10 == 0) {
                        LOG.info("App consumer received {} / {} messages", count, appMessageCount);
                    }
                    allReceived.countDown();
                }
            });

            var drained = allReceived.await(10, TimeUnit.SECONDS);

            var brokerQueue = (Queue) ((RegionBroker) broker.getRegionBroker())
                    .getQueueRegion().getDestinationMap().get(new ActiveMQQueue(QUEUE_NAME));

            assertTrue("Queue dequeue metric should appMessageCount:" + appMessageCount, Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return brokerQueue.getDestinationStatistics().getDequeues().getCount() == appMessageCount;
                }
            }, TimeUnit.SECONDS.toMillis(5), 100));

            var finalReceived = received.get();
            LOG.info("App consumer received {} / {} messages (drained={})", finalReceived, appMessageCount, drained);
            if (brokerQueue != null) {
                LOG.info("Queue stats - queueSize: {}, enqueues: {}, dequeues: {}, inflight: {}, dispatched: {}",
                        brokerQueue.getDestinationStatistics().getMessages().getCount(),
                        brokerQueue.getDestinationStatistics().getEnqueues().getCount(),
                        brokerQueue.getDestinationStatistics().getDequeues().getCount(),
                        brokerQueue.getDestinationStatistics().getInflight().getCount(),
                        brokerQueue.getDestinationStatistics().getDispatched().getCount());
            }

            assertTrue(
                    "DISPATCH STARVATION: app consumer received only " + finalReceived +
                            " of " + appMessageCount + " deliverable messages. " +
                            "Undeliverable messages filled the dispatchPendingList to maxPageSize (" +
                            MAX_PAGE_SIZE + "), blocking page-in of remaining deliverable messages from the store.",
                    drained);

            assertEquals("All app messages should be received", appMessageCount, finalReceived);
        }
    }

    private void produceInterleavedMessages(int count) throws Exception {
        var factory = new ActiveMQConnectionFactory(brokerURL);
        try(var conn = factory.createConnection();
            var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME))) {
            conn.start();

            for (int i = 0; i < count; i++) {
                var msg = session.createTextMessage("Message-" + i);
                if (i % 5 == 0) {
                    msg.setStringProperty("target", "app");
                } else {
                    msg.setStringProperty("target", "other");
                }
                producer.send(msg);
            }

            LOG.info("Produced {} messages ({} app, {} other)", count, count / 5, count - count / 5);
        }
    }
}
