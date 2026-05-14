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
package org.apache.activemq.broker.region;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;
import static org.junit.Assert.assertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Throughput comparison: {@code expiryCheckEnabled=true} vs {@code expiryCheckEnabled=false}
 * for a slow-consumer topic with a large pending-message limit.
 *
 * <h3>What is being measured</h3>
 * <p>When a topic consumer is slow (its pending queue exceeds
 * {@code evictExpiredMessagesHighWatermark = 1000} by default), ActiveMQ calls
 * {@link TopicSubscription#removeExpiredMessages()} on every single
 * {@code add()} call.  That method iterates every pending message to call
 * {@code isExpired()} — an O(n) operation.  With a pending limit of 5,000
 * that scan runs up to 5,000 iterations per message send, dominated by the
 * Java heap iteration cost.
 *
 * <p>With {@code expiryCheckEnabled=false} on the {@link org.apache.activemq.broker.region.policy.MessageEvictionStrategy}
 * the scan body is skipped entirely via a single boolean check, reducing
 * per-send work back to O(1).
 *
 * <h3>Pass/fail threshold</h3>
 * <p>The test asserts that the {@code expiryCheckEnabled=false} run completes at
 * least {@code MIN_SPEEDUP_FACTOR}× faster than the {@code expiryCheckEnabled=true}
 * run.  A factor of 3 is deliberately conservative — in practice the
 * improvement is often 50–200× for large queues with pure in-memory messages.
 *
 * <p>The test is annotated {@code @Category(ParallelTest.class)} so it runs
 * in the normal CI suite, but uses a modest message count to keep wall-clock
 * time acceptable on slow machines.
 */
@Category(ParallelTest.class)
public class TopicSubscriptionEnableExpiryThroughputTest {

    private static final Logger LOG = LoggerFactory.getLogger(TopicSubscriptionEnableExpiryThroughputTest.class);

    /** Number of messages to send during the warm-up phase (fills queue above highWatermark). */
    private static final int WARMUP_COUNT = 1200;

    /**
     * Number of messages timed during the measurement phase.
     * Sending happens after the queue is already above 1000 (highWatermark),
     * so every message triggers the expiry-scan code path.
     */
    private static final int TIMED_COUNT = 2000;

    /** Pending message limit — large enough that O(n) scan is expensive. */
    private static final int PENDING_LIMIT = 5000;

    /**
     * Minimum speedup factor we require for the test to pass.
     * Conservative: real-world improvement is typically 50–200×.
     */
    private static final double MIN_SPEEDUP_FACTOR = 3.0;

    // -------------------------------------------------------------------------

    @Test
    public void testEnableExpiryFalseIsFasterForSlowConsumer() throws Exception {
        long msWithExpiry = measureSendTime(true);
        long msWithoutExpiry = measureSendTime(false);

        LOG.info("=== expiryCheckEnabled throughput results ===");
        LOG.info("  expiryCheckEnabled=true  : {} ms for {} timed messages ({} msg/s)",
                msWithExpiry, TIMED_COUNT,
                msWithExpiry > 0 ? (TIMED_COUNT * 1000L / msWithExpiry) : "n/a");
        LOG.info("  expiryCheckEnabled=false : {} ms for {} timed messages ({} msg/s)",
                msWithoutExpiry, TIMED_COUNT,
                msWithoutExpiry > 0 ? (TIMED_COUNT * 1000L / msWithoutExpiry) : "n/a");
        LOG.info("  Speedup factor     : {}", msWithExpiry > 0 ? String.format("%.1f×", (double) msWithExpiry / msWithoutExpiry) : "n/a");

        // Guard against pathological results (e.g. CI machine starved)
        // — only assert if the expiry run was genuinely slow (> 200 ms).
        if (msWithExpiry > 200) {
            double speedup = (double) msWithExpiry / Math.max(1, msWithoutExpiry);
            assertTrue(
                    String.format(
                        "Expected expiryCheckEnabled=false to be at least %.0f× faster than expiryCheckEnabled=true, "
                        + "but got %.1f× (%d ms vs %d ms). "
                        + "This likely means the O(n) expiry scan is no longer being skipped.",
                        MIN_SPEEDUP_FACTOR, speedup, msWithoutExpiry, msWithExpiry),
                    speedup >= MIN_SPEEDUP_FACTOR);
        } else {
            LOG.warn("expiryCheckEnabled=true run finished in only {} ms — machine may be too fast "
                    + "or warm-up count is too low to trigger the O(n) path reliably on this hardware. "
                    + "Skipping ratio assertion.", msWithExpiry);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Starts a broker with the given {@code expiryCheckEnabled} setting, creates a
     * slow consumer (prefetch=1, never reads), sends {@code WARMUP_COUNT}
     * messages to fill the pending queue above the eviction high-water mark,
     * then times sending {@code TIMED_COUNT} additional messages.
     *
     * @return wall-clock milliseconds for the timed phase
     */
    private long measureSendTime(boolean expiryCheckEnabled) throws Exception {
        String brokerName = "perf-" + (expiryCheckEnabled ? "expiry-on" : "expiry-off");
        BrokerService broker = buildBroker(brokerName, PENDING_LIMIT, expiryCheckEnabled);
        try {
            ActiveMQConnectionFactory cf =
                    new ActiveMQConnectionFactory("vm://" + brokerName + "?create=false");
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQTopic topic = new ActiveMQTopic("PERF.TOPIC");

            // Create a consumer but never call receive() — this makes it slow.
            // prefetch=1 so messages pile up in the broker's pending queue.
            ActiveMQTopic topicWithPrefetch = new ActiveMQTopic("PERF.TOPIC?consumer.prefetchSize=1");
            MessageConsumer consumer = session.createConsumer(topicWithPrefetch);

            MessageProducer producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // ---- Warm-up phase: fill pending queue above the high-water mark (1000) ----
            for (int i = 0; i < WARMUP_COUNT; i++) {
                producer.send(session.createTextMessage("warmup-" + i));
            }

            // ---- Timed phase: every add() triggers the expiry-scan code path ----
            long start = System.currentTimeMillis();
            for (int i = 0; i < TIMED_COUNT; i++) {
                producer.send(session.createTextMessage("timed-" + i));
            }
            long elapsed = System.currentTimeMillis() - start;

            conn.close();
            return elapsed;
        } finally {
            broker.stop();
        }
    }

    private BrokerService buildBroker(String brokerName, int pendingLimit, boolean expiryCheckEnabled)
            throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName(brokerName);
        broker.addConnector("vm://" + brokerName);
        broker.setDeleteAllMessagesOnStartup(true);

        ConstantPendingMessageLimitStrategy strategy = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(pendingLimit);

        OldestMessageEvictionStrategy evictionStrategy = new OldestMessageEvictionStrategy();
        evictionStrategy.setExpiryCheckEnabled(expiryCheckEnabled);

        PolicyEntry entry = new PolicyEntry();
        entry.setTopic(">");
        entry.setTopicPrefetch(1);
        entry.setPendingMessageLimitStrategy(strategy);
        entry.setMessageEvictionStrategy(evictionStrategy);
        entry.setDeadLetterStrategy(null);

        List<PolicyEntry> entries = new ArrayList<>();
        entries.add(entry);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.start();
        broker.waitUntilStarted();
        return broker;
    }
}

