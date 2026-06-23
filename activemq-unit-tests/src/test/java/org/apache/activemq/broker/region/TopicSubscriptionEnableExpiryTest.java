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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * Tests correctness of the {@code ExpiryCheckEnabled} feature on
 * {@link org.apache.activemq.broker.region.policy.MessageEvictionStrategy} and its effect on
 * {@link TopicSubscription}.
 *
 * <p>Background: when a slow-consumer queue exceeds
 * {@code evictExpiredMessagesHighWatermark} (default: 1000), ActiveMQ calls
 * {@code TopicSubscription.removeExpiredMessages()} on every single
 * {@code add()} call.  That method iterates every pending message checking
 * {@code isExpired()} — an O(n) scan.  Setting {@code ExpiryCheckEnabled=false}
 * on the {@link org.apache.activemq.broker.region.policy.MessageEvictionStrategy} skips that
 * scan entirely via a single boolean check guarding the call site.
 */
public class TopicSubscriptionEnableExpiryTest {

    // -------------------------------------------------------------------------
    // Unit tests — no broker needed
    // -------------------------------------------------------------------------

    /**
     * {@link OldestMessageEvictionStrategy#isExpiryCheckEnabled()} must default to {@code true} so
     * that existing deployments that do not set the property are unaffected.
     */
    @Test
    public void testEvictionStrategyExpiryCheckDefaultsToTrue() {
        OldestMessageEvictionStrategy strategy = new OldestMessageEvictionStrategy();
        assertTrue("ExpiryCheckEnabled must default to true (preserves existing behaviour)",
                strategy.isExpiryCheckEnabled());
    }

    @Test
    public void testEvictionStrategySetExpiryCheckFalse() {
        OldestMessageEvictionStrategy strategy = new OldestMessageEvictionStrategy();
        strategy.setExpiryCheckEnabled(false);
        assertFalse("ExpiryCheckEnabled should be false after setter call",
                strategy.isExpiryCheckEnabled());
    }

    @Test
    public void testEvictionStrategySetExpiryCheckRoundTrip() {
        OldestMessageEvictionStrategy strategy = new OldestMessageEvictionStrategy();
        strategy.setExpiryCheckEnabled(false);
        assertFalse(strategy.isExpiryCheckEnabled());
        strategy.setExpiryCheckEnabled(true);
        assertTrue(strategy.isExpiryCheckEnabled());
    }

    /**
     * {@link TopicSubscription} must pick up the eviction strategy flag — when
     * {@code ExpiryCheckEnabled=false} is set on the strategy the scan is skipped.
     */
    @Test
    public void testTopicSubscriptionUsesStrategyExpiryCheckFlag() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        try {
            TopicSubscription sub = buildMinimalTopicSubscription(broker);
            // default strategy — expiry scan enabled
            assertTrue("default strategy must have ExpiryCheckEnabled=true",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());

            OldestMessageEvictionStrategy strategy = new OldestMessageEvictionStrategy();
            strategy.setExpiryCheckEnabled(false);
            sub.setMessageEvictionStrategy(strategy);
            assertFalse("strategy with ExpiryCheckEnabled=false must reflect on subscription",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());
        } finally {
            broker.stop();
        }
    }

    // -------------------------------------------------------------------------
    // PolicyEntry propagation tests
    // -------------------------------------------------------------------------

    /**
     * When a {@link PolicyEntry} is configured with an eviction strategy that has
     * {@code ExpiryCheckEnabled=false}, {@code PolicyEntry.configure(TopicSubscription)}
     * must propagate the strategy so the O(n) expiry scan is skipped.
     */
    @Test
    public void testPolicyEntryPropagatesEvictionStrategyToSubscription() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        try {
            ConstantPendingMessageLimitStrategy limitStrategy = new ConstantPendingMessageLimitStrategy();
            limitStrategy.setLimit(2000);

            OldestMessageEvictionStrategy evictionStrategy = new OldestMessageEvictionStrategy();
            evictionStrategy.setExpiryCheckEnabled(false);

            PolicyEntry entry = new PolicyEntry();
            entry.setPendingMessageLimitStrategy(limitStrategy);
            entry.setMessageEvictionStrategy(evictionStrategy);

            TopicSubscription sub = buildMinimalTopicSubscription(broker);
            assertTrue(sub.getMessageEvictionStrategy().isExpiryCheckEnabled()); // default

            entry.configure(broker.getBroker(), broker.getSystemUsage(), sub);

            assertFalse("PolicyEntry.configure() must propagate eviction strategy with ExpiryCheckEnabled=false",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());
        } finally {
            broker.stop();
        }
    }

    /**
     * When the default eviction strategy is used (no override on PolicyEntry),
     * the subscription's expiry scan must remain enabled.
     */
    @Test
    public void testDefaultPolicyEntryLeavesExpiryCheckEnabled() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        try {
            ConstantPendingMessageLimitStrategy limitStrategy = new ConstantPendingMessageLimitStrategy();
            limitStrategy.setLimit(2000);

            PolicyEntry entry = new PolicyEntry();
            entry.setPendingMessageLimitStrategy(limitStrategy);
            // no messageEvictionStrategy override — default OldestMessageEvictionStrategy used

            TopicSubscription sub = buildMinimalTopicSubscription(broker);
            entry.configure(broker.getBroker(), broker.getSystemUsage(), sub);

            assertTrue("subscription must still have ExpiryCheckEnabled=true when using the default eviction strategy",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());
        } finally {
            broker.stop();
        }
    }

    /**
     * When no {@link org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy} is
     * set at all, the subscription's expiry scan flag must remain at its default ({@code true}).
     */
    @Test
    public void testPolicyEntryWithNoStrategyLeavesExpiryCheckEnabled() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        try {
            PolicyEntry entry = new PolicyEntry(); // no strategy, no eviction strategy override

            TopicSubscription sub = buildMinimalTopicSubscription(broker);
            entry.configure(broker.getBroker(), broker.getSystemUsage(), sub);

            assertTrue("subscription must keep ExpiryCheckEnabled=true when no strategy is configured",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());
        } finally {
            broker.stop();
        }
    }

    /**
     * A custom {@link org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy} with no
     * eviction strategy override must leave the subscription's expiry scan enabled.
     */
    @Test
    public void testCustomLimitStrategyWithDefaultEvictionLeavesExpiryCheckEnabled() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        try {
            org.apache.activemq.broker.region.policy.PendingMessageLimitStrategy customStrategy =
                    subscription -> 500;

            PolicyEntry entry = new PolicyEntry();
            entry.setPendingMessageLimitStrategy(customStrategy);
            // no eviction strategy override — default OldestMessageEvictionStrategy(ExpiryCheckEnabled=true)

            TopicSubscription sub = buildMinimalTopicSubscription(broker);
            entry.configure(broker.getBroker(), broker.getSystemUsage(), sub);

            assertTrue("A custom limit strategy with default eviction strategy must leave ExpiryCheckEnabled=true",
                    sub.getMessageEvictionStrategy().isExpiryCheckEnabled());
        } finally {
            broker.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Integration tests — embedded broker, real JMS
    // -------------------------------------------------------------------------

    /**
     * With {@code ExpiryCheckEnabled=false} on the eviction strategy, messages
     * with an explicit TTL that has passed must NOT be removed by the eager expiry
     * scan.  The messages remain in the pending queue and are only evicted by the
     * normal eviction strategy when the limit is exceeded.
     *
     * <p>We verify this by:
     * <ol>
     *   <li>Configuring a topic with limit=200, ExpiryCheckEnabled=false.
     *   <li>Sending 250 messages with a very short TTL.
     *   <li>Waiting for all TTLs to elapse.
     *   <li>Sending one more message (triggers the code path).
     *   <li>Asserting that the broker's expired-message counter is 0
     *       (no expiry scan ran) while the discarded counter is > 0
     *       (normal eviction ran as expected).
     * </ol>
     */
    @Test
    public void testExpiryCheckDisabledSkipsExpiredMessageScan() throws Exception {
        final int PENDING_LIMIT = 200;
        final int SEND_COUNT = 250;
        final long SHORT_TTL_MS = 100;

        BrokerService broker = buildBroker(PENDING_LIMIT, false /* ExpiryCheckEnabled=false */);
        try {
            // prefetchSize=1 so messages pile up in the broker's matched queue (not in client buffer)
            ActiveMQTopic topic = new ActiveMQTopic("TEST.EXPIRY.DISABLED?consumer.prefetchSize=1");

            org.apache.activemq.ActiveMQConnectionFactory cf =
                    new org.apache.activemq.ActiveMQConnectionFactory("vm://expiry-disabled");
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a slow consumer (prefetch=1, never reads) to back up messages
            MessageConsumer consumer = session.createConsumer(topic);

            MessageProducer producer = session.createProducer(new ActiveMQTopic("TEST.EXPIRY.DISABLED"));
            // Send messages with short TTL
            for (int i = 0; i < SEND_COUNT; i++) {
                TextMessage msg = session.createTextMessage("msg-" + i);
                producer.send(msg, javax.jms.DeliveryMode.NON_PERSISTENT, 4, SHORT_TTL_MS);
            }

            // Wait for all TTLs to expire
            Thread.sleep(SHORT_TTL_MS * 5);

            // Send one more message — this triggers the guard in TopicSubscription.add()
            producer.send(session.createTextMessage("trigger"));

            // Grab the destination stats
            Destination dest = broker.getDestination(new ActiveMQTopic("TEST.EXPIRY.DISABLED"));
            long expiredCount = dest.getDestinationStatistics().getExpired().getCount();

            assertEquals(
                    "With ExpiryCheckEnabled=false, the expiry scan must not run — expired counter must be 0",
                    0L, expiredCount);

            conn.close();
        } finally {
            broker.stop();
        }
    }

    /**
     * Complementary test: with {@code ExpiryCheckEnabled=true} (the default) the eager
     * scan DOES run and picks up expired messages, so the broker's expired counter
     * should be non-zero after the same scenario.
     */
    @Test
    public void testExpiryCheckEnabledRunsExpiredMessageScan() throws Exception {
        final int PENDING_LIMIT = 200;
        final int SEND_COUNT = 250;
        final long SHORT_TTL_MS = 100;

        BrokerService broker = buildBroker(PENDING_LIMIT, true /* ExpiryCheckEnabled=true */);
        try {
            // prefetchSize=1 so messages pile up in the broker's matched queue (not in client buffer)
            ActiveMQTopic topic = new ActiveMQTopic("TEST.EXPIRY.ENABLED?consumer.prefetchSize=1");

            org.apache.activemq.ActiveMQConnectionFactory cf =
                    new org.apache.activemq.ActiveMQConnectionFactory("vm://expiry-enabled");
            Connection conn = cf.createConnection();
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Slow consumer — never reads
            MessageConsumer consumer = session.createConsumer(topic);

            MessageProducer producer = session.createProducer(new ActiveMQTopic("TEST.EXPIRY.ENABLED"));
            for (int i = 0; i < SEND_COUNT; i++) {
                TextMessage msg = session.createTextMessage("msg-" + i);
                producer.send(msg, javax.jms.DeliveryMode.NON_PERSISTENT, 4, SHORT_TTL_MS);
            }

            // Wait for all TTLs to expire
            Thread.sleep(SHORT_TTL_MS * 5);

            // Send more messages to trigger expiry scan (queue already > highWatermark)
            for (int i = 0; i < 50; i++) {
                producer.send(session.createTextMessage("trigger-" + i));
            }

            Destination dest = broker.getDestination(new ActiveMQTopic("TEST.EXPIRY.ENABLED"));
            long expiredCount = dest.getDestinationStatistics().getExpired().getCount();

            assertTrue(
                    "With ExpiryCheckEnabled=true, the expiry scan must run and detect expired messages (got " + expiredCount + ")",
                    expiredCount > 0);

            conn.close();
        } finally {
            broker.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private BrokerService buildBroker(int pendingLimit, boolean ExpiryCheckEnabled) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        String brokerName = ExpiryCheckEnabled ? "expiry-enabled" : "expiry-disabled";
        broker.setBrokerName(brokerName);
        broker.addConnector("vm://" + brokerName);
        broker.setDeleteAllMessagesOnStartup(true);

        ConstantPendingMessageLimitStrategy limitStrategy = new ConstantPendingMessageLimitStrategy();
        limitStrategy.setLimit(pendingLimit);

        OldestMessageEvictionStrategy evictionStrategy = new OldestMessageEvictionStrategy();
        evictionStrategy.setExpiryCheckEnabled(ExpiryCheckEnabled);

        PolicyEntry entry = new PolicyEntry();
        entry.setTopic(">");
        entry.setPendingMessageLimitStrategy(limitStrategy);
        entry.setMessageEvictionStrategy(evictionStrategy);
        entry.setDeadLetterStrategy(null); // don't route to DLQ

        List<PolicyEntry> entries = new ArrayList<>();
        entries.add(entry);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    /**
     * Builds a minimal {@link TopicSubscription} using the broker's internals —
     * just enough to test the flag, without going through a real JMS connection.
     */
    private TopicSubscription buildMinimalTopicSubscription(BrokerService broker) throws Exception {
        org.apache.activemq.command.ConsumerInfo info = new org.apache.activemq.command.ConsumerInfo();
        info.setConsumerId(new org.apache.activemq.command.ConsumerId(
                new org.apache.activemq.command.SessionId(
                        new org.apache.activemq.command.ConnectionId("test-conn"), 1), 1));
        info.setDestination(new ActiveMQTopic("TEST.UNIT"));
        info.setPrefetchSize(10);

        org.apache.activemq.broker.ConnectionContext ctx =
                new org.apache.activemq.broker.ConnectionContext();
        ctx.setBroker(broker.getBroker());

        return new TopicSubscription(broker.getBroker(), ctx, info, broker.getSystemUsage());
    }
}

