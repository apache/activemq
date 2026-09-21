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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * [AMQ-9692] Verify destination GC behavior for topics whose only
 * subscription is a durable *wildcard* subscriber.
 *
 * Durable subscription state (registration + pending messages) is kept in the
 * per-topic message store, and TopicRegion.addSubscriptionsForDestination()
 * recovers durable subs from that store when a destination is (re)created.
 * Destination GC destroys the store, so these tests probe whether the
 * durable-subscription guarantees survive the gc + recreate cycle.
 */
@Category(ParallelTest.class)
public class DestinationGCWildcardDurableSubTest {

    private static final String CLIENT_ID = "durable-wildcard-client";
    private static final String SUB_NAME = "durable-wildcard-sub";
    private static final ActiveMQTopic WILDCARD_TOPIC = new ActiveMQTopic("TEST.DUR.>");
    private static final ActiveMQTopic TOPIC_A = new ActiveMQTopic("TEST.DUR.A");

    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(true);
    }

    protected BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        return createBroker(deleteAllMessagesOnStartup, true);
    }

    protected BrokerService createBroker(boolean deleteAllMessagesOnStartup, boolean gcWithOnlyWildcardConsumers) throws Exception {
        var entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setGcWithOnlyWildcardConsumers(gcWithOnlyWildcardConsumers);
        entry.setInactiveTimeoutBeforeGC(1000);
        var map = new PolicyMap();
        map.setDefaultEntry(entry);

        var broker = new BrokerService();
        // Persistent so durable subscription state goes through a real store
        broker.setPersistent(true);
        broker.setDataDirectory("target/activemq-data/DestinationGCWildcardDurableSubTest");
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);
        broker.setUseJmx(true);
        broker.setSchedulePeriodForDestinationPurge(500);
        broker.setDestinationPolicy(map);
        return broker;
    }

    private Connection createConnection() throws Exception {
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        var connection = factory.createConnection();
        connection.setClientID(CLIENT_ID);
        connection.start();
        return connection;
    }

    private int countMatchingTopics() throws Exception {
        var count = 0;
        for (var name : brokerService.getAdminView().getTopics()) {
            var destinationName = name.getKeyProperty("destinationName");
            if (destinationName != null && destinationName.startsWith("TEST.DUR.")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Asserts the topic survives several gc sweep periods. The sweep runs every
     * 500ms with a 1000ms inactive timeout, so 4s covers multiple full
     * mark-and-collect cycles.
     */
    private void assertTopicNotGcd() throws Exception {
        Thread.sleep(4000);
        assertEquals("Topic with a durable subscription must not be gc'd", 1, countMatchingTopics());
    }

    /**
     * A durable subscription's registration and pending messages live in the
     * topic's store, which gc destroys. An active durable wildcard subscriber
     * must therefore prevent the matched topic from being gc'd, and delivery
     * must continue working.
     */
    @Test(timeout = 60000)
    public void testActiveDurableWildcardSubPreventsTopicGc() throws Exception {
        // Anonymous producer - does not register on the destination, so the gc
        // assertions exercise only the durable consumer
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME);
             var producer = session.createProducer(null)) {

            producer.send(TOPIC_A, session.createTextMessage("first"));

            var received = durableSubscriber.receive(5000);
            assertNotNull("Durable wildcard sub should receive first message", received);
            assertEquals("first", ((TextMessage) received).getText());

            // Topic is drained but has a durable wildcard consumer - must not gc
            assertTopicNotGcd();

            producer.send(TOPIC_A, session.createTextMessage("second"));

            received = durableSubscriber.receive(5000);
            assertNotNull("Durable wildcard sub should continue receiving messages", received);
            assertEquals("second", ((TextMessage) received).getText());

            durableSubscriber.close();
            session.unsubscribe(SUB_NAME);
        }
    }

    /**
     * A topic holding a message pending for an offline durable wildcard
     * subscriber must NOT be gc'd - the pending message keeps the destination
     * message count non-zero.
     */
    @Test(timeout = 60000)
    public void testOfflineDurableWildcardSubWithPendingMessageNotGcd() throws Exception {
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(TOPIC_A)) {

            session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME).close();

            // Subscriber offline (closed); send a message it should receive later
            producer.send(session.createTextMessage("pending"));
        }

        // Give the gc sweep several periods to (incorrectly) collect the topic
        assertTopicNotGcd();

        // Reconnect and confirm the pending message is delivered
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME)) {

            var received = durableSubscriber.receive(5000);
            assertNotNull("Offline durable sub should receive pending message on reconnect", received);
            assertEquals("pending", ((TextMessage) received).getText());

            durableSubscriber.close();
            session.unsubscribe(SUB_NAME);
        }
    }

    /**
     * Durability across broker restart: the durable wildcard sub's registration is
     * recovered from the topic's store at startup as an INACTIVE subscription -
     * counted on the destination, but not present in its consumers list. The
     * recovered topic must not be gc'd, and a message sent before the subscriber
     * reconnects must be delivered.
     */
    @Test(timeout = 60000)
    public void testOfflineDurableWildcardSubSurvivesBrokerRestart() throws Exception {
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME);
             var producer = session.createProducer(TOPIC_A)) {

            producer.send(session.createTextMessage("before-restart"));
            assertNotNull(durableSubscriber.receive(5000));
        }

        // Restart without wiping the store - the topic is recovered with an
        // inactive durable subscription
        brokerService.stop();
        brokerService.waitUntilStopped();
        brokerService = createBroker(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        // The recovered topic holds the durable registration - must not gc
        assertTopicNotGcd();

        // Send while the durable sub is still offline
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(TOPIC_A)) {
            producer.send(session.createTextMessage("while-offline-after-restart"));
        }

        // Reconnect - durability requires the message survive the restart + gc sweeps
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME)) {

            var received = durableSubscriber.receive(5000);
            assertNotNull("Message sent while durable sub was offline across a restart must be delivered", received);
            assertEquals("while-offline-after-restart", ((TextMessage) received).getText());

            durableSubscriber.close();
            session.unsubscribe(SUB_NAME);
        }
    }

    /**
     * Pre-change behavior: with gcWithOnlyWildcardConsumers disabled (the default),
     * a durable wildcard subscriber keeps its matched topics from being gc'd -
     * active or offline - and durability holds. Once the subscription is removed
     * entirely, gcInactiveDestinations collects the abandoned topic as before.
     * Also documents the registration model: the durable sub is persisted in each
     * concrete matching topic's store, recorded under its wildcard destination -
     * no destination or store exists for the wildcard name itself.
     */
    @Test(timeout = 60000)
    public void testDurableWildcardSubWithGcWildcardDisabled() throws Exception {
        // Swap in a broker with the wildcard gc flag OFF (pre-change / default config)
        brokerService.stop();
        brokerService.waitUntilStopped();
        brokerService = createBroker(true, false);
        brokerService.start();
        brokerService.waitUntilStarted();

        // Anonymous producer - does not register on the destination, so the gc
        // assertions exercise only the durable consumer
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME);
             var producer = session.createProducer(null)) {

            producer.send(TOPIC_A, session.createTextMessage("first"));
            assertNotNull(durableSubscriber.receive(5000));

            // The durable sub is registered in the concrete topic's store under its
            // wildcard subscribed destination
            var store = (TopicMessageStore) brokerService.getDestination(TOPIC_A).getMessageStore();
            var subscriptions = store.getAllSubscriptions();
            assertEquals(1, subscriptions.length);
            assertEquals(WILDCARD_TOPIC, subscriptions[0].getSubscribedDestination());

            // Active durable wildcard sub - topic must not gc with the flag off
            assertTopicNotGcd();
        }

        // Offline durable wildcard sub - topic must still not gc
        assertTopicNotGcd();

        // Send while the durable sub is offline
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(TOPIC_A)) {
            producer.send(session.createTextMessage("while-offline"));
        }

        // Reconnect - durability held
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME)) {

            var received = durableSubscriber.receive(5000);
            assertNotNull("Message sent while durable sub was offline must be delivered", received);
            assertEquals("while-offline", ((TextMessage) received).getText());

            // Remove the subscription entirely - the topic is now truly abandoned
            durableSubscriber.close();
            session.unsubscribe(SUB_NAME);
        }

        // With no durable registration left, gcInactiveDestinations collects the topic
        assertTrue("Abandoned topic should be gc'd once the durable subscription is removed",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return countMatchingTopics() == 0;
                    }
                }, 15000, 500));
    }

    /**
     * Durability guarantee: durable wildcard sub goes offline with the topic
     * fully drained. The empty topic must NOT be gc'd (its store holds the
     * durable registration), and a message sent while the subscriber is offline
     * MUST be delivered when it reconnects.
     */
    @Test(timeout = 60000)
    public void testOfflineDurableWildcardSubEmptyTopicNotGcdAndDeliversOnReconnect() throws Exception {
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME);
             var producer = session.createProducer(TOPIC_A)) {

            producer.send(session.createTextMessage("before-offline"));
            assertNotNull(durableSubscriber.receive(5000));
        }

        // Topic is drained and the durable wildcard sub is offline - must not gc
        assertTopicNotGcd();

        // Send a message while the durable sub is offline
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(TOPIC_A)) {
            producer.send(session.createTextMessage("while-offline"));
        }

        // Reconnect the durable subscriber - durability requires the message arrive
        try (var connection = createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var durableSubscriber = session.createDurableSubscriber(WILDCARD_TOPIC, SUB_NAME)) {

            var received = durableSubscriber.receive(5000);
            assertNotNull("Message sent while durable sub was offline must be delivered on reconnect", received);
            assertEquals("while-offline", ((TextMessage) received).getText());

            durableSubscriber.close();
            session.unsubscribe(SUB_NAME);
        }
    }
}
