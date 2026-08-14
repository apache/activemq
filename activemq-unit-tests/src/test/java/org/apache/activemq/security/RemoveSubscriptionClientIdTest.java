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
package org.apache.activemq.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

/**
 * A durable subscription is identified by (clientID, subscription name) and, per the
 * Jakarta Messaging specification, {@code Session.unsubscribe(String)} only names the
 * subscription - the clientID half of the identity is always the calling connection's
 * own. A connection therefore has no legitimate way to unsubscribe a durable
 * subscription owned by a different clientID.
 *
 * These tests exercise the broker's remove-subscription command handling directly with a
 * crafted {@link RemoveSubscriptionInfo} to confirm that (a) a connection may remove its
 * own durable subscription and (b) a connection may not remove another clientID's durable
 * subscription.
 */
@Category(ParallelTest.class)
public class RemoveSubscriptionClientIdTest {

    private static final String VICTIM_CLIENT_ID = "victim-client";
    private static final String ATTACKER_CLIENT_ID = "attacker-client";
    private static final String SUBSCRIPTION_NAME = "durable-sub";

    private BrokerService broker;
    private String connectionUri;
    private final ActiveMQTopic topic = new ActiveMQTopic("SecuredTopic");

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setPersistent(false);
        broker.setDeleteAllMessagesOnStartup(true);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private ActiveMQConnection createConnection(String clientId) throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        var connection = (ActiveMQConnection) factory.createConnection();
        connection.setClientID(clientId);
        connection.start();
        return connection;
    }

    /**
     * Register a durable subscription for the victim clientID, then take it offline so it
     * exists as an inactive durable subscription in the broker.
     */
    private void createInactiveVictimSubscription() throws Exception {
        try (var victim = createConnection(VICTIM_CLIENT_ID);
             var session = victim.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createDurableSubscriber(topic, SUBSCRIPTION_NAME)) {
            // register the durable subscription; closing the resources below leaves it inactive
        }

        assertTrue("victim durable subscription should exist and be inactive",
            Wait.waitFor(() -> broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1,
                5000, 10));
    }

    /**
     * Positive control: a connection removing its OWN durable subscription must succeed,
     * so a fix that simply rejected every remove-subscription request would not pass.
     */
    @Test(timeout = 60 * 1000)
    public void testRemoveOwnSubscriptionSucceeds() throws Exception {
        createInactiveVictimSubscription();

        try (var victim = createConnection(VICTIM_CLIENT_ID)) {
            var rsi = new RemoveSubscriptionInfo();
            rsi.setConnectionId(victim.getConnectionInfo().getConnectionId());
            rsi.setClientId(VICTIM_CLIENT_ID);
            rsi.setSubscriptionName(SUBSCRIPTION_NAME);
            victim.syncSendPacket(rsi);
        }

        assertEquals("owner should be able to remove its own durable subscription",
            0, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
    }

    /**
     * Security path: an authenticated connection using its own valid connectionId but a
     * FOREIGN clientID must not be able to delete another clientID's durable subscription.
     * The victim's subscription must survive the crafted request.
     */
    @Test(timeout = 60 * 1000)
    public void testRemoveSubscriptionForForeignClientIdIsRejected() throws Exception {
        createInactiveVictimSubscription();

        try (var attacker = createConnection(ATTACKER_CLIENT_ID)) {
            var rsi = new RemoveSubscriptionInfo();
            // attacker's own connectionId - resolves a valid ConnectionContext at the broker
            rsi.setConnectionId(attacker.getConnectionInfo().getConnectionId());
            // but names the victim's subscription identity
            rsi.setClientId(VICTIM_CLIENT_ID);
            rsi.setSubscriptionName(SUBSCRIPTION_NAME);

            try {
                attacker.syncSendPacket(rsi);
            } catch (JMSException rejected) {
                // A hardened broker may reject the mismatched request outright - acceptable.
            }
        }

        assertEquals("a foreign clientID must not be able to remove the victim's durable subscription",
            1, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
    }

    /**
     * A remove-subscription command whose clientId differs from the connection's own
     * clientId has no legitimate cause, so the broker logs it at WARN to help detect
     * and troubleshoot a crafted or misbehaving client.
     */
    @Test(timeout = 60 * 1000)
    public void testForeignClientIdRemovalLogsWarning() throws Exception {
        createInactiveVictimSubscription();

        final var warned = new AtomicBoolean(false);
        var appender = new DefaultTestAppender() {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel().equals(Level.WARN)
                        && event.getMessage().getFormattedMessage().contains("Ignoring clientId")
                        && event.getMessage().getFormattedMessage().contains(VICTIM_CLIENT_ID)) {
                    warned.set(true);
                }
            }
        };
        appender.start();

        var log4jLogger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(TopicRegion.class);
        log4jLogger.addAppender(appender);

        try {
            try (var attacker = createConnection(ATTACKER_CLIENT_ID)) {
                var rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(attacker.getConnectionInfo().getConnectionId());
                rsi.setClientId(VICTIM_CLIENT_ID);
                rsi.setSubscriptionName(SUBSCRIPTION_NAME);
                try {
                    attacker.syncSendPacket(rsi);
                } catch (JMSException expected) {
                    // no subscription exists under the attacker's own clientId
                }
            }

            assertTrue("a clientId mismatch on remove-subscription should be logged at WARN",
                Wait.waitFor(() -> warned.get(), 5000, 10));
        } finally {
            log4jLogger.removeAppender(appender);
        }
    }
}
