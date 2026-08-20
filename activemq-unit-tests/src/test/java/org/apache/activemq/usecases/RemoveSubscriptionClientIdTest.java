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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * These tests exercise the broker's remove-subscription command handling directly with a
 * crafted {@link RemoveSubscriptionInfo} to confirm that (a) a connection may remove its
 * own durable subscription and (b) a connection may not remove another clientID's durable
 * subscription.
 */
public class RemoveSubscriptionClientIdTest {

    private static final String FIRST_CLIENT_ID = "first-client";
    private static final String SECOND_CLIENT_ID = "second-client";
    private static final String SUBSCRIPTION_NAME = "durable-sub";

    private BrokerService broker;
    private String connectionUri;
    private final ActiveMQTopic topic = new ActiveMQTopic("SomeTopic");

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
     * Register a durable subscription for the first clientID, then take it offline so it
     * exists as an inactive durable subscription in the broker.
     */
    private void createInactiveFirstSubscription() throws Exception {
        try (var first = createConnection(FIRST_CLIENT_ID);
             var session = first.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createDurableSubscriber(topic, SUBSCRIPTION_NAME)) {
            // register the durable subscription; closing the resources below leaves it inactive
        }

        assertTrue("first durable subscription should exist and be inactive",
            Wait.waitFor(() -> broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1,
                5000, 10));
    }

    /**
     * A connection removing its own durable subscription must succeed
     */
    @Test(timeout = 60 * 1000)
    public void testRemoveOwnSubscriptionSucceeds() throws Exception {
        createInactiveFirstSubscription();

        try (var first = createConnection(FIRST_CLIENT_ID)) {
            var rsi = new RemoveSubscriptionInfo();
            rsi.setConnectionId(first.getConnectionInfo().getConnectionId());
            rsi.setClientId(FIRST_CLIENT_ID);
            rsi.setSubscriptionName(SUBSCRIPTION_NAME);
            first.syncSendPacket(rsi);
        }

        assertEquals("owner should be able to remove its own durable subscription",
            0, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
    }

    /**
     * A foreign clientID must not be able to delete another clientID's durable subscription.
     */
    @Test(timeout = 60 * 1000)
    public void testRemoveSubscriptionForForeignClientIdIsIgnored() throws Exception {
        createInactiveFirstSubscription();

        try (var second = createConnection(SECOND_CLIENT_ID)) {
            var rsi = new RemoveSubscriptionInfo();
            rsi.setConnectionId(second.getConnectionInfo().getConnectionId());
            // but names the first subscription clientId
            rsi.setClientId(FIRST_CLIENT_ID);
            rsi.setSubscriptionName(SUBSCRIPTION_NAME);

            try {
                second.syncSendPacket(rsi);
            } catch (JMSException rejected) {
                // A hardened broker may reject the mismatched request outright - acceptable.
            }
        }

        assertEquals("a foreign clientID must not be able to remove the another's durable subscription",
            1, broker.getAdminView().getInactiveDurableTopicSubscribers().length);
    }
}
