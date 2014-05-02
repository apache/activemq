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
package org.apache.activemq;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

public class AuthorizationTest extends RuntimeConfigTestSupport {

    private static final int RECEIVE_TIMEOUT = 1000;
    String configurationSeed = "authorizationTest";

    @Test
    public void testMod() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");

        assertDeniedTemp("guest");

        applyNewConfig(brokerConfig, configurationSeed + "-users-guests", SLEEP);

        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");

        assertAllowedTemp("guest");
    }

    @Test
    public void testModRm() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-rm-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users-guests");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");
        assertAllowedTemp("guest");

        applyNewConfig(brokerConfig, configurationSeed + "-users", SLEEP);

        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");
        assertDeniedTemp("guest");
    }

    @Test
    public void testWildcard() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-wildcard-users-guests");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        final String ALL_USERS = "ALL.USERS";
        final String ALL_GUESTS = "ALL.GUESTS";

        assertAllowed("user", ALL_USERS);
        assertAllowed("guest", ALL_GUESTS);
        assertDenied("user", ALL_USERS + "," + ALL_GUESTS);
        assertDenied("guest", ALL_GUESTS + "," + ALL_USERS);

        final String ALL_PREFIX = "ALL.>";
        final String ALL_WILDCARD = "ALL.*";

        assertAllowed("user", ALL_PREFIX);
        assertAllowed("user", ALL_WILDCARD);
        assertAllowed("guest", ALL_PREFIX);
        assertAllowed("guest", ALL_WILDCARD);

        assertAllowed("user", "ALL.USERS,ALL.>");
        assertAllowed("guest", "ALL.GUESTS,ALL.*");
        assertDenied("user", "ALL.GUESTS,ALL.>");
        assertDenied("guest", "ALL.USERS,ALL.*");

        assertDenied("user", "ALL.USERS,ALL.GUESTS.>");
        assertDenied("guest", "ALL.GUESTS,ALL.USERS.*");
        assertDenied("user", "ALL.USERS.*,ALL.GUESTS.>");
        assertDenied("guest", "ALL.GUESTS.>,ALL.USERS.*");

        // subscribe to wildcards and check whether messages are actually filtered
        final ActiveMQConnection userConn = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection("user", "user");
        final ActiveMQConnection guestConn = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection("guest", "guest");
        userConn.start();
        guestConn.start();
        try {
            final Session userSession = userConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Session guestSession = guestConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer userProducer = userSession.createProducer(null);
            final MessageProducer guestProducer = guestSession.createProducer(null);

            // test prefix filter
            MessageConsumer userConsumer = userSession.createConsumer(userSession.createQueue(ALL_PREFIX));
            MessageConsumer guestConsumer = guestSession.createConsumer(userSession.createQueue(ALL_PREFIX));

            userProducer.send(userSession.createQueue(ALL_USERS), userSession.createTextMessage(ALL_USERS));
            assertNotNull(userConsumer.receive(RECEIVE_TIMEOUT));
            assertNull(guestConsumer.receive(RECEIVE_TIMEOUT));

            guestProducer.send(guestSession.createQueue(ALL_GUESTS), guestSession.createTextMessage(ALL_GUESTS));
            assertNotNull(guestConsumer.receive(RECEIVE_TIMEOUT));
            assertNull(userConsumer.receive(RECEIVE_TIMEOUT));

            userConsumer.close();
            guestConsumer.close();

            // test wildcard filter
            userConsumer = userSession.createConsumer(userSession.createQueue(ALL_WILDCARD));
            guestConsumer = guestSession.createConsumer(userSession.createQueue(ALL_WILDCARD));

            userProducer.send(userSession.createQueue(ALL_USERS), userSession.createTextMessage(ALL_USERS));
            assertNotNull(userConsumer.receive(RECEIVE_TIMEOUT));
            assertNull(guestConsumer.receive(RECEIVE_TIMEOUT));

            guestProducer.send(guestSession.createQueue(ALL_GUESTS), guestSession.createTextMessage(ALL_GUESTS));
            assertNotNull(guestConsumer.receive(RECEIVE_TIMEOUT));
            assertNull(userConsumer.receive(RECEIVE_TIMEOUT));

        } finally {
            userConn.close();
            guestConn.close();
        }

        assertAllowedTemp("guest");
    }

    private void assertDeniedTemp(String userPass) {
        try {
            assertAllowedTemp(userPass);
            fail("Expected not allowed exception");
        } catch (Exception expected) {
            LOG.debug("got:" + expected, expected);
        }
    }

    private void assertAllowedTemp(String userPass) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTemporaryQueue());
        } finally {
            connection.close();
        }

    }

    private void assertDenied(String userPass, String destination) {
        try {
            assertAllowed(userPass, destination);
            fail("Expected not allowed exception");
        } catch (JMSException expected) {
            LOG.debug("got:" + expected, expected);
        }
    }

    private void assertAllowed(String userPass, String dest) throws JMSException {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(dest));
        } finally {
            connection.close();
        }
    }

}
