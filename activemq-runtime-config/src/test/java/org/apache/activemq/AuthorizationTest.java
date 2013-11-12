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
import javax.jms.Session;
import org.junit.Test;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationTest extends RuntimeConfigTestSupport {

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
