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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Jakarta Messaging expects createConnection and createContext to authenticate
 * the caller immediately. ActiveMQ defers the ConnectionInfo exchange until
 * first use, so the eager handshake is enabled by strictCompliance.
 */
public class StrictComplianceAuthenticationTest {

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        broker.setPlugins(new BrokerPlugin[] {new SimpleAuthenticationPlugin(users)});
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateConnectionRejectsBadCredentialsImmediately() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        try {
            factory.createConnection("invalid", "credentials");
            fail("Expected JMSSecurityException from createConnection");
        } catch (JMSSecurityException expected) {
        }

        try (Connection connection = factory.createConnection("system", "manager")) {
            connection.start();
            assertNotNull(connection.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateContextRejectsBadCredentialsImmediately() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        try {
            factory.createContext("invalid", "credentials");
            fail("Expected JMSSecurityRuntimeException from createContext");
        } catch (JMSSecurityRuntimeException expected) {
        }

        try (JMSContext context = factory.createContext("system", "manager")) {
            context.start();
            assertNotNull(context.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateConnectionStillAllowsSetClientID() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        // the spec-mandated sequence: set the client identifier immediately after
        // creation, before any other action; eager authentication must not
        // consume the connection's identity
        try (Connection connection = factory.createConnection("system", "manager")) {
            connection.setClientID("strict-client");
            connection.start();
            assertEquals("strict-client", connection.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testLegacyCreateConnectionDefersAuthenticationToStart() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // Default strictCompliance = false: creation succeeds, the failure surfaces
        // when the connection first talks to the broker
        Connection connection = factory.createConnection("invalid", "credentials");
        try {
            connection.start();
            fail("Expected JMSSecurityException on start");
        } catch (JMSSecurityException expected) {
        } catch (JMSException disposed) {
            // the failed connection may be torn down asynchronously before the
            // security exception reaches start(); either form indicates rejection
        } finally {
            try { connection.close(); } catch (Exception ignored) {}
        }
    }
}
