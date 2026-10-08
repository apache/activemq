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
package org.apache.activemq.jaas;

import java.io.IOException;
import java.util.ArrayList;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

/**
 * Verifies the optional clientId authentication of {@link PropertiesLoginModule}:
 * a connection's clientId is authorized together with the user credentials.
 */
public class ClientIdPropertiesLoginModuleTest extends TestCase {

    private static final String LOGIN_MODULE = "PropertiesLoginClientId";
    private static final String CONNECTION_ID = "ID:test-host-61616-1-1:1";
    private static final String REMOTE_ADDRESS = "tcp://127.0.0.1:52000";
    private static final String CONNECTOR_NAME = "openwire";

    static {
        var path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            var resource = ClientIdPropertiesLoginModuleTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                System.setProperty("java.security.auth.login.config", resource.getFile());
            }
        }
    }

    public void testExplicitClientIdAllowed() throws Exception {
        // 'first = false, first-primary, first-*' — exact match
        login("first", "secret", "first-primary");
    }

    public void testWildcardClientIdAllowed() throws Exception {
        // 'first = false, ..., first-*' — wildcard match
        login("first", "secret", "first-42");
    }

    public void testClientIdNotAllowedFailsLogin() throws Exception {
        // 'first' is confined to first-*; a foreign clientId must fail login
        try {
            login("first", "secret", "quote-1");
            fail("Should have thrown a FailedLoginException for a disallowed clientId");
        } catch (FailedLoginException expected) {
        }
    }

    public void testWildcardUserAllowedAnyClientId() throws Exception {
        // 'admin = false, *' — any clientId permitted
        login("admin", "admin", "anything-goes");
    }

    public void testFallbackRuleAppliesToUserWithoutEntry() throws Exception {
        // 'second' has no explicit entry -> '${userId} = false, ${userId}-*' -> second-*
        login("second", "password", "second-1");
    }

    public void testFallbackRuleRejectsForeignPrefix() throws Exception {
        try {
            login("second", "password", "first-1");
            fail("Should have thrown a FailedLoginException; second may only use second-*");
        } catch (FailedLoginException expected) {
        }
    }

    public void testNoClientIdAllowed() throws Exception {
        // a connection that presents no clientId still authenticates (no durable ownership)
        var subject = login("first", "secret", null);
        var connection = subject.getPrincipals(ConnectionPrincipal.class).iterator().next();
        assertEquals("no clientId recorded", null, connection.getClientId());
        assertFalse(connection.isNetworkConnection());
    }

    public void testUserPrincipalFirstConnectionPrincipalSecond() throws Exception {
        var subject = login("first", "secret", "first-primary");

        assertEquals("one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("one connection principal", 1, subject.getPrincipals(ConnectionPrincipal.class).size());
        var connection = subject.getPrincipals(ConnectionPrincipal.class).iterator().next();
        assertEquals("connection principal is named by the connection id", CONNECTION_ID, connection.getName());
        assertEquals("connection principal carries the clientId", "first-primary", connection.getClientId());
        assertTrue("connection principal carries the ssl flag", connection.isSsl());
        assertEquals("connection principal carries the remote address", REMOTE_ADDRESS, connection.getRemoteAddress());
        assertEquals("connection principal carries the connector name", CONNECTOR_NAME, connection.getTransportConnectorName());

        var ordered = new ArrayList<>(subject.getPrincipals());
        assertTrue("UserPrincipal must be first", ordered.get(0) instanceof UserPrincipal);
        assertTrue("ConnectionPrincipal must be second", ordered.get(1) instanceof ConnectionPrincipal);
    }

    public void testNetworkConnectionAllowedForFlaggedUser() throws Exception {
        // 'system = true, ...' and NC_*_outbound is an allowed clientId
        var subject = login("system", "manager", "NC_broker2_outbound", "broker1", true);
        assertTrue("decision must be recorded as allowed", networkDecision(subject));
    }

    public void testNetworkConnectionDeniedWithoutFlag() throws Exception {
        // 'first = false, ...' has an allowed clientId but may not register a network connection
        try {
            login("first", "secret", "first-primary", "broker1", true);
            fail("Should have thrown a FailedLoginException; first may not register a network connection");
        } catch (FailedLoginException expected) {
        }
    }

    public void testWildcardClientIdDoesNotGrantNetworkConnection() throws Exception {
        // 'admin = false, *' permits any clientId, but the leading false denies network connections
        try {
            login("admin", "admin", "NC_broker2_outbound", "broker1", true);
            fail("Should have thrown a FailedLoginException; admin rule starts with false");
        } catch (FailedLoginException expected) {
        }
    }

    public void testApplicationConnectionUnaffectedByNetworkFlag() throws Exception {
        // a flagged user is still free to make an ordinary application connection
        var subject = login("system", "manager", null, "broker1", false);
        assertTrue(networkDecision(subject));
    }

    public void testBrokerNameMacroExpandsToAuthenticatingBroker() throws Exception {
        // NC_*_inbound_${brokerName} with brokerName=broker1
        login("system", "manager", "NC_broker2_inbound_broker1", "broker1", true);
        try {
            login("system", "manager", "NC_broker2_inbound_broker9", "broker1", true);
            fail("Should have thrown a FailedLoginException; inbound clientId names another broker");
        } catch (FailedLoginException expected) {
        }
    }

    public void testNetworkDecisionRecordedAsDeniedByDefault() throws Exception {
        // the principal is always present when clientId authorization is on, so a
        // connection that later declares itself a network connection can be held to it
        var subject = login("second", "password", "second-1");
        assertFalse(networkDecision(subject));
    }

    public void testEntryWithoutLeadingBooleanDeniesClientIds() throws Exception {
        // 'broken = broken-*' has no leading true|false and is ignored as a whole
        try {
            login("broken", "broken", "broken-1");
            fail("Should have thrown a FailedLoginException; the malformed rule must not allow any clientId");
        } catch (FailedLoginException expected) {
        }
    }

    public void testEntryWithoutLeadingBooleanDeniesNetworkConnection() throws Exception {
        try {
            login("broken", "broken", null, "broker1", true);
            fail("Should have thrown a FailedLoginException; the malformed rule must not allow a network connection");
        } catch (FailedLoginException expected) {
        }
        // credentials alone still work for an application connection without a clientId
        assertFalse(networkDecision(login("broken", "broken", null)));
    }

    private static boolean networkDecision(Subject subject) {
        var connections = subject.getPrincipals(ConnectionPrincipal.class);
        assertEquals("exactly one connection principal expected", 1, connections.size());
        return connections.iterator().next().isNetworkConnection();
    }

    private Subject login(String user, String pass, String clientId) throws LoginException {
        return login(user, pass, clientId, null, false);
    }

    private Subject login(String user, String pass, String clientId, String brokerName, boolean networkConnection) throws LoginException {
        var context = new LoginContext(LOGIN_MODULE,
                new UserPassClientIdHandler(user, pass, clientId, brokerName, networkConnection));
        context.login();
        return context.getSubject();
    }

    private static class UserPassClientIdHandler implements CallbackHandler {

        private final String user;
        private final String pass;
        private final String clientId;
        private final String brokerName;
        private final boolean networkConnection;

        UserPassClientIdHandler(String user, String pass, String clientId, String brokerName, boolean networkConnection) {
            this.user = user;
            this.pass = pass;
            this.clientId = clientId;
            this.brokerName = brokerName;
            this.networkConnection = networkConnection;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (var callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(user);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(pass.toCharArray());
                } else if (callback instanceof ConnectionCallback) {
                    var connection = (ConnectionCallback) callback;
                    connection.setConnectionId(CONNECTION_ID);
                    connection.setClientId(clientId);
                    connection.setSsl(true);
                    connection.setRemoteAddress(REMOTE_ADDRESS);
                    connection.setTransportConnectorName(CONNECTOR_NAME);
                    connection.setBrokerName(brokerName);
                    connection.setNetworkConnection(networkConnection);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}
