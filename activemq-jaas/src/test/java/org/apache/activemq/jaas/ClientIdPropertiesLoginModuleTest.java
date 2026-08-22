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
        // 'first = first-primary, first-*' — exact match
        login("first", "secret", "first-primary");
    }

    public void testWildcardClientIdAllowed() throws Exception {
        // 'first = ..., first-*' — wildcard match
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
        // 'admin = *' — any clientId permitted
        login("admin", "admin", "anything-goes");
    }

    public void testFallbackRuleAppliesToUserWithoutEntry() throws Exception {
        // 'second' has no explicit entry -> '${userId} = ${userId}-*' -> second-*
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
        assertEquals("no ClientIdPrincipal expected", 0, subject.getPrincipals(ClientIdPrincipal.class).size());
    }

    public void testUserPrincipalFirstClientIdPrincipalSecond() throws Exception {
        var subject = login("first", "secret", "first-primary");

        assertEquals("one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("one clientId principal", 1, subject.getPrincipals(ClientIdPrincipal.class).size());
        assertEquals("clientId principal carries the clientId", "first-primary",
                subject.getPrincipals(ClientIdPrincipal.class).iterator().next().getName());

        var ordered = new ArrayList<>(subject.getPrincipals());
        assertTrue("UserPrincipal must be first", ordered.get(0) instanceof UserPrincipal);
        assertTrue("ClientIdPrincipal must be second", ordered.get(1) instanceof ClientIdPrincipal);
    }

    private Subject login(String user, String pass, String clientId) throws LoginException {
        var context = new LoginContext(LOGIN_MODULE, new UserPassClientIdHandler(user, pass, clientId));
        context.login();
        return context.getSubject();
    }

    private static class UserPassClientIdHandler implements CallbackHandler {

        private final String user;
        private final String pass;
        private final String clientId;

        UserPassClientIdHandler(String user, String pass, String clientId) {
            this.user = user;
            this.pass = pass;
            this.clientId = clientId;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (var callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(user);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(pass.toCharArray());
                } else if (callback instanceof ClientIdCallback) {
                    ((ClientIdCallback) callback).setClientId(clientId);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}
