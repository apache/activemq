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
import java.net.URL;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import junit.framework.TestCase;


/**
 * @version $Rev: $ $Date: $
 */
public class GuestLoginModuleTest extends TestCase {

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            URL resource = GuestLoginModuleTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
    }

    public void testLogin() throws LoginException {
        LoginContext context = new LoginContext("GuestLogin", new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                assertEquals("Should have no Callbacks", 0, callbacks.length);
            }
        });
        context.login();

        Subject subject = context.getSubject();

        assertEquals("Should have two principals", 2, subject.getPrincipals().size());
        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertTrue("User principal is 'foo'",subject.getPrincipals(UserPrincipal.class).contains(new UserPrincipal("foo")));

        assertEquals("Should have one group principal", 1, subject.getPrincipals(GroupPrincipal.class).size());
        assertTrue("Group principal is 'bar'", subject.getPrincipals(GroupPrincipal.class).contains(new GroupPrincipal("bar")));

        context.logout();

        assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
    }

    public void testLoginWithDefaults() throws LoginException {
        LoginContext context = new LoginContext("GuestLoginWithDefaults", new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                assertEquals("Should have no Callbacks", 0, callbacks.length);
            }
        });
        context.login();

        Subject subject = context.getSubject();

        assertEquals("Should have two principals", 2, subject.getPrincipals().size());
        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertTrue("User principal is 'guest'",subject.getPrincipals(UserPrincipal.class).contains(new UserPrincipal("guest")));

        assertEquals("Should have one group principal", 1, subject.getPrincipals(GroupPrincipal.class).size());
        assertTrue("Group principal is 'guests'", subject.getPrincipals(GroupPrincipal.class).contains(new GroupPrincipal("guests")));

        context.logout();

        assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
    }
}
