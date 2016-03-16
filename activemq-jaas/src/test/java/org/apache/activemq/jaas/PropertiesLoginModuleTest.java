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

import java.io.File;
import java.io.IOException;
import java.net.URL;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;


/**
 * @version $Rev: $ $Date: $
 */
public class PropertiesLoginModuleTest extends TestCase {

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            URL resource = PropertiesLoginModuleTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
    }

    public void testLogin() throws LoginException {
        LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("first", "secret"));
        context.login();

        Subject subject = context.getSubject();

        assertEquals("Should have three principals", 3, subject.getPrincipals().size());
        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have two group principals", 2, subject.getPrincipals(GroupPrincipal.class).size());

        context.logout();

        assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
    }

    public void testLoginReload() throws Exception {
        File targetPropDir = new File("target/loginReloadTest");
        File sourcePropDir = new File("src/test/resources");
        File usersFile = new File(targetPropDir, "users.properties");
        File groupsFile = new File(targetPropDir, "groups.properties");

        //Set up initial properties
        FileUtils.copyFile(new File(sourcePropDir, "users.properties"), usersFile);
        FileUtils.copyFile(new File(sourcePropDir, "groups.properties"), groupsFile);

        LoginContext context = new LoginContext("PropertiesLoginReload",
                new UserPassHandler("first", "secret"));
        context.login();
        Subject subject = context.getSubject();

        //test initial principals
        assertEquals("Should have three principals", 3, subject.getPrincipals().size());
        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have two group principals", 2, subject.getPrincipals(GroupPrincipal.class).size());

        context.logout();

        assertEquals("Should have zero principals", 0, subject.getPrincipals().size());

        //Modify the file and test that the properties are reloaded
        Thread.sleep(1000);
        FileUtils.copyFile(new File(sourcePropDir, "usersReload.properties"), usersFile);
        FileUtils.copyFile(new File(sourcePropDir, "groupsReload.properties"), groupsFile);
        FileUtils.touch(usersFile);
        FileUtils.touch(groupsFile);

        //Use new password to verify  users file was reloaded
        context = new LoginContext("PropertiesLoginReload", new UserPassHandler("first", "secrets"));
        context.login();
        subject = context.getSubject();

        //Check that the principals changed
        assertEquals("Should have three principals", 2, subject.getPrincipals().size());
        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have one group principals", 1, subject.getPrincipals(GroupPrincipal.class).size());

        context.logout();

        assertEquals("Should have zero principals", 0, subject.getPrincipals().size());
    }

    public void testBadUseridLogin() throws Exception {
        LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("BAD", "secret"));

        try {
            context.login();
            fail("Should have thrown a FailedLoginException");
        } catch (FailedLoginException doNothing) {
        }

    }

    public void testBadPWLogin() throws Exception {
        LoginContext context = new LoginContext("PropertiesLogin", new UserPassHandler("first", "BAD"));

        try {
            context.login();
            fail("Should have thrown a FailedLoginException");
        } catch (FailedLoginException doNothing) {
        }

    }

    private static class UserPassHandler implements CallbackHandler {

        private final String user;
        private final String pass;

        public UserPassHandler(final String user, final String pass) {
            this.user = user;
            this.pass = pass;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof NameCallback) {
                    ((NameCallback) callbacks[i]).setName(user);
                } else if (callbacks[i] instanceof PasswordCallback) {
                    ((PasswordCallback) callbacks[i]).setPassword(pass.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callbacks[i]);
                }
            }
        }
    }
}
