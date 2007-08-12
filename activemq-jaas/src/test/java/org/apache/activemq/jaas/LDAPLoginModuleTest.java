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
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.activemq.jaas.ldap.MutableServerStartupConfiguration;
import org.apache.activemq.jaas.ldap.ServerContextFactory;
import org.apache.ldap.server.configuration.ShutdownConfiguration;
import org.apache.ldap.server.jndi.CoreContextFactory;



/**
 * @version $Rev: $ $Date: $
 */
public class LDAPLoginModuleTest extends TestCase {

    private static final String PRINCIPAL = "uid=admin,ou=system";
    private static final String CREDENTIALS = "secret";

    public void testNothing() {
    }

    @SuppressWarnings("unchecked")
    public void testRunning() throws Exception {

        Hashtable env = new Hashtable();
        env.put(Context.PROVIDER_URL, "ldap://localhost:9389");
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
        env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
        DirContext ctx = new InitialDirContext(env);

        // Perform search using URL
        // NamingEnumeration answer = ctx.search(
        // "ldap://localhost:389/ou=system", "(uid=admin)", null);
        HashSet set = new HashSet();

        NamingEnumeration list = ctx.list("ou=system");

        while (list.hasMore()) {
            NameClassPair ncp = (NameClassPair) list.next();
            set.add(ncp.getName());
        }

        assertTrue(set.contains("uid=admin"));
        assertTrue(set.contains("ou=users"));
        assertTrue(set.contains("ou=groups"));
        assertTrue(set.contains("ou=configuration"));
        assertTrue(set.contains("prefNodeName=sysPrefRoot"));

    }

    public void xtestLogin() throws LoginException {
        LoginContext context = new LoginContext("LDAPLogin", new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (int i = 0; i < callbacks.length; i++) {
                    if (callbacks[i] instanceof NameCallback) {
                        ((NameCallback) callbacks[i]).setName("first");
                    } else if (callbacks[i] instanceof PasswordCallback) {
                        ((PasswordCallback) callbacks[i]).setPassword("secret".toCharArray());
                    } else {
                        throw new UnsupportedCallbackException(callbacks[i]);
                    }
                }
            }
        });
        context.login();
        context.logout();
    }

    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        MutableServerStartupConfiguration startup = new MutableServerStartupConfiguration();
        // put some mandatory JNDI properties here
        startup.setWorkingDirectory(new File("target/ldap"));
        startup.setAllowAnonymousAccess(true);
        startup.setLdapPort(9389);
        startup.setEnableNetworking(true);
        startup.setHost(InetAddress.getByName("localhost"));

        Properties env = new Properties();
        env.putAll(startup.toJndiEnvironment());
        env.put(Context.INITIAL_CONTEXT_FACTORY, ServerContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "ou=system");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
        env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);

        //Fire it up
        new InitialDirContext(env);
    }

    @SuppressWarnings("unchecked")
    public void tearDown() throws Exception {
        Properties env = new Properties();
        env.putAll(new ShutdownConfiguration().toJndiEnvironment());
        env.put(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());
        env.put(Context.PROVIDER_URL, "ou=system");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
        env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);

        //Shut it down
        new InitialDirContext(env);
    }
}
