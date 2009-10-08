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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Hashtable;

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

import org.apache.directory.server.core.integ.Level;
import org.apache.directory.server.core.integ.annotations.ApplyLdifs;
import org.apache.directory.server.core.integ.annotations.CleanupLevel;
import org.apache.directory.server.integ.SiRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.directory.server.ldap.LdapServer;

@RunWith ( SiRunner.class ) 
@CleanupLevel ( Level.CLASS )
@ApplyLdifs( {
	"dn: uid=first,ou=system\n" +
	"uid: first\n" +
	"userPassword: secret\n" +
	"objectClass: account\n" +
	"objectClass: simpleSecurityObject\n" +
	"objectClass: top\n" 
}
)
public class LDAPLoginModuleTest {
	
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
    
    private static final String BASE = "ou=system";
    public static LdapServer ldapServer;
    private static final String FILTER = "(objectclass=*)";
    
    private static final String PRINCIPAL = "uid=admin,ou=system";
    private static final String CREDENTIALS = "secret";
    
    @SuppressWarnings("unchecked")
    @Test
    public void testRunning() throws Exception {

        Hashtable env = new Hashtable();
        env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
        env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
        DirContext ctx = new InitialDirContext(env);

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
    
    @Test
    public void testLogin() throws LoginException {
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

}
