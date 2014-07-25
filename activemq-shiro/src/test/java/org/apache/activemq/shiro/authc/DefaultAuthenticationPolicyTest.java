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
package org.apache.activemq.shiro.authc;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.shiro.subject.SubjectAdapter;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.shiro.env.DefaultEnvironment;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class DefaultAuthenticationPolicyTest {

    private DefaultAuthenticationPolicy policy;

    @Before
    public void setUp() {
        this.policy = new DefaultAuthenticationPolicy();
    }

    @Test
    public void testVmConnectionAuthenticationRequired() {
        boolean required = true;
        policy.setVmConnectionAuthenticationRequired(required);
        assertEquals(required, policy.isVmConnectionAuthenticationRequired());
    }

    @Test
    public void testSystemAccountUsername() {
        String name = "foo";
        policy.setSystemAccountUsername(name);
        assertEquals(name, policy.getSystemAccountUsername());
    }

    @Test
    public void testSystemAccountRealmName() {
        String name = "fooRealm";
        policy.setSystemAccountRealmName(name);
        assertEquals(name, policy.getSystemAccountRealmName());
    }

    @Test
    public void testAnonymousAllowed() {
        boolean allowed = true;
        policy.setAnonymousAccessAllowed(allowed);
        assertEquals(allowed, policy.isAnonymousAccessAllowed());
    }

    @Test
    public void testAnonymousAccountUsername() {
        String name = "blah";
        policy.setAnonymousAccountUsername(name);
        assertEquals(name, policy.getAnonymousAccountUsername());
    }

    @Test
    public void testAnonymousAccountRealmName() {
        String name = "blahRealm";
        policy.setAnonymousAccountRealmName(name);
        assertEquals(name, policy.getAnonymousAccountRealmName());
    }

    @Test
    public void testIsAnonymousAccount() {
        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("anonymous", "iniRealm");
            }
        };
        assertTrue(policy.isAnonymousAccount(subject));
    }

    @Test
    public void testIsAnonymousAccountWithNullPrincipals() {
        assertFalse(policy.isAnonymousAccount(new SubjectAdapter()));
    }

    @Test
    public void testIsSystemAccountWithNullPrincipals() {
        assertFalse(policy.isSystemAccount(new SubjectAdapter()));
    }

    @Test
    public void testIsAuthenticationRequiredWhenAlreadyRequired() {
        Subject subject = new SubjectAdapter() {
            @Override
            public boolean isAuthenticated() {
                return true;
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertFalse(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAuthenticationRequiredWhenAnonymousAllowedAnonymousSubject() {

        policy.setAnonymousAccessAllowed(true);

        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("anonymous", "iniRealm");
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertFalse(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAuthenticationRequiredWhenAnonymousAllowedAndNotAnonymousSubject() {

        policy.setAnonymousAccessAllowed(true);

        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("system", "iniRealm");
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertFalse(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAuthenticationRequiredWhenSystemConnectionAndSystemSubject() {

        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("system", "iniRealm");
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertFalse(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAuthenticationRequiredWhenSystemConnectionRequiresAuthentication() {

        policy.setVmConnectionAuthenticationRequired(true);

        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("system", "iniRealm");
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertTrue(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAuthenticationRequiredWhenSystemConnectionDoesNotRequireAuthenticationAndNotSystemAccount() {

        Subject subject = new SubjectAdapter() {
            @Override
            public PrincipalCollection getPrincipals() {
                return new SimplePrincipalCollection("foo", "iniRealm");
            }
        };
        SubjectConnectionReference sc = new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                new DefaultEnvironment(), subject);

        assertTrue(policy.isAuthenticationRequired(sc));
    }

    @Test
    public void testIsAssumeIdentity() {
        policy.setAnonymousAccessAllowed(true);
        assertTrue(policy.isAssumeIdentity(null));
    }

    @Test
    public void testIsAssumeIdentityWithSystemConnection() {

        ConnectionContext ctx = new ConnectionContext();
        Connection connection = new Connection() {
            @Override
            public Connector getConnector() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void dispatchSync(Command message) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void dispatchAsync(Command command) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Response service(Command command) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void serviceException(Throwable error) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isSlow() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isBlocked() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isConnected() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isActive() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public int getDispatchQueueSize() {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public ConnectionStatistics getStatistics() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isManageable() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public String getRemoteAddress() {
                return "vm://localhost";
            }

            @Override
            public void serviceExceptionAsync(IOException e) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public String getConnectionId() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isNetworkConnection() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isFaultTolerantConnection() {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void updateClient(ConnectionControl control) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void start() throws Exception {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void stop() throws Exception {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public int getActiveTransactionCount() {
                return 0;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public Long getOldestActiveTransactionDuration() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        };

        ctx.setConnection(connection);

        SubjectConnectionReference sc = new SubjectConnectionReference(ctx, new ConnectionInfo(),
                new DefaultEnvironment(), new SubjectAdapter());

        assertTrue(policy.isAssumeIdentity(sc));
    }
}
