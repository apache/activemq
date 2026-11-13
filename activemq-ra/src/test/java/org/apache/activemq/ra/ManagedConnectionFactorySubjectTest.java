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
package org.apache.activemq.ra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.resource.ResourceException;
import jakarta.resource.spi.ManagedConnection;
import jakarta.resource.spi.ManagedConnectionFactory;
import jakarta.resource.spi.security.PasswordCredential;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks that the credentials given by the application server in the {@link Subject}
 * (container-managed sign-on) are used to create and match the managed connections.
 */
public class ManagedConnectionFactorySubjectTest {

    private static final String USER = "subjectUser";
    private static final String PASSWORD = "subjectPassword";
    private static final String OTHER_USER = "otherUser";
    private static final String OTHER_PASSWORD = "otherPassword";

    private BrokerService broker;
    private ActiveMQManagedConnectionFactory managedConnectionFactory;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("subject-test");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setSchedulerSupport(false);
        // accepts only the two known users
        broker.setPlugins(new BrokerPlugin[] {next -> new BrokerFilter(next) {
            @Override
            public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                if (!isValid(info.getUserName(), info.getPassword())) {
                    throw new SecurityException("User name [" + info.getUserName() + "] or password is invalid.");
                }
                super.addConnection(context, info);
            }
        }});
        broker.start();
        broker.waitUntilStarted();

        // no credentials on the factory: they must come from the Subject
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl("vm://subject-test?create=false");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testCreateManagedConnectionUsesSubjectCredentials() throws Exception {
        ManagedConnection connection = managedConnectionFactory.createManagedConnection(subject(USER, PASSWORD, managedConnectionFactory), null);
        try {
            assertEquals(USER, userNameOf(connection));
        } finally {
            connection.destroy();
        }
    }

    @Test(timeout = 60000)
    public void testSubjectCredentialsReplaceFactoryCredentials() throws Exception {
        // default credentials of the factory, as set by the ra.xml of activemq-rar
        managedConnectionFactory.setUserName("defaultUser");
        managedConnectionFactory.setPassword("defaultPassword");
        // createConnection() without credentials: the request info is a copy of the factory one
        ActiveMQConnectionRequestInfo info = managedConnectionFactory.getInfo().copy();
        ManagedConnection connection = managedConnectionFactory.createManagedConnection(subject(USER, PASSWORD, managedConnectionFactory), info);
        try {
            assertEquals(USER, userNameOf(connection));
        } finally {
            connection.destroy();
        }
        // the request info given by the caller is left untouched
        assertEquals("defaultUser", info.getUserName());
    }

    @Test(timeout = 60000)
    public void testApplicationCredentialsTakePrecedenceOverSubject() throws Exception {
        // createConnection(userName, password): the application gives its own credentials
        ActiveMQConnectionRequestInfo info = managedConnectionFactory.getInfo().copy();
        info.setUserName(OTHER_USER);
        info.setPassword(OTHER_PASSWORD);
        ManagedConnection connection = managedConnectionFactory.createManagedConnection(subject(USER, PASSWORD, managedConnectionFactory), info);
        try {
            assertEquals(OTHER_USER, userNameOf(connection));
        } finally {
            connection.destroy();
        }
    }

    @Test(timeout = 60000)
    public void testCredentialOfAnotherFactoryIsIgnored() throws Exception {
        ActiveMQManagedConnectionFactory otherFactory = new ActiveMQManagedConnectionFactory();
        ManagedConnection connection = null;
        try {
            connection = managedConnectionFactory.createManagedConnection(subject(USER, PASSWORD, otherFactory), null);
            userNameOf(connection);
            fail("The credential of another managed connection factory should not be used");
        } catch (ResourceException | JMSSecurityException expected) {
            // the broker rejects the connection without credentials
        } finally {
            if (connection != null) {
                connection.destroy();
            }
        }
    }

    @Test(timeout = 60000)
    public void testMatchManagedConnectionsWithSubject() throws Exception {
        Subject subject = subject(USER, PASSWORD, managedConnectionFactory);
        ActiveMQConnectionRequestInfo info = managedConnectionFactory.getInfo().copy();
        ManagedConnection connection = managedConnectionFactory.createManagedConnection(subject, info);
        try {
            Set<ManagedConnection> connections = new HashSet<>(Collections.singleton(connection));

            // same Subject: the pooled connection is reused
            assertSame(connection, managedConnectionFactory.matchManagedConnections(connections, subject, info.copy()));
            assertEquals(USER, userNameOf(connection));

            // other Subject: not reused
            assertNull(managedConnectionFactory.matchManagedConnections(connections,
                subject(OTHER_USER, OTHER_PASSWORD, managedConnectionFactory), info.copy()));
        } finally {
            connection.destroy();
        }
    }

    @Test(timeout = 60000)
    public void testSubjectCredentialsThatCannotBeReadAreIgnored() throws Exception {
        // as with a security manager that does not grant the PrivateCredentialPermission
        Subject subject = mock(Subject.class);
        when(subject.getPrivateCredentials(PasswordCredential.class)).thenThrow(new SecurityException("access denied"));
        managedConnectionFactory.setUserName(OTHER_USER);
        managedConnectionFactory.setPassword(OTHER_PASSWORD);
        ManagedConnection connection = managedConnectionFactory.createManagedConnection(subject, managedConnectionFactory.getInfo().copy());
        try {
            // the credentials of the factory are used, as before
            assertEquals(OTHER_USER, userNameOf(connection));
        } finally {
            connection.destroy();
        }
    }

    private static boolean isValid(String userName, String password) {
        return (USER.equals(userName) && PASSWORD.equals(password)) || (OTHER_USER.equals(userName) && OTHER_PASSWORD.equals(password));
    }

    private static Subject subject(String userName, String password, ManagedConnectionFactory factory) {
        PasswordCredential credential = new PasswordCredential(userName, password.toCharArray());
        credential.setManagedConnectionFactory(factory);
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(credential);
        return subject;
    }

    /**
     * Starts the physical connection, so that the broker authenticates it, and returns its user name.
     */
    private static String userNameOf(ManagedConnection connection) throws JMSException {
        ActiveMQConnection physical = (ActiveMQConnection) ((ActiveMQManagedConnection) connection).getPhysicalConnection();
        assertNotNull(physical);
        physical.start();
        return physical.getConnectionInfo().getUserName();
    }
}
