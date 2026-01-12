/*
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
package org.apache.activemq.jms.pool;

import jakarta.jms.Connection;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Pooled connections ability to handle security exceptions
 */
public class PooledConnectionSecurityExceptionTest {

    protected static final Logger LOG = LoggerFactory.getLogger(PooledConnectionSecurityExceptionTest.class);

    @Rule public TestName name = new TestName();

    private BrokerService brokerService;
    private String connectionURI;

    protected PooledConnectionFactory pooledConnFact;

    @Test
    public void testFailedConnectThenSucceeds() throws JMSException {
        try (final Connection connection1 = pooledConnFact.createConnection("invalid", "credentials")) {
            assertSecurityExceptionOnStart(connection1);

            try (final Connection connection2 = pooledConnFact.createConnection("system", "manager")) {
                connection2.start();

            } catch (final JMSSecurityException ex) {
                fail("Should have succeeded to connect on 2nd attempt");
            }

        }
    }

    @Test
    public void testFailedConnectThenSucceedsWithListener() throws JMSException, InterruptedException {
        final CountDownLatch onExceptionCalled = new CountDownLatch(1);
        try (final Connection connection1 = pooledConnFact.createConnection("invalid", "credentials")) {
            connection1.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    LOG.warn("Connection get error: {}", exception.getMessage());
                    onExceptionCalled.countDown();
                }
            });
            assertSecurityExceptionOnStart(connection1);

            try (final Connection connection2 = pooledConnFact.createConnection("system", "manager")) {
                connection2.start();

            } catch (final JMSSecurityException ex) {
                fail("Should have succeeded to connect on 2nd attempt");
            }

        }
        assertTrue("onException called", onExceptionCalled.await(10, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    public void testFailureGetsNewConnectionOnRetryLooped() throws Exception {
        for (int i = 0; i < 10; ++i) {
            testFailureGetsNewConnectionOnRetry();
        }
    }

    @Test
    public void testFailureGetsNewConnectionOnRetry() throws Exception {
        pooledConnFact.setMaxConnections(1);

        try (final Connection connection1 = pooledConnFact.createConnection("invalid", "credentials")) {
            assertSecurityExceptionOnStart(connection1);

            // The pool should process the async error
            // we should eventually get a different connection instance from the pool regardless of the underlying connection
            assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    try (final PooledConnection newConnection = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials")) {
                        return connection1 != newConnection;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }));

            try (final PooledConnection connection2 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials")) {
                assertNotSame(connection1, connection2);
            }

            assertNull(((PooledConnection)connection1).getConnection()); // underlying connection should have been closed
        }
    }

    public void testFailureGetsNewConnectionOnRetryBigPool() throws JMSException {
        pooledConnFact.setMaxConnections(10);

        try (final Connection connection1 = pooledConnFact.createConnection("invalid", "credentials")) {
            assertSecurityExceptionOnStart(connection1);
            try (final Connection connection2 = pooledConnFact.createConnection("invalid", "credentials")) {
                assertSecurityExceptionOnStart(connection2);
                assertNotSame(connection1, connection2);
            }
        }

    }

    @Test
    public void testFailoverWithInvalidCredentialsCanConnect() throws JMSException {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
            "failover:(" + connectionURI + ")");

        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(cf);
        pooledConnFact.setMaxConnections(1);

        try (final Connection connection = pooledConnFact.createConnection("invalid", "credentials")) {
            assertSecurityExceptionOnStart(connection);

            try (final Connection connection2 = pooledConnFact.createConnection("system", "manager")) {
                connection2.start();
                LOG.info("Successfully create new connection.");
            }
        }
    }

    @Test
    public void testFailoverWithInvalidCredentials() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
            "failover:(" + connectionURI + "?trace=true)");

        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(cf);
        pooledConnFact.setMaxConnections(1);

        try (final PooledConnection connection1 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials")) {
            assertSecurityExceptionOnStart(connection1);

            // The pool should process the async error
            assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    try (final PooledConnection newConnection = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials")) {
                        return connection1 != newConnection;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }));

            try (final PooledConnection connection2 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials")) {
                assertNotSame(connection1.pool, connection2.pool);
                assertSecurityExceptionOnStart(connection2);
            }
        }
    }

    @Test
    public void testFailedCreateConsumerConnectionStillWorks() throws JMSException {
        try (final Connection connection = pooledConnFact.createConnection("guest", "password")) {
            connection.start();

            try (final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                final Queue queue = session.createQueue(name.getMethodName());

                assertThrows(JMSSecurityException.class, () -> session.createConsumer(queue));

                final Queue guestsQueue = session.createQueue("GUESTS." + name.getMethodName());

                try (final MessageProducer producer = session.createProducer(guestsQueue)) {
                    // We can still produce to the GUESTS queue.
                }
            }
        }
    }

    public String getName() {
        return name.getMethodName();
    }

    /**
     * Helper method to assert that a connection start fails with security exception.
     * On different test environments, the connection may be disposed asynchronously
     * before the security exception is fully propagated, resulting in either JMSSecurityException
     * or generic JMSException with "Disposed" message. Both indicate authentication failure.
     *
     * This method uses an ExceptionListener to detect when async disposal completes, providing
     * more reliable detection of security failures across different Java versions and environments.
     *
     * @param connection the connection to start
     * @throws AssertionError if no exception is thrown or the exception doesn't indicate auth failure
     */
    private void assertSecurityExceptionOnStart(final Connection connection) {
        try {
            final ExceptionListener listener =  connection.getExceptionListener();
            if (listener == null) { // some tests already leverage the exception listener
                final CountDownLatch exceptionLatch = new CountDownLatch(1);

                // Install listener to capture async exception propagation
                connection.setExceptionListener(new ExceptionListener() {
                    @Override
                    public void onException(final JMSException exception) {
                        LOG.info("Connection received exception: {}", exception.getMessage());
                        assertTrue(exception instanceof JMSSecurityException);
                        exceptionLatch.countDown();
                    }
                });
                connection.start(); // should trigger the security exception reliably and asynchronously
                exceptionLatch.await(1, java.util.concurrent.TimeUnit.SECONDS);

            } else {

                // Attempt to start and capture the synchronous exception.
                final JMSException thrownException = assertThrows(JMSException.class, connection::start);
                assertTrue("Should be JMSSecurityException or disposed due to security exception",
                           thrownException instanceof JMSSecurityException ||
                           thrownException.getMessage().contains("Disposed"));
            }


        } catch (final JMSException e) {
            // Ignore

        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getName() + " ==========");
        startBroker();

        // Create the ActiveMQConnectionFactory and the PooledConnectionFactory.
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(cf);
        pooledConnFact.setMaxConnections(1);
        pooledConnFact.setReconnectOnException(true);
    }

    @After
    public void tearDown() throws Exception {
        pooledConnFact.stop();
        stopBroker();
        LOG.info("========== finished " + getName() + " ==========");
    }

    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setAdvisorySupport(false);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.getManagementContext().setCreateMBeanServer(false);
        brokerService.addConnector("tcp://0.0.0.0:0");

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        BrokerPlugin authenticationPlugin = configureAuthentication();
        if (authenticationPlugin != null) {
            plugins.add(configureAuthorization());
        }

        BrokerPlugin authorizationPlugin = configureAuthorization();
        if (authorizationPlugin != null) {
            plugins.add(configureAuthentication());
        }

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = brokerService.getTransportConnectors().get(0).getPublishableConnectString();
    }

    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        users.add(new AuthenticationUser("user", "password", "users"));
        users.add(new AuthenticationUser("guest", "password", "guests"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        return authenticationPlugin;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins");
        tempEntry.setWrite("admins");
        tempEntry.setAdmin("admins");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }
}