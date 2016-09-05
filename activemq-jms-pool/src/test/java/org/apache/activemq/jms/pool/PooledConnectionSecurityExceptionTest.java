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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

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
        Connection connection = pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("system", "manager");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
    }

    @Test
    public void testFailedConnectThenSucceedsWithListener() throws JMSException {
        Connection connection = pooledConnFact.createConnection("invalid", "credentials");
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.warn("Connection get error: {}", exception.getMessage());
            }
        });

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("system", "manager");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
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

        final PooledConnection connection1 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        // The pool should process the async error
        assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection1.getConnection() !=
                    ((PooledConnection) pooledConnFact.createConnection("invalid", "credentials")).getConnection();
            }
        }));

        final PooledConnection connection2 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials");
        assertNotSame(connection1.getConnection(), connection2.getConnection());

        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        } finally {
            connection2.close();
        }

        connection1.close();
    }

    @Test
    public void testFailureGetsNewConnectionOnRetryBigPool() throws JMSException {
        pooledConnFact.setMaxConnections(10);

        Connection connection1 = pooledConnFact.createConnection("invalid", "credentials");
        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        Connection connection2 = pooledConnFact.createConnection("invalid", "credentials");
        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        assertNotSame(connection1, connection2);

        connection1.close();
        connection2.close();
    }

    @Test
    public void testFailoverWithInvalidCredentialsCanConnect() throws JMSException {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
            "failover:(" + connectionURI + ")");

        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(cf);
        pooledConnFact.setMaxConnections(1);

        Connection connection = pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        connection = pooledConnFact.createConnection("system", "manager");
        connection.start();

        LOG.info("Successfully create new connection.");

        connection.close();
    }

    @Test
    public void testFailoverWithInvalidCredentials() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
            "failover:(" + connectionURI + "?trace=true)");

        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(cf);
        pooledConnFact.setMaxConnections(1);

        final PooledConnection connection1 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials");

        try {
            connection1.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
            // Intentionally don't close here to see that async pool reconnect takes place.
        }

        // The pool should process the async error
        assertTrue("Should get new connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection1.getConnection() !=
                          ((PooledConnection) pooledConnFact.createConnection("invalid", "credentials")).getConnection();
            }
        }));

        final PooledConnection connection2 = (PooledConnection) pooledConnFact.createConnection("invalid", "credentials");
        assertNotSame(connection1.getConnection(), connection2.getConnection());

        try {
            connection2.start();
            fail("Should fail to connect");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
            connection2.close();
        } finally {
            connection2.close();
        }

        connection1.close();
    }

    @Test
    public void testFailedCreateConsumerConnectionStillWorks() throws JMSException {
        Connection connection = pooledConnFact.createConnection("guest", "password");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(name.getMethodName());

        try {
            session.createConsumer(queue);
            fail("Should fail to create consumer");
        } catch (JMSSecurityException ex) {
            LOG.info("Caught expected security error");
        }

        queue = session.createQueue("GUESTS." + name.getMethodName());

        MessageProducer producer = session.createProducer(queue);
        producer.close();

        connection.close();
    }

    public String getName() {
        return name.getMethodName();
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
