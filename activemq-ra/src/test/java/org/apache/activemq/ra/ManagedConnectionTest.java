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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ManagedConnectionTest {

    private static final String DEFAULT_HOST = "vm://localhost?broker.persistent=false";

    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private ConnectionFactory connectionFactory;
    private ManagedConnectionProxy connection;
    private ActiveMQManagedConnection managedConnection;

    @Before
    public void setUp() throws Exception {
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl(DEFAULT_HOST);
        managedConnectionFactory.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        managedConnectionFactory.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        connectionFactory = (ConnectionFactory)managedConnectionFactory.createConnectionFactory(connectionManager);
        connection = (ManagedConnectionProxy)connectionFactory.createConnection();
        managedConnection = connection.getManagedConnection();
    }

    @After
    public void destroyManagedConnection() throws Exception {
        if (managedConnection != null) {
            managedConnection.destroy();
        }
    }

    @Test(timeout = 60000)
    public void testConnectionCloseEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
            @Override
            public void connectionClosed(ConnectionEvent arg0) {
                test[0] = true;
            }
        });
        connection.close();
        assertTrue(test[0]);
    }

    @Test(timeout = 60000)
    public void testLocalTransactionCommittedEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
            @Override
            public void localTransactionCommitted(ConnectionEvent arg0) {
                test[0] = true;
            }
        });

        managedConnection.getLocalTransaction().begin();
        Session session = connection.createSession(true, 0);

        doWork(session);
        session.commit();

        assertTrue(test[0]);
    }

    @Test(timeout = 60000)
    public void testLocalTransactionRollbackEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
            @Override
            public void localTransactionRolledback(ConnectionEvent arg0) {
                test[0] = true;
            }
        });
        managedConnection.getLocalTransaction().begin();
        Session session = connection.createSession(true, 0);
        doWork(session);
        session.rollback();

        assertTrue(test[0]);
    }

    @Test(timeout = 60000)
    public void testLocalTransactionStartedEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
            @Override
            public void localTransactionStarted(ConnectionEvent arg0) {
                test[0] = true;
            }
        });

        // Begin the transaction... that should kick off the event.
        managedConnection.getLocalTransaction().begin();
        Session session = connection.createSession(true, 0);
        doWork(session);

        assertTrue(test[0]);
    }

    /**
     * A managed connection that has been clean up should throw exceptions when
     * it used.
     */
    @Test(timeout = 60000)
    public void testCleanup() throws ResourceException, JMSException {

        // Do some work and close it...
        Session session = connection.createSession(true, 0);
        doWork(session);
        connection.close();
        try {
            // This should throw exception
            doWork(session);
            fail("Using a session after the connection is closed should throw exception.");
        } catch (JMSException e) {
        }
    }

    @Test(timeout = 60000)
    public void testSetClientIdAfterCleanup() throws Exception {

        connection.setClientID("test");
        try {
            connection.setClientID("test");
            fail("Should have received JMSException");
        } catch (JMSException e) {
        }

        ActiveMQConnection physicalConnection = (ActiveMQConnection) managedConnection.getPhysicalConnection();
        try {
            physicalConnection.setClientID("testTwo");
            fail("Should have received JMSException");
        } catch (JMSException e) {
        }

        // close the proxy
        connection.close();

        // can set the id on the physical connection again after cleanup
        physicalConnection.setClientID("test3");

        try {
            physicalConnection.setClientID("test4");
            fail("Should have received JMSException");
        } catch (JMSException e) {
        }
    }

    @Test(timeout = 60000)
    public void testSessionCloseIndependance() throws ResourceException, JMSException {

        Session session1 = connection.createSession(true, 0);
        Session session2 = connection.createSession(true, 0);
        assertTrue(session1 != session2);

        doWork(session1);
        session1.close();
        try {
            // This should throw exception
            doWork(session1);
            fail("Using a session after the connection is closed should throw exception.");
        } catch (JMSException e) {
        }

        // Make sure that closing session 1 does not close session 2
        doWork(session2);
        session2.close();
        try {
            // This should throw exception
            doWork(session2);
            fail("Using a session after the connection is closed should throw exception.");
        } catch (JMSException e) {
        }
    }

    /**
     * Does some work so that we can test commit/rollback etc.
     *
     * @throws JMSException
     */
    public void doWork(Session session) throws JMSException {
        Queue t = session.createQueue("TEST");
        MessageProducer producer = session.createProducer(t);
        producer.send(session.createTextMessage("test message."));
    }

    @Test(timeout = 60000)
    public void testImplementsQueueAndTopicConnection() throws Exception {
        QueueConnection qc = ((QueueConnectionFactory)connectionFactory).createQueueConnection();
        assertNotNull(qc);
        TopicConnection tc = ((TopicConnectionFactory)connectionFactory).createTopicConnection();
        assertNotNull(tc);
    }

    @Test(timeout = 60000)
    public void testSelfEquality() {
        assertEquality(managedConnection, managedConnection);
    }

    @Test(timeout = 60000)
    public void testSamePropertiesButNotEqual() throws Exception {
        ManagedConnectionProxy newConnection = (ManagedConnectionProxy)connectionFactory.createConnection();
        assertNonEquality(managedConnection, newConnection.getManagedConnection());
        newConnection.close();
    }

    private void assertEquality(ActiveMQManagedConnection leftCon, ActiveMQManagedConnection rightCon) {
        assertTrue("ActiveMQManagedConnection are not equal", leftCon.equals(rightCon));
        assertTrue("ActiveMQManagedConnection are not equal", rightCon.equals(leftCon));
        assertTrue("HashCodes are not equal", leftCon.hashCode() == rightCon.hashCode());
    }

    private void assertNonEquality(ActiveMQManagedConnection leftCon, ActiveMQManagedConnection rightCon) {
        assertFalse("ActiveMQManagedConnection are equal", leftCon.equals(rightCon));
        assertFalse("ActiveMQManagedConnection are equal", rightCon.equals(leftCon));
        assertFalse("HashCodes are equal", leftCon.hashCode() == rightCon.hashCode());
    }
}
