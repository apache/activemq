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

import java.util.Timer;

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
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @version $Revision$
 */
public class ManagedConnectionTest extends TestCase {

    private static final String DEFAULT_HOST = "vm://localhost";

    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private ConnectionFactory connectionFactory;
    private ManagedConnectionProxy connection;
    private ActiveMQManagedConnection managedConnection;

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {

        ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter();
        adapter.setServerUrl(DEFAULT_HOST);
        adapter.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        adapter.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        adapter.start(new BootstrapContext() {
            public WorkManager getWorkManager() {
                return null;
            }

            public XATerminator getXATerminator() {
                return null;
            }

            public Timer createTimer() throws UnavailableException {
                return null;
            }
        });

        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setResourceAdapter(adapter);

        connectionFactory = (ConnectionFactory)managedConnectionFactory.createConnectionFactory(connectionManager);
        connection = (ManagedConnectionProxy)connectionFactory.createConnection();
        managedConnection = connection.getManagedConnection();

    }

    public void testConnectionCloseEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
            public void connectionClosed(ConnectionEvent arg0) {
                test[0] = true;
            }
        });
        connection.close();
        assertTrue(test[0]);
    }

    public void testLocalTransactionCommittedEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
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

    public void testLocalTransactionRollbackEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
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

    public void testLocalTransactionStartedEvent() throws ResourceException, JMSException {

        final boolean test[] = new boolean[] {
            false
        };
        connectionManager.addConnectionEventListener(new ConnectionEventListenerAdapter() {
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
    public void testCleanup() throws ResourceException, JMSException {

        // Do some work and close it...
        Session session = connection.createSession(true, 0);
        doWork(session);
        connection.close();
        try {
            // This should throw expection
            doWork(session);
            fail("Using a session after the connection is closed should throw exception.");
        } catch (JMSException e) {
        }
    }

    public void testSessionCloseIndependance() throws ResourceException, JMSException {

        Session session1 = connection.createSession(true, 0);
        Session session2 = connection.createSession(true, 0);
        assertTrue(session1 != session2);

        doWork(session1);
        session1.close();
        try {
            // This should throw expection
            doWork(session1);
            fail("Using a session after the connection is closed should throw exception.");
        } catch (JMSException e) {
        }

        // Make sure that closing session 1 does not close session 2
        doWork(session2);
        session2.close();
        try {
            // This should throw expection
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

    public void testImplementsQueueAndTopicConnection() throws Exception {
        QueueConnection qc = ((QueueConnectionFactory)connectionFactory).createQueueConnection();
        assertNotNull(qc);
        TopicConnection tc = ((TopicConnectionFactory)connectionFactory).createTopicConnection();
        assertNotNull(tc);
    }

    public void testSelfEquality() {
        assertEquality(managedConnection, managedConnection);
    }

    public void testSamePropertiesButNotEqual() throws Exception {
        ManagedConnectionProxy newConnection = (ManagedConnectionProxy)connectionFactory.createConnection();
        assertNonEquality(managedConnection, newConnection.getManagedConnection());
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
