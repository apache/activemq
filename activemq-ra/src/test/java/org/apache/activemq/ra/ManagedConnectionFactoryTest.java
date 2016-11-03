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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TopicConnectionFactory;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Before;
import org.junit.Test;

public class ManagedConnectionFactoryTest {

    private static final String DEFAULT_HOST = "vm://localhost?broker.persistent=false&broker.schedulerSupport=false";
    private static final String REMOTE_HOST = "vm://remotehost?broker.persistent=false&broker.schedulerSupport=false";
    private ActiveMQManagedConnectionFactory managedConnectionFactory;

    @Before
    public void setUp() throws Exception {
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl(DEFAULT_HOST);
        managedConnectionFactory.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        managedConnectionFactory.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        managedConnectionFactory.setUseSessionArgs(false);
    }

    @Test(timeout = 60000)
    public void testConnectionFactoryAllocation() throws ResourceException, JMSException {

        // Make sure that the ConnectionFactory is asking the connection manager
        // to allocate the connection.
        final boolean allocateRequested[] = new boolean[] {
            false
        };

        Object cf = managedConnectionFactory.createConnectionFactory(new ConnectionManagerAdapter() {
            private static final long serialVersionUID = 1699499816530099939L;

            @Override
            public Object allocateConnection(ManagedConnectionFactory connectionFactory, ConnectionRequestInfo info) throws ResourceException {
                allocateRequested[0] = true;
                return super.allocateConnection(connectionFactory, info);
            }
        });

        // We should be getting a JMS Connection Factory.
        assertTrue(cf instanceof ConnectionFactory);
        ConnectionFactory connectionFactory = (ConnectionFactory)cf;

        // Make sure that the connection factory is using the
        // ConnectionManager..
        Connection connection = connectionFactory.createConnection();
        assertTrue(allocateRequested[0]);

        // Make sure that the returned connection is of the expected type.
        assertTrue(connection != null);
        assertTrue(connection instanceof ManagedConnectionProxy);

        Session session = connection.createSession(true, 0);
        assertFalse("transacted attribute is ignored, only transacted with xa or local tx", session.getTransacted());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testConnectionSessionArgs() throws ResourceException, JMSException {
        ActiveMQConnectionRequestInfo connectionRequestInfo = new ActiveMQConnectionRequestInfo();
        connectionRequestInfo.setServerUrl(DEFAULT_HOST);
        connectionRequestInfo.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        connectionRequestInfo.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        connectionRequestInfo.setUseSessionArgs(true);

        ManagedConnection managedConnection = managedConnectionFactory.createManagedConnection(null, connectionRequestInfo);
        Connection connection = (Connection) managedConnection.getConnection(null, connectionRequestInfo);

        Session session = connection.createSession(true, 0);
        assertTrue("transacted attribute is respected", session.getTransacted());
        connection.close();
    }

    @Test(timeout = 60000)
    public void testConnectionFactoryConnectionMatching() throws ResourceException, JMSException {

        ActiveMQConnectionRequestInfo ri1 = new ActiveMQConnectionRequestInfo();
        ri1.setServerUrl(DEFAULT_HOST);
        ri1.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        ri1.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        ActiveMQConnectionRequestInfo ri2 = new ActiveMQConnectionRequestInfo();
        ri2.setServerUrl(REMOTE_HOST);
        ri2.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        ri2.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        assertNotSame(ri1, ri2);

        ManagedConnection connection1 = managedConnectionFactory.createManagedConnection(null, ri1);
        ManagedConnection connection2 = managedConnectionFactory.createManagedConnection(null, ri2);
        assertTrue(connection1 != connection2);

        HashSet<ManagedConnection> set = new HashSet<ManagedConnection>();
        set.add(connection1);
        set.add(connection2);

        // Can we match for the first connection?
        ActiveMQConnectionRequestInfo ri3 = ri1.copy();
        assertTrue(ri1 != ri3 && ri1.equals(ri3));
        ManagedConnection test = managedConnectionFactory.matchManagedConnections(set, null, ri3);
        assertTrue(connection1 == test);

        // Can we match for the second connection?
        ri3 = ri2.copy();
        assertTrue(ri2 != ri3 && ri2.equals(ri3));
        test = managedConnectionFactory.matchManagedConnections(set, null, ri2);
        assertTrue(connection2 == test);

        for (ManagedConnection managedConnection : set) {
            managedConnection.destroy();
        }
    }

    @Test(timeout = 60000)
    public void testConnectionFactoryIsSerializableAndReferenceable() throws ResourceException, JMSException {
        Object cf = managedConnectionFactory.createConnectionFactory(new ConnectionManagerAdapter());
        assertTrue(cf != null);
        assertTrue(cf instanceof Serializable);
        assertTrue(cf instanceof Referenceable);
    }

    @Test(timeout = 60000)
    public void testImplementsQueueAndTopicConnectionFactory() throws Exception {
        Object cf = managedConnectionFactory.createConnectionFactory(new ConnectionManagerAdapter());
        assertTrue(cf instanceof QueueConnectionFactory);
        assertTrue(cf instanceof TopicConnectionFactory);
    }

    @Test(timeout = 60000)
    public void testSerializability() throws Exception {

        managedConnectionFactory.setLogWriter(new PrintWriter(new ByteArrayOutputStream()));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(managedConnectionFactory);
        oos.close();
        byte[] byteArray = bos.toByteArray();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        ActiveMQManagedConnectionFactory deserializedFactory = (ActiveMQManagedConnectionFactory) ois.readObject();
        ois.close();

        assertNull(
                "[logWriter] property of deserialized ActiveMQManagedConnectionFactory is not null",
                deserializedFactory.getLogWriter());
        assertNotNull(
                "ConnectionRequestInfo of deserialized ActiveMQManagedConnectionFactory is null",
                deserializedFactory.getInfo());
        assertEquals(
                "[serverUrl] property of deserialized ConnectionRequestInfo object is not [" + DEFAULT_HOST + "]",
                DEFAULT_HOST,
                deserializedFactory.getInfo().getServerUrl());
        assertNotNull(
                "Log instance of deserialized ActiveMQManagedConnectionFactory is null",
                deserializedFactory.log);
    }
}
