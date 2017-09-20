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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;

/**
 * A couple of tests against the PooledConnection class.
 *
 */
public class PooledConnectionTest extends JmsPoolTestSupport {

    private final Logger LOG = LoggerFactory.getLogger(PooledConnectionTest.class);

    /**
     * AMQ-3752:
     * Tests how the ActiveMQConnection reacts to repeated calls to
     * setClientID().
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testRepeatedSetClientIDCalls() throws Exception {
        LOG.debug("running testRepeatedSetClientIDCalls()");

        // 1st test: call setClientID("newID") twice
        // this should be tolerated and not result in an exception
        //
        ConnectionFactory cf = createPooledConnectionFactory();
        Connection conn = cf.createConnection();
        conn.setClientID("newID");

        try {
            conn.setClientID("newID");
            conn.start();
            conn.close();
        } catch (IllegalStateException ise) {
            LOG.error("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
            fail("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
        } finally {
            ((PooledConnectionFactory) cf).stop();
        }

        // 2nd test: call setClientID() twice with different IDs
        // this should result in an IllegalStateException
        //
        cf = createPooledConnectionFactory();
        conn = cf.createConnection();
        conn.setClientID("newID1");
        try {
            conn.setClientID("newID2");
            fail("calling ActiveMQConnection.setClientID() twice with different clientID must raise an IllegalStateException");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            ((PooledConnectionFactory) cf).stop();
        }

        // 3rd test: try to call setClientID() after start()
        // should result in an exception
        cf = createPooledConnectionFactory();
        conn = cf.createConnection();
        try {
        conn.start();
        conn.setClientID("newID3");
        fail("Calling setClientID() after start() mut raise a JMSException.");
        } catch (IllegalStateException ise) {
            LOG.debug("Correctly received " + ise);
        } finally {
            conn.close();
            ((PooledConnectionFactory) cf).stop();
        }

        LOG.debug("Test finished.");
    }

    protected ConnectionFactory createPooledConnectionFactory() {
        PooledConnectionFactory cf = new PooledConnectionFactory();
        cf.setConnectionFactory(new ActiveMQConnectionFactory(
            "vm://localhost?broker.persistent=false&broker.useJmx=false&broker.schedulerSupport=false"));
        cf.setMaxConnections(1);
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }

    @Test(timeout = 60000)
    public void testAllSessionsAvailableOnConstrainedPool() throws Exception {
        PooledConnectionFactory cf = new PooledConnectionFactory();
        cf.setConnectionFactory(new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false&broker.useJmx=false&broker.schedulerSupport=false"));
        cf.setMaxConnections(5);
        cf.setMaximumActiveSessionPerConnection(2);
        cf.setBlockIfSessionPoolIsFull(false);

        LinkedList<Connection> connections = new LinkedList<>();
        HashSet<Session> sessions = new HashSet();

        for (int i=0; i<10; i++) {
            Connection conn = cf.createConnection();
            LOG.info("connection: " + i + ", " + ((PooledConnection)conn).getConnection());

            conn.start();
            connections.add(conn);
            sessions.add(conn.createSession(false, Session.AUTO_ACKNOWLEDGE));
        }

        assertEquals(sessions.size(), 10);
        assertEquals(connections.size(), 10);

        Connection connectionToClose = connections.getLast();
        connectionToClose.close();

        Connection conn = cf.createConnection();
        LOG.info("connection:" + ((PooledConnection)conn).getConnection());

        conn.start();
        connections.add(conn);
        try {
            sessions.add(conn.createSession(false, Session.AUTO_ACKNOWLEDGE));
        } catch (JMSException expected) {
            conn.close();
        }

        conn = cf.createConnection();
        LOG.info("connection:" + ((PooledConnection)conn).getConnection());

        conn.start();
        connections.add(conn);
        try {
            sessions.add(conn.createSession(false, Session.AUTO_ACKNOWLEDGE));
        } catch (JMSException expected) {
            conn.close();
        }

        assertEquals(sessions.size(), 10);
        assertEquals(connections.size(), 12);
    }
}
