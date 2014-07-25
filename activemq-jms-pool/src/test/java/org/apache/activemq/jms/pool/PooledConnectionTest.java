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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.IllegalStateException;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A couple of tests against the PooledConnection class.
 *
 */
public class PooledConnectionTest extends TestCase {

    private final Logger log = LoggerFactory.getLogger(PooledConnectionTest.class);

    @Override
    public void setUp() throws Exception {
        log.debug("setUp() called.");
    }


    @Override
    public void tearDown() throws Exception {
        log.debug("tearDown() called.");
    }

    /**
     * AMQ-3752:
     * Tests how the ActiveMQConnection reacts to repeated calls to
     * setClientID().
     *
     * @throws Exception
     */
    public void testRepeatedSetClientIDCalls() throws Exception {
        log.debug("running testRepeatedSetClientIDCalls()");

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
            cf = null;
        } catch (IllegalStateException ise) {
            log.error("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
            fail("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
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
            log.debug("Correctly received " + ise);
        } finally {
            conn.close();
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
            log.debug("Correctly received " + ise);
        } finally {
            conn.close();
        }

        log.debug("Test finished.");
    }

    protected ConnectionFactory createPooledConnectionFactory() {
        PooledConnectionFactory cf = new PooledConnectionFactory();
        cf.setConnectionFactory(new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false"));
        cf.setMaxConnections(1);
        log.debug("ConnectionFactory initialized.");
        return cf;
    }
}
