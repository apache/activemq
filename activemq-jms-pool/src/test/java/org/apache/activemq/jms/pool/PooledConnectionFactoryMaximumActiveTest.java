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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

/**
 * Checks the behavior of the PooledConnectionFactory when the maximum amount of sessions is being reached
 * (maximumActive). When using setBlockIfSessionPoolIsFull(true) on the ConnectionFactory, further requests for sessions
 * should block. If it does not block, its a bug.
 *
 * @author: tmielke
 */
public class PooledConnectionFactoryMaximumActiveTest extends TestCase {
    public final static Logger LOG = Logger.getLogger(PooledConnectionFactoryMaximumActiveTest.class);
    public static Connection conn = null;
    public static int sleepTimeout = 5000;

    private static ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<Integer, Session>();

    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
     */
    public PooledConnectionFactoryMaximumActiveTest(String testName) {
        super(testName);
    }

    public static void addSession(Session s) {
        sessions.put(s.hashCode(), s);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(PooledConnectionFactoryMaximumActiveTest.class);
    }

    /**
     * Tests the behavior of the sessionPool of the PooledConnectionFactory when maximum number of sessions are reached.
     * This test uses maximumActive=1. When creating two threads that both try to create a JMS session from the same JMS
     * connection, the thread that is second to call createSession() should block (as only 1 session is allowed) until
     * the session is returned to pool. If it does not block, its a bug.
     *
     */
    public void testApp() throws Exception {
        // Initialize JMS connection
        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory("vm://broker1?marshal=false&broker.persistent=false");
        PooledConnectionFactory cf = new PooledConnectionFactory();
        cf.setConnectionFactory(amq);
        cf.setMaxConnections(3);
        cf.setMaximumActiveSessionPerConnection(1);
        cf.setBlockIfSessionPoolIsFull(true);
        conn = cf.createConnection();

        // start test runner threads. It is expected that the second thread
        // blocks on the call to createSession()

        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.submit(new TestRunner2());
        // Thread.sleep(100);
        Future<Boolean> result2 = executor.submit(new TestRunner2());

        // sleep to allow threads to run
        Thread.sleep(sleepTimeout);

        // second task should not have finished, instead wait on getting a
        // JMS Session
        assertEquals(false, result2.isDone());

        // Only 1 session should have been created
        assertEquals(1, sessions.size());

        // Take all threads down
        executor.shutdownNow();
    }
}

class TestRunner2 implements Callable<Boolean> {

    public final static Logger LOG = Logger.getLogger(TestRunner2.class);

    /**
     * @return true if test succeeded, false otherwise
     */
    @Override
    public Boolean call() {

        Session one = null;

        // wait at most 5 seconds for the call to createSession
        try {

            if (PooledConnectionFactoryMaximumActiveTest.conn == null) {
                LOG.error("Connection not yet initialized. Aborting test.");
                return new Boolean(false);
            }

            one = PooledConnectionFactoryMaximumActiveTest.conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            LOG.info("Created new Session with id" + one);
            PooledConnectionFactoryMaximumActiveTest.addSession(one);
            Thread.sleep(2 * PooledConnectionFactoryMaximumActiveTest.sleepTimeout);
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            return new Boolean(false);

        } finally {
            if (one != null)
                try {
                    one.close();
                } catch (JMSException e) {
                    LOG.error(e.getMessage());
                }
        }

        // all good, test succeeded
        return new Boolean(true);
    }
}
