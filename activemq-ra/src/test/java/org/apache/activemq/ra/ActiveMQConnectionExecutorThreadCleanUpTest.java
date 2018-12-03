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

import java.util.Set;
import java.util.Iterator;
import javax.transaction.xa.XAResource;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ra.ActiveMQResourceAdapter;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test for AMQ-6700.
 * Will fail to connect to embedded broker using JCA and uses
 * "ActiveMQ Connection Executor" thread to deal with low
 * level exception. This tests verifies if this thread gets
 * cleared up correctly after use.
 */
public class ActiveMQConnectionExecutorThreadCleanUpTest {

    protected static Logger LOG =
        LoggerFactory.getLogger(ActiveMQConnectionExecutorThreadCleanUpTest.class);
    protected static final String AMQ_CONN_EXECUTOR_THREAD_NAME =
        "ActiveMQ Connection Executor";
    private BrokerService broker = null;


    @Before
    public void setUp() throws Exception {
        LOG.info("Configuring broker programmatically.");
        broker = new BrokerService();
        broker.setPersistent(false);

        // explicitly limiting to 0 connections so that test is unable
        // to connect
        broker.addConnector("tcp://localhost:0?maximumConnections=0");
        broker.start();
        broker.waitUntilStarted(5000);
    }


    @After
    public void shutDown() throws Exception {
        if (broker != null) {
            if (broker.isStarted()) {
                broker.stop();
                broker.waitUntilStopped();
            }
        }
    }


    /**
     * This test tries to create connections into the broker using the
     * resource adapter's transaction recovery functionality.
     * If the broker does not accept the connection, the connection's
     * thread pool executor is used to deal with the error.
     * This has lead to race conditions where the thread was not shutdown
     * but got leaked.
     * @throws Exception
     */
    @Test
    public void testAMQConnectionExecutorThreadCleanUp() throws Exception {
        LOG.info("testAMQConnectionExecutorThreadCleanUp() started.");

        ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
        ra.setServerUrl(broker.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        LOG.info("Using brokerUrl " + ra.getServerUrl());

        // running in a small loop as very occasionally the call to
        // ActiveMQResourceAdapter.$2.makeConnection() raises an exception
        // rather than using the connection's executor task to deal with the
        // connection error.
        for (int i=0; i<10; i++) {
            LOG.debug("Iteration " + i);
            try {
                XAResource[] resources = ra.getXAResources(null);
                resources[0].recover(100);
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            // allow some small time for thread cleanup to happen
            Thread.sleep(300);

            // check if thread exists
            Assert.assertFalse("Thread named \"" +
                    AMQ_CONN_EXECUTOR_THREAD_NAME +
                    "\" not cleared up with ActiveMQConnection.",
                hasActiveMQConnectionExceutorThread());
        }
        ra.stop();
    }


    /**
     * Retrieves all threads from JVM and checks if any thread names contain
     * AMQ_CONN_EXECUTOR_THREAD_NAME.
     *
     * @return true if such thread exists, otherwise false
     */
    public boolean hasActiveMQConnectionExceutorThread() {
        // retrieve all threads
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        Iterator<Thread> iter = threadSet.iterator();
        while (iter.hasNext()) {
            Thread thread = (Thread)iter.next();
            if (thread.getName().startsWith(AMQ_CONN_EXECUTOR_THREAD_NAME )) {
                LOG.error("Thread with name {} found.", thread.getName());
               return true;
            }
        }
        LOG.debug("Thread with name {} not found.", AMQ_CONN_EXECUTOR_THREAD_NAME);
        return false;
    }
}
