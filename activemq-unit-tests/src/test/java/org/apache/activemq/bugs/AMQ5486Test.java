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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.support.JmsUtils;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AMQ5486Test {
    private static Logger LOG = LoggerFactory.getLogger(AMQ5486Test.class);
    private static final int maxConnections = 100;
    private static final int maxPoolSize = 10;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private String connectionUri;
    private BrokerService service;
    private TransportConnector connector;
    final ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<Connection>();

    @Before
    public void setUp() throws Exception {

        // max out the pool and reject work
        System.setProperty("org.apache.activemq.transport.nio.SelectorManager.maximumPoolSize", String.valueOf(maxPoolSize));
        System.setProperty("org.apache.activemq.transport.nio.SelectorManager.workQueueCapacity", "0");
        System.setProperty("org.apache.activemq.transport.nio.SelectorManager.rejectWork", "true");
        service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        connector = service.addConnector("nio://0.0.0.0:0");
        connectionUri = connector.getPublishableConnectString();
        service.start();
        service.waitUntilStarted();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    @Test
    public void testFailureOnSelectorThreadPoolExhaustion() throws Exception {
        final ConnectionFactory cf = createConnectionFactory();
        final CountDownLatch startupLatch = new CountDownLatch(1);
        final List<Exception> exceptions = Collections.synchronizedList(new LinkedList<Exception>());
        for(int i = 0; i < maxConnections; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ActiveMQConnection conn = null;
                    try {
                        startupLatch.await();
                        conn = (ActiveMQConnection) cf.createConnection();
                        conn.start();
                        connections.add(conn);
                    } catch (Exception e) {
                        exceptions.add(e);
                        JmsUtils.closeConnection(conn);
                    }
                }
            });
        }

        // No connections at first
        assertEquals(0, connector.getConnections().size());
        // Release the latch to set up connections in parallel
        startupLatch.countDown();

        final TransportConnector connector = this.connector;


        // Expect the max connections is created
        assertTrue("Expected some exceptions",
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return !exceptions.isEmpty();
                }
            })
        );

        assertTrue("Expected some connections, provided not all errored out",
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        LOG.info("Exceptions size: " + exceptions.size() + ", connections size: " + connector.getConnections().size());
                        return exceptions.size() == maxConnections || connector.getConnections().size() > 0;
                    }
                })
        );

        assertTrue("Expected: connections or exceptions match attempts: "  + maxConnections,
                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        LOG.info("Exceptions size: " + exceptions.size() + ", connections size: " + connector.getConnections().size());
                        return connector.getConnections().size()  + exceptions.size() == maxConnections;
                    }
                })
        );
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();

        for (Connection connection : connections) {
            JmsUtils.closeConnection(connection);
        }

        connections.clear();
        service.stop();
        service.waitUntilStopped();
    }
}
