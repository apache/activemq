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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jms.support.JmsUtils;

public class AMQ4469Test {

    private static final int maxConnections = 100;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private String connectionUri;
    private BrokerService service;
    private TransportConnector connector;

    @Before
    public void setUp() throws Exception {
        service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        connector = service.addConnector("tcp://0.0.0.0:0?maximumConnections="+maxConnections);
        connectionUri = connector.getPublishableConnectString();
        service.start();
        service.waitUntilStarted();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    @Test
    public void testMaxConnectionControl() throws Exception {
        final ConnectionFactory cf = createConnectionFactory();
        final CountDownLatch startupLatch = new CountDownLatch(1);
        for(int i = 0; i < maxConnections + 20; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        startupLatch.await();
                        conn = cf.createConnection();
                        conn.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                        JmsUtils.closeConnection(conn);
                    }
                }
            });
        }

        TcpTransportServer transportServer = (TcpTransportServer)connector.getServer();
        // ensure the max connections is in effect
        assertEquals(maxConnections, transportServer.getMaximumConnections());
        // No connections at first
        assertEquals(0, connector.getConnections().size());
        // Release the latch to set up connections in parallel
        startupLatch.countDown();
        TimeUnit.SECONDS.sleep(5);

        final TransportConnector connector = this.connector;

        // Expect the max connections is created
        assertTrue("Expected: " + maxConnections + " found: " + connector.getConnections().size(),
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return connector.getConnections().size() == maxConnections;
                }
            })
        );
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();

        service.stop();
        service.waitUntilStopped();
    }
}
