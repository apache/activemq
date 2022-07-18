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
package org.apache.activemq.transport.auto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AutoTransportConnectionsTest {

    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    private static final int maxConnections = 20;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private String connectionUri;
    private BrokerService service;
    private TransportConnector connector;
    private final String transportType;

    @Parameters(name="transport={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"auto"},
                {"auto+nio"},
                {"auto+ssl"},
                {"auto+nio+ssl"},
            });
    }


    public AutoTransportConnectionsTest(String transportType) {
        super();
        this.transportType = transportType;
    }


    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);

        service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();

        service.stop();
        service.waitUntilStopped();
    }

    public void configureConnectorAndStart(String bindAddress) throws Exception {
        connector = service.addConnector(bindAddress);
        connectionUri = connector.getPublishableConnectString();
        if (connectionUri.contains("ssl")) {
            connectionUri += connectionUri.contains("?") ? "&socket.verifyHostName=false" : "?socket.verifyHostName=false";
        }
        service.start();
        service.waitUntilStarted();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    @Test
    public void testMaxConnectionControl() throws Exception {
        configureConnectorAndStart(transportType + "://0.0.0.0:0?maxConnectionThreadPoolSize=10&maximumConnections="+maxConnections);

        final ConnectionFactory cf = createConnectionFactory();
        final CountDownLatch startupLatch = new CountDownLatch(1);

        //create an extra 10 connections above max
        for(int i = 0; i < maxConnections + 10; i++) {
            final int count = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        startupLatch.await();
                        //sleep for a short period of time
                        Thread.sleep(count * 3);
                        conn = cf.createConnection();
                        conn.start();
                    } catch (Exception e) {
                    }
                }
            });
        }

        TcpTransportServer transportServer = (TcpTransportServer)connector.getServer();
        // ensure the max connections is in effect
        assertEquals(maxConnections, transportServer.getMaximumConnections());
        // No connections at first
        assertEquals(0, connector.getConnections().size());
        // No connections exceeded at first
        assertEquals(Long.valueOf(0l), Long.valueOf(connector.getMaxConnectionExceededCount()));
        // Release the latch to set up connections in parallel
        startupLatch.countDown();

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

        // The 10 extra connections exceeded connection count
        assertEquals(Long.valueOf(10l), Long.valueOf(connector.getMaxConnectionExceededCount()));

        // Confirm reset statistics
        connector.resetStatistics();
        assertEquals(Long.valueOf(0l), Long.valueOf(connector.getMaxConnectionExceededCount()));
    }

    @Test
    public void testConcurrentConnections() throws Exception {
        configureConnectorAndStart(transportType + "://0.0.0.0:0");

        int connectionAttempts = 50;
        ConnectionFactory factory = createConnectionFactory();
        final AtomicInteger connectedCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            for (int i = 0; i < connectionAttempts; i++) {
                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            latch.await();
                            Connection con = factory.createConnection();
                            con.start();
                            connectedCount.incrementAndGet();
                        } catch (Exception e) {
                            //print for debugging but don't fail it might just be the transport stopping
                            e.printStackTrace();
                        }

                    }
                });
            }
            latch.countDown();

            //Make sure all attempts connected without error
            assertTrue(Wait.waitFor(new Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return connectedCount.get() == connectionAttempts;
                }
            }));

        } catch (Exception e) {
            //print for debugging but don't fail it might just be the transport stopping
            e.printStackTrace();
        }
    }
}
