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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.JaasDualAuthenticationPlugin;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// https://issues.apache.org/jira/browse/AMQ-3393
public class ConnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectTest.class);

    private BrokerService brokerService;
    private final Vector<Throwable> exceptions = new Vector<Throwable>();

    @Before
    public void startBroker() throws Exception {
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test(timeout = 60000)
    public void testStompConnectLeak() throws Exception {

        brokerService.addConnector("stomp://0.0.0.0:0?transport.soLinger=0");
        brokerService.start();

        StompConnection connection = new StompConnection();
        //test 500 connect/disconnects
        for (int x = 0; x < 500; x++) {
            try {
                connection.open("localhost", brokerService.getTransportConnectors().get(0).getConnectUri().getPort());
                connection.connect("system", "manager");
                connection.disconnect();
            } catch (Exception ex) {
                LOG.error("unexpected exception on connect/disconnect", ex);
                exceptions.add(ex);
            }
        }

        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == brokerService.getTransportConnectors().get(0).connectionCount();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(200)));

        assertTrue("no exceptions", exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testJaasDualStopWithOpenConnection() throws Exception {

        brokerService.setPlugins(new BrokerPlugin[]{new JaasDualAuthenticationPlugin()});
        brokerService.addConnector("stomp://0.0.0.0:0?transport.closeAsync=false");
        brokerService.start();

        final CountDownLatch doneConnect = new CountDownLatch(1);
        final int listenPort = brokerService.getTransportConnectors().get(0).getConnectUri().getPort();
        Thread t1 = new Thread() {
            StompConnection connection = new StompConnection();

            @Override
            public void run() {
                try {
                    connection.open("localhost", listenPort);
                    connection.connect("system", "manager");
                    doneConnect.countDown();
                } catch (Exception ex) {
                    LOG.error("unexpected exception on connect/disconnect", ex);
                    exceptions.add(ex);
                }
            }
        };

        t1.start();

        assertTrue("one connection", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 1 == brokerService.getTransportConnectors().get(0).connectionCount();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(200)));

        assertTrue("connected on time", doneConnect.await(5, TimeUnit.SECONDS));
        brokerService.stop();

        // server socket should be available after stop
        ServerSocket socket = ServerSocketFactory.getDefault().createServerSocket();
        socket.setReuseAddress(true);
        InetAddress address = InetAddress.getLocalHost();
        socket.bind(new InetSocketAddress(address, listenPort));
        LOG.info("bound address: " + socket);
        socket.close();
        assertTrue("no exceptions", exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testInactivityMonitor() throws Exception {

        brokerService.addConnector("stomp://0.0.0.0:0?transport.defaultHeartBeat=1000,0&transport.useKeepAlive=false");
        brokerService.start();

        Thread t1 = new Thread() {
            StompConnection connection = new StompConnection();

            @Override
            public void run() {
                try {
                    connection.open("localhost",  brokerService.getTransportConnectors().get(0).getConnectUri().getPort());
                    connection.connect("system", "manager");
                } catch (Exception ex) {
                    LOG.error("unexpected exception on connect/disconnect", ex);
                    exceptions.add(ex);
                }
            }
        };

        t1.start();

        assertTrue("one connection", Wait.waitFor(new Wait.Condition() {
             @Override
             public boolean isSatisified() throws Exception {
                 return 1 == brokerService.getTransportConnectors().get(0).connectionCount();
             }
         }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(200)));

        // and it should be closed due to inactivity
        assertTrue("no dangling connections", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 0 == brokerService.getTransportConnectors().get(0).connectionCount();
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(200)));

        assertTrue("no exceptions", exceptions.isEmpty());
    }
}