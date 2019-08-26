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
package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.util.Wait;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test STOMP over WebSockets functionality.
 */
public class StompWSTransportTest extends WSTransportTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompWSTransportTest.class);

    protected WebSocketClient wsClient;
    protected StompWSConnection wsStompConnection;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        wsStompConnection = new StompWSConnection();


        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setSubProtocols("v11.stomp");

        wsClient = new WebSocketClient(new SslContextFactory.Client(true));
        wsClient.start();

        wsClient.connect(wsStompConnection, wsConnectUri, request);
        if (!wsStompConnection.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to STOMP WS endpoint");
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (wsStompConnection != null) {
            wsStompConnection.close();
            wsStompConnection = null;
            wsClient = null;
        }

        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testConnect() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertNotNull(incoming);
        assertTrue(incoming.startsWith("CONNECTED"));
        assertEquals("v11.stomp", wsStompConnection.getConnection().getUpgradeResponse().getAcceptedSubProtocol());

        assertTrue("Connection should close", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 1;
            }
        }));

        wsStompConnection.sendFrame(new StompFrame(Stomp.Commands.DISCONNECT));
        wsStompConnection.close();

        assertTrue("Connection should close", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testConnectWithVersionOptions() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.0,1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(connectFrame);

        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);

        assertTrue(incoming.startsWith("CONNECTED"));
        assertTrue(incoming.indexOf("version:1.1") >= 0);
        assertTrue(incoming.indexOf("session:") >= 0);

        wsStompConnection.sendFrame(new StompFrame(Stomp.Commands.DISCONNECT));
        wsStompConnection.close();
    }

    @Test(timeout = 60000)
    public void testRejectInvalidHeartbeats1() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(connectFrame);

        try {
            String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);

            assertTrue(incoming.startsWith("ERROR"));
            assertTrue(incoming.indexOf("heart-beat") >= 0);
            assertTrue(incoming.indexOf("message:") >= 0);
        } catch (IOException ex) {
            LOG.debug("Connection closed before Frame was read.");
        }

        assertTrue("Connection should close", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testRejectInvalidHeartbeats2() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:T,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(connectFrame);

        try {
            String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);

            assertTrue(incoming.startsWith("ERROR"));
            assertTrue(incoming.indexOf("heart-beat") >= 0);
            assertTrue(incoming.indexOf("message:") >= 0);
        } catch (IOException ex) {
            LOG.debug("Connection closed before Frame was read.");
        }

        assertTrue("Connection should close", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testRejectInvalidHeartbeats3() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:100,10,50\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(connectFrame);

        try {
            String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);

            assertTrue(incoming.startsWith("ERROR"));
            assertTrue(incoming.indexOf("heart-beat") >= 0);
            assertTrue(incoming.indexOf("message:") >= 0);
        } catch (IOException ex) {
            LOG.debug("Connection closed before Frame was read.");
        }

        assertTrue("Connection should close", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testHeartbeatsDropsIdleConnection() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:1000,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);
        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertTrue(incoming.startsWith("CONNECTED"));
        assertTrue(incoming.indexOf("version:1.1") >= 0);
        assertTrue(incoming.indexOf("heart-beat:") >= 0);
        assertTrue(incoming.indexOf("session:") >= 0);

        assertTrue("Broker should have closed WS connection:", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !wsStompConnection.isConnected();
            }
        }));
    }

    @Test(timeout = 60000)
    public void testHeartbeatsKeepsConnectionOpen() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:2000,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);
        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertTrue(incoming.startsWith("CONNECTED"));
        assertTrue(incoming.indexOf("version:1.1") >= 0);
        assertTrue(incoming.indexOf("heart-beat:") >= 0);
        assertTrue(incoming.indexOf("session:") >= 0);

        String message = "SEND\n" + "destination:/queue/" + getTestName() + "\n\n" + "Hello World" + Stomp.NULL;
        wsStompConnection.sendRawFrame(message);

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Sending next KeepAlive");
                    wsStompConnection.keepAlive();
                } catch (Exception e) {
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        TimeUnit.SECONDS.sleep(15);

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getTestName() + "\n" +
                       "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(frame);

        incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertTrue(incoming.startsWith("MESSAGE"));

        service.shutdownNow();
        service.awaitTermination(5, TimeUnit.SECONDS);

        try {
            wsStompConnection.sendFrame(new StompFrame(Stomp.Commands.DISCONNECT));
        } catch (Exception ex) {
            LOG.info("Caught exception on write of disconnect", ex);
        }
    }

    @Test(timeout = 60000)
    public void testEscapedHeaders() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:0,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);
        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertTrue(incoming.startsWith("CONNECTED"));

        String message = "SEND\n" + "destination:/queue/" + getTestName() + "\nescaped-header:one\\ntwo\\cthree\n\n" + "Hello World" + Stomp.NULL;
        wsStompConnection.sendRawFrame(message);

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getTestName() + "\n" +
                       "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        wsStompConnection.sendRawFrame(frame);

        incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertTrue(incoming.startsWith("MESSAGE"));
        assertTrue(incoming.indexOf("escaped-header:one\\ntwo\\cthree") >= 0);

        try {
            wsStompConnection.sendFrame(new StompFrame(Stomp.Commands.DISCONNECT));
        } catch (Exception ex) {
            LOG.info("Caught exception on write of disconnect", ex);
        }
    }
}
