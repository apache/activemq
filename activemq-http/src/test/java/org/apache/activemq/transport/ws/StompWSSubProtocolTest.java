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
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.stomp.Stomp;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test STOMP sub protocol detection.
 */
public class StompWSSubProtocolTest extends WSTransportTestSupport {

    protected WebSocketClient wsClient;
    protected StompWSConnection wsStompConnection;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        wsStompConnection = new StompWSConnection();
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
    public void testConnectV12() throws Exception {
        connect("v12.stomp");

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("v12.stomp");
    }

    @Test(timeout = 60000)
    public void testConnectV11() throws Exception {
        connect("v11.stomp");

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("v11.stomp");
    }

    @Test(timeout = 60000)
    public void testConnectV10() throws Exception {
        connect("v10.stomp");

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("v10.stomp");
    }

    @Test(timeout = 60000)
    public void testConnectNone() throws Exception {

        connect(null);

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("stomp");
    }

    @Test(timeout = 60000)
    public void testConnectMultiple() throws Exception {

        connect("v10.stomp,v11.stomp");

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("v11.stomp");
    }

    @Test(timeout = 60000)
    public void testConnectInvalid() throws Exception {
        connect("invalid");

        String connectFrame = "STOMP\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        wsStompConnection.sendRawFrame(connectFrame);

        assertSubProtocol("stomp");
    }

    protected void connect(String subProtocol) throws Exception{
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        if (subProtocol != null) {
            request.setSubProtocols(subProtocol);
        }

        wsClient = new WebSocketClient(new SslContextFactory(true));
        wsClient.start();

        wsClient.connect(wsStompConnection, wsConnectUri, request);
        if (!wsStompConnection.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to STOMP WS endpoint");
        }
    }

    protected void assertSubProtocol(String subProtocol) throws Exception {
        String incoming = wsStompConnection.receive(30, TimeUnit.SECONDS);
        assertNotNull(incoming);
        assertTrue(incoming.startsWith("CONNECTED"));
        assertEquals(subProtocol, wsStompConnection.getConnection().getUpgradeResponse().getAcceptedSubProtocol());
    }

}
