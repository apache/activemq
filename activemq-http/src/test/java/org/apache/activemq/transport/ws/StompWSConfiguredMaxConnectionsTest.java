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

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.util.Wait;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StompWSConfiguredMaxConnectionsTest extends WSTransportTestSupport {

    private static final String connectorScheme = "ws";

    private static final int MAX_CONNECTIONS = 10;

    @Test(timeout = 60000)
    public void testMaxConnectionsSettingIsHonored() throws Exception {
        List<StompWSConnection> connections = new ArrayList<>();

        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            StompWSConnection connection = createConnection();
            if (!connection.awaitConnection(30, TimeUnit.SECONDS)) {
                throw new IOException("Could not connect to STOMP WS endpoint");
            }

            connections.add(connection);

            String connectFrame = "STOMP\n" +
                    "login:system\n" +
                    "passcode:manager\n" +
                    "accept-version:1.0,1.1\n" +
                    "host:localhost\n" +
                    "\n" + Stomp.NULL;
            connection.sendRawFrame(connectFrame);

            String incoming = connection.receive(30, TimeUnit.SECONDS);
            assertNotNull(incoming);
            assertTrue(incoming.startsWith("CONNECTED"));
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());
        assertEquals(Long.valueOf(0l), Long.valueOf(getProxyToConnectionView(connectorScheme).getMaxConnectionExceededCount()));

        {
            StompWSConnection connection = createConnection();
            if (!connection.awaitError(30, TimeUnit.SECONDS)) {
                throw new IOException("WS endpoint has maximumConnections=" + MAX_CONNECTIONS);
            }

            connections.add(connection);
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());
        assertEquals(1, getProxyToConnectionView(connectorScheme).getMaxConnectionExceededCount());

        for (StompWSConnection connection : connections) {
            connection.close();
        }
        assertTrue("Connection should close", Wait.waitFor(() -> getProxyToBroker().getCurrentConnectionsCount() == 0));

        // Confirm reset statistics
        getProxyToConnectionView(connectorScheme).resetStatistics();
        assertEquals(0, getProxyToConnectionView(connectorScheme).getMaxConnectionExceededCount());
    }

    @Override
    protected String getWSConnectorURI() {
        return super.getWSConnectorURI() + "&maximumConnections=" + MAX_CONNECTIONS;
    }

    private StompWSConnection createConnection() throws Exception {
        StompWSConnection connection = new StompWSConnection();

        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setSubProtocols("v11.stomp");

        SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        sslContextFactory.setTrustAll(true);
        ClientConnector clientConnector = new ClientConnector();
        clientConnector.setSslContextFactory(sslContextFactory);

        WebSocketClient wsClient = new WebSocketClient(new HttpClient(new HttpClientTransportDynamic(clientConnector)));
        wsClient.start();

        wsClient.connect(connection, wsConnectUri, request);

        return connection;
    }
}
