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

import org.apache.activemq.util.Wait;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MQTTWSConfiguredMaxConnectionsTest extends WSTransportTestSupport {

    private static final String connectorScheme = "ws";

    private static final int MAX_CONNECTIONS = 10;

    @Test(timeout = 60000)
    public void testMaxConnectionsSettingIsHonored() throws Exception {
        List<MQTTWSConnection> connections = new ArrayList<>();

        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            MQTTWSConnection connection = createConnection();
            if (!connection.awaitConnection(30, TimeUnit.SECONDS)) {
                throw new IOException("Could not connect to STOMP WS endpoint");
            }

            connections.add(connection);

            CONNECT command = new CONNECT();

            command.clientId(new UTF8Buffer(UUID.randomUUID().toString()));
            command.cleanSession(false);
            command.version(3);
            command.keepAlive((short) 60);

            connection.sendFrame(command.encode());

            MQTTFrame received = connection.receive(15, TimeUnit.SECONDS);
            if (received == null || received.messageType() != CONNACK.TYPE) {
                fail("Client did not get expected CONNACK");
            }
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());
        assertEquals(Long.valueOf(0L), Long.valueOf(getProxyToConnectionView(connectorScheme).getMaxConnectionExceededCount()));

        {
            MQTTWSConnection connection = createConnection();
            if (!connection.awaitError(30, TimeUnit.SECONDS)) {
                throw new IOException("WS endpoint has maximumConnections=" + MAX_CONNECTIONS);
            }

            connections.add(connection);
        }

        assertEquals(MAX_CONNECTIONS, getProxyToBroker().getCurrentConnectionsCount());
        assertEquals(1, getProxyToConnectionView(connectorScheme).getMaxConnectionExceededCount());

        for (MQTTWSConnection connection : connections) {
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

    private MQTTWSConnection createConnection() throws Exception {
        MQTTWSConnection connection = new MQTTWSConnection();

        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setSubProtocols("mqttv3.1");

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
