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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.transport.amqp.AmqpTestSupport;
import org.junit.After;

/**
 * Test support class for tests that will be using the AMQP Proton wrapper client.
 */
public class AmqpClientTestSupport extends AmqpTestSupport {

    private String connectorScheme = "amqp";
    private boolean useSSL;

    private List<AmqpConnection> connections = new ArrayList<AmqpConnection>();

    public AmqpClientTestSupport() {
    }

    public AmqpClientTestSupport(String connectorScheme, boolean useSSL) {
        this.connectorScheme = connectorScheme;
        this.useSSL = useSSL;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        for (AmqpConnection connection : connections) {
            try {
                connection.close();
            } catch (Exception ex) {}
        }

        super.tearDown();
    }

    public String getConnectorScheme() {
        return connectorScheme;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public String getAmqpConnectionURIOptions() {
        return "";
    }

    @Override
    protected boolean isUseTcpConnector() {
        return !isUseSSL() && !connectorScheme.contains("nio") && !connectorScheme.contains("ws");
    }

    @Override
    protected boolean isUseSslConnector() {
        return isUseSSL() && !connectorScheme.contains("nio") && !connectorScheme.contains("wss");
    }

    @Override
    protected boolean isUseNioConnector() {
        return !isUseSSL() && connectorScheme.contains("nio");
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return isUseSSL() && connectorScheme.contains("nio");
    }

    @Override
    protected boolean isUseWsConnector() {
        return !isUseSSL() && connectorScheme.contains("ws");
    }

    @Override
    protected boolean isUseWssConnector() {
        return isUseSSL() && connectorScheme.contains("wss");
    }

    public URI getBrokerAmqpConnectionURI() {
        boolean webSocket = false;

        try {
            int port = 0;
            switch (connectorScheme) {
                case "amqp":
                    port = this.amqpPort;
                    break;
                case "amqp+ws":
                    port = this.amqpWsPort;
                    webSocket = true;
                    break;
                case "amqp+wss":
                    port = this.amqpWssPort;
                    webSocket = true;
                    break;
                case "amqp+ssl":
                    port = this.amqpSslPort;
                    break;
                case "amqp+nio":
                    port = this.amqpNioPort;
                    break;
                case "amqp+nio+ssl":
                    port = this.amqpNioPlusSslPort;
                    break;
                default:
                    throw new IOException("Invalid AMQP connector scheme passed to test.");
            }

            String uri = null;

            if (isUseSSL()) {
                if (webSocket) {
                    uri = "wss://127.0.0.1:" + port;
                } else {
                    uri = "ssl://127.0.0.1:" + port;
                }
            } else {
                if (webSocket) {
                    uri = "ws://127.0.0.1:" + port;
                } else {
                    uri = "tcp://127.0.0.1:" + port;
                }
            }

            if (!getAmqpConnectionURIOptions().isEmpty()) {
                uri = uri + "?" + getAmqpConnectionURIOptions();
            }

            return new URI(uri);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public AmqpConnection trackConnection(AmqpConnection connection) {
        connections.add(connection);
        return connection;
    }

    public AmqpConnection createAmqpConnection() throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI());
    }

    public AmqpConnection createAmqpConnection(String username, String password) throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI) throws Exception {
        return createAmqpConnection(brokerURI, null, null);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
        return trackConnection(createAmqpClient(brokerURI, username, password).connect());
    }

    public AmqpClient createAmqpClient() throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), null, null);
    }

    public AmqpClient createAmqpClient(URI brokerURI) throws Exception {
        return createAmqpClient(brokerURI, null, null);
    }

    public AmqpClient createAmqpClient(String username, String password) throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpClient createAmqpClient(URI brokerURI, String username, String password) throws Exception {
        return new AmqpClient(brokerURI, username, password);
    }
}
