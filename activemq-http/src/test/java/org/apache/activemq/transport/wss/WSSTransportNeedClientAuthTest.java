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
package org.apache.activemq.transport.wss;

import junit.framework.Assert;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.ws.MQTTWSConnection;
import org.apache.activemq.transport.ws.StompWSConnection;
import org.apache.activemq.util.Wait;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.eclipse.jetty.websocket.client.io.ConnectPromise;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class WSSTransportNeedClientAuthTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final String KEYSTORE = "src/test/resources/server.keystore";


    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = BrokerFactory.createBroker("xbean:activemq-https-need-client-auth.xml");
        broker.setPersistent(false);
        broker.start();
        broker.waitUntilStarted();

        // these are used for the client side... for the server side, the SSL context
        // will be configured through the <sslContext> spring beans
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testStompNeedClientAuth() throws Exception {
        StompWSConnection wsStompConnection = new StompWSConnection();
        System.out.println("starting connection");
        SslContextFactory factory = new SslContextFactory();
        factory.setKeyStorePath(KEYSTORE);
        factory.setKeyStorePassword(PASSWORD);
        factory.setKeyStoreType(KEYSTORE_TYPE);
        factory.setTrustStorePath(TRUST_KEYSTORE);
        factory.setTrustStorePassword(PASSWORD);
        factory.setTrustStoreType(KEYSTORE_TYPE);
        WebSocketClient wsClient = new WebSocketClient(factory);
        wsClient.start();

        Future<Session> connected = wsClient.connect(wsStompConnection, new URI("wss://localhost:61618"));
        Session sess = connected.get(30, TimeUnit.SECONDS);

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

        wsStompConnection.sendFrame(new StompFrame(Stomp.Commands.DISCONNECT));
        wsStompConnection.close();

    }

    @Test
    public void testMQTTNeedClientAuth() throws Exception {
        SslContextFactory factory = new SslContextFactory();
        factory.setKeyStorePath(KEYSTORE);
        factory.setKeyStorePassword(PASSWORD);
        factory.setKeyStoreType(KEYSTORE_TYPE);
        factory.setTrustStorePath(TRUST_KEYSTORE);
        factory.setTrustStorePassword(PASSWORD);
        factory.setTrustStoreType(KEYSTORE_TYPE);
        WebSocketClient wsClient = new WebSocketClient(factory);
        wsClient.start();

        ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setSubProtocols("mqttv3.1");

        MQTTWSConnection wsMQTTConnection = new MQTTWSConnection();

        wsClient.connect(wsMQTTConnection, new URI("wss://localhost:61618"), request);
        if (!wsMQTTConnection.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to MQTT WS endpoint");
        }

        wsMQTTConnection.connect();

        assertTrue("Client not connected", wsMQTTConnection.isConnected());

        wsMQTTConnection.disconnect();
        wsMQTTConnection.close();

    }

}
