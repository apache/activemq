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
package org.apache.activemq.transport.https;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.spring.SpringSslContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The Jetty backed https and wss connectors honour requirePostQuantumKeyExchange
 * through the SslContextFactory customize hook. Lives in src/test/java27, so it
 * runs on JDK 27 and later only.
 */
@RunWith(Parameterized.class)
public class PostQuantumKeyExchangeJettyTest {

    private static final String[] CLASSICAL_ONLY = {"x25519", "secp256r1"};
    private static final String[] HYBRID_ONLY = {"X25519MLKEM768"};

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> transports() {
        return Arrays.asList(new Object[][] {{"https"}, {"wss"}});
    }

    @Parameterized.Parameter
    public String transport;

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        SpringSslContext context = new SpringSslContext();
        context.setKeyStore("src/test/resources/server.keystore");
        context.setKeyStoreKeyPassword("password");
        context.setTrustStore("src/test/resources/client.keystore");
        context.setTrustStorePassword("password");
        context.afterPropertiesSet();
        broker.setSslContext(context);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void requiredPostQuantumKeyExchangeRefusesClassicalOnlyClients() throws Exception {
        TransportConnector connector = broker.addConnector(transport + "://127.0.0.1:0?transport.requirePostQuantumKeyExchange=true");
        broker.start();
        broker.waitUntilStarted();

        assertEquals("TLSv1.3", handshake(connector, null));
        assertEquals("TLSv1.3", handshake(connector, HYBRID_ONLY));
        assertThrows(IOException.class, () -> handshake(connector, CLASSICAL_ONLY));
    }

    @Test(timeout = 60000)
    public void defaultConnectorStillAcceptsClassicalClients() throws Exception {
        TransportConnector connector = broker.addConnector(transport + "://127.0.0.1:0");
        broker.start();
        broker.waitUntilStarted();

        assertEquals("TLSv1.3", handshake(connector, CLASSICAL_ONLY));
    }

    private static String handshake(TransportConnector connector, String[] clientNamedGroups) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] {new TrustAll()}, null);
        URI uri = connector.getPublishableConnectURI();
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(uri.getHost(), uri.getPort())) {
            socket.setSoTimeout(10000);
            if (clientNamedGroups != null) {
                SSLParameters parameters = socket.getSSLParameters();
                // SSLParameters.setNamedGroups exists since Java 20; this module compiles for 17
                SSLParameters.class.getMethod("setNamedGroups", String[].class).invoke(parameters, (Object) clientNamedGroups);
                socket.setSSLParameters(parameters);
            }
            socket.startHandshake();
            return socket.getSession().getProtocol();
        }
    }

    private static final class TrustAll implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
