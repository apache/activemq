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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.DefaultSslContext;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds every SSL capable transport with URI options and a per-connector
 * {@link SslContext}, then verifies through a raw TLS handshake what the
 * accepted socket or engine actually negotiated.
 *
 * <p>Assertions are black-box on purpose: {@code transport.*} options are
 * applied to the {@code SSLServerSocket} or the per-connection
 * {@code SSLEngine}, not to the flags on the server object, so the server
 * getters would not reflect them. The handshake completes before any protocol
 * bytes flow, which is what lets one matrix cover OpenWire, AMQP, MQTT and
 * STOMP transports alike.
 */
@Category(ParallelTest.class)
@RunWith(Parameterized.class)
public class SslConnectorOptionsAndContextTest {

    private static final Logger LOG = LoggerFactory.getLogger(SslConnectorOptionsAndContextTest.class);

    private static final char[] PASSWORD = "password".toCharArray();
    private static final String KEYSTORE = "keystore";
    private static final String KEYSTORE_ALIAS = "activemq";
    private static final String ALTERNATIVE_KEYSTORE = "alternative.keystore";
    private static final String ALTERNATIVE_ALIAS = "alternative";

    private static final String HARDENED = "?transport.needClientAuth=true&transport.enabledProtocols=TLSv1.2";
    private static final String EXPECTED_PROTOCOL = "TLSv1.2";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> transports() {
        return Arrays.asList(new Object[][] {
            {"ssl"},
            {"nio+ssl"},
            {"auto+ssl"},
            {"auto+nio+ssl"},
            {"amqp+ssl"},
            {"amqp+nio+ssl"},
            {"mqtt+ssl"},
            {"mqtt+nio+ssl"},
            {"stomp+ssl"},
            {"stomp+nio+ssl"},
        });
    }

    @Parameterized.Parameter
    public String transport;

    private BrokerService broker;

    @Before
    public void setUp() {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    // ---- transport.* options reach the accepted socket / engine ----------

    @Test(timeout = 60000)
    public void testNeedClientAuthAndEnabledProtocolsApplied() throws Exception {
        var connector = addConnector(HARDENED, sslContext(KEYSTORE));
        broker.start();
        broker.waitUntilStarted();

        assertHardened(connector);
    }

    /**
     * Confirms current behavior: an unknown {@code transport.} option is silently
     * dropped and the connector still binds with the valid options applied.
     * If option validation is ever tightened this test must change with it.
     */
    @Test(timeout = 60000)
    public void testUnknownTransportOptionIsIgnored() throws Exception {
        var connector = addConnector(HARDENED + "&transport.cheese=abc", sslContext(KEYSTORE));
        broker.start();
        broker.waitUntilStarted();

        assertHardened(connector);
    }

    /**
     * Confirms current behavior: an unknown top level option is silently dropped
     * on bind. The connect side rejects the same option with
     * "Invalid connect parameters"; the bind side does not.
     */
    @Test(timeout = 60000)
    public void testUnknownTopLevelOptionIsIgnored() throws Exception {
        var connector = addConnector(HARDENED + "&cheese=abc", sslContext(KEYSTORE));
        broker.start();
        broker.waitUntilStarted();

        assertHardened(connector);
    }

    /**
     * Confirms current behavior: an unknown {@code wireFormat.} option is silently
     * dropped on bind.
     */
    @Test(timeout = 60000)
    public void testUnknownWireFormatOptionIsIgnored() throws Exception {
        var connector = addConnector(HARDENED + "&wireFormat.cheese=abc", sslContext(KEYSTORE));
        broker.start();
        broker.waitUntilStarted();

        assertHardened(connector);
    }

    /**
     * Three connectors of the same transport on one broker: A and B carry
     * their own SslContext with different identities, C has none and must
     * fall back to the broker level context. Each must present the
     * certificate of the context that applies to it.
     */
    @Test(timeout = 60000)
    public void testPerConnectorSslContextIsolation() throws Exception {
        broker.setSslContext(sslContext(ALTERNATIVE_KEYSTORE));
        var connectorA = addConnector("", sslContext(KEYSTORE));
        var connectorB = addConnector("", sslContext(ALTERNATIVE_KEYSTORE));
        var connectorC = addConnector("", null);
        broker.start();
        broker.waitUntilStarted();

        var expectedA = certificate(KEYSTORE, KEYSTORE_ALIAS);
        var expectedAlternative = certificate(ALTERNATIVE_KEYSTORE, ALTERNATIVE_ALIAS);

        assertEquals("connector A must present its own certificate", expectedA, handshake(connectorA, null).leaf());
        assertEquals("connector B must present its own certificate", expectedAlternative, handshake(connectorB, null).leaf());
        assertEquals("connector C must fall back to the broker certificate", expectedAlternative, handshake(connectorC, null).leaf());
    }

    /**
     * Builds the connector by hand so the bind happens at broker start with the
     * connector's own SslContext. {@code BrokerService.addConnector(URI)} binds
     * eagerly with the broker level context at the time of the call, so a
     * {@code setSslContext} on the connector it returns has no effect.
     */
    private TransportConnector addConnector(String options, SslContext sslContext) throws Exception {
        var connector = new TransportConnector();
        connector.setUri(new URI(transport + "://localhost:0" + options));
        connector.setSslContext(sslContext);
        return broker.addConnector(connector);
    }

    private void assertHardened(TransportConnector connector) throws Exception {
        // A client presenting a certificate the connector trusts completes the
        // handshake, and only TLSv1.2 can be negotiated.
        var result = handshake(connector, keyManagers(KEYSTORE));
        LOG.info("{} negotiated {}", connector.getConnectUri(), result.protocol());
        assertEquals(EXPECTED_PROTOCOL, result.protocol());
        assertEquals(certificate(KEYSTORE, KEYSTORE_ALIAS), result.leaf());

        // A client with no certificate is refused. Restricting to TLSv1.2 is
        // what makes this deterministic: the server rejects the empty client
        // Certificate message before its Finished, so the failure surfaces in
        // startHandshake() rather than on a later read as it can with TLSv1.3.
        var thrown = assertThrows(IOException.class, () -> handshake(connector, null));
        LOG.info("{} refused a client without a certificate: {}", connector.getConnectUri(), thrown.toString());
    }

    private static Handshake handshake(TransportConnector connector, KeyManager[] clientKeyManagers) throws Exception {
        var catcher = new CertChainCatcher();
        var context = SSLContext.getInstance("TLS");
        context.init(clientKeyManagers, new TrustManager[] {catcher}, null);

        var uri = connector.getConnectUri();
        try (var socket = (SSLSocket) context.getSocketFactory().createSocket(uri.getHost(), uri.getPort())) {
            socket.setSoTimeout(10000);
            socket.startHandshake();
            assertNotNull("server did not present a certificate chain", catcher.serverCerts);
            assertTrue("server presented an empty certificate chain", catcher.serverCerts.length > 0);
            return new Handshake(socket.getSession().getProtocol(), catcher.serverCerts[0]);
        }
    }

    private record Handshake(String protocol, X509Certificate leaf) {
    }

    private static SslContext sslContext(String keystoreName) throws Exception {
        var keyStore = loadKeyStore(keystoreName);
        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, PASSWORD);
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        return new DefaultSslContext(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    }

    private static KeyManager[] keyManagers(String keystoreName) throws Exception {
        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(loadKeyStore(keystoreName), PASSWORD);
        return kmf.getKeyManagers();
    }

    private static X509Certificate certificate(String keystoreName, String alias) throws Exception {
        var certificate = (X509Certificate) loadKeyStore(keystoreName).getCertificate(alias);
        assertNotNull("alias " + alias + " not found in " + keystoreName, certificate);
        return certificate;
    }

    private static KeyStore loadKeyStore(String keystoreName) throws Exception {
        var url = SslConnectorOptionsAndContextTest.class.getClassLoader().getResource(keystoreName);
        assertNotNull("test keystore not on classpath: " + keystoreName, url);
        var keyStore = KeyStore.getInstance("jks");
        try (var in = new FileInputStream(new File(url.toURI()))) {
            keyStore.load(in, PASSWORD);
        }
        return keyStore;
    }

    /** Accepts any server certificate and records the chain it presented. */
    private static final class CertChainCatcher implements X509TrustManager {
        volatile X509Certificate[] serverCerts;

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            serverCerts = chain;
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
