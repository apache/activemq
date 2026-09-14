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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.DefaultSslContext;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;

/**
 * Raw TLS helpers shared by the ssl connector tests: a broker side
 * {@link SslContext} from a test keystore, a connector built by hand so it binds
 * with that context, and a client handshake whose named groups can be chosen.
 */
public final class SslTestSupport {

    public static final char[] PASSWORD = "password".toCharArray();
    public static final String KEYSTORE = "keystore";

    private SslTestSupport() {
    }

    public static SslContext sslContext(String keystoreName) throws Exception {
        KeyStore keyStore = loadKeyStore(keystoreName);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, PASSWORD);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        return new DefaultSslContext(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    }

    /**
     * {@code BrokerService.addConnector(URI)} binds at once with the broker level
     * context, so the connector is built by hand and bound at broker start.
     */
    public static TransportConnector addConnector(BrokerService broker, String uri, SslContext sslContext) throws Exception {
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI(uri));
        connector.setSslContext(sslContext);
        return broker.addConnector(connector);
    }

    /**
     * Completes a TLS handshake against the connector, trusting whatever it
     * presents, and returns the negotiated protocol.
     *
     * @param clientNamedGroups the key exchange groups the client offers, or null for the JDK default
     * @throws IOException when the server refuses the handshake
     */
    public static String handshake(TransportConnector connector, String[] clientNamedGroups) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] {new TrustAll()}, null);
        URI uri = connector.getConnectUri();
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(uri.getHost(), uri.getPort())) {
            socket.setSoTimeout(10000);
            if (clientNamedGroups != null) {
                SSLParameters parameters = socket.getSSLParameters();
                setNamedGroups(parameters, clientNamedGroups);
                socket.setSSLParameters(parameters);
            }
            socket.startHandshake();
            return socket.getSession().getProtocol();
        }
    }

    /** SSLParameters.setNamedGroups exists since Java 20; this module compiles for 17 */
    public static void setNamedGroups(SSLParameters parameters, String[] namedGroups) throws Exception {
        SSLParameters.class.getMethod("setNamedGroups", String[].class).invoke(parameters, (Object) namedGroups);
    }

    public static KeyStore loadKeyStore(String keystoreName) throws Exception {
        var url = SslTestSupport.class.getClassLoader().getResource(keystoreName);
        assertNotNull("test keystore not on classpath: " + keystoreName, url);
        KeyStore keyStore = KeyStore.getInstance("jks");
        try (var in = new FileInputStream(new File(url.toURI()))) {
            keyStore.load(in, PASSWORD);
        }
        return keyStore;
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
