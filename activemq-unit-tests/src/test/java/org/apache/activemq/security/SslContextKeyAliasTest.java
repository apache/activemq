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
package org.apache.activemq.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.apache.activemq.spring.SpringSslContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A key store with two keys, where keyAlias on the SSL context decides
 * which one is presented.
 */
public class SslContextKeyAliasTest {

    private static final String CERT_BASE = "src/test/resources/org/apache/activemq/security";
    private static final String PASSWORD = "password";
    private static final String BROKER_DN = "CN=broker1, OU=activemq, O=apache";
    private static final String CLIENT_DN = "CN=client, OU=activemq, O=apache";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String twoKeyStore;

    @Before
    public void setUp() throws Exception {
        final var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        copyKey(store, CERT_BASE + "/broker1.ks", "broker1");
        copyKey(store, CERT_BASE + "/client.ks", "client");

        final var file = folder.newFile("two-keys.p12");
        try (var out = new FileOutputStream(file)) {
            store.store(out, PASSWORD.toCharArray());
        }
        twoKeyStore = file.toURI().toString();
    }

    private static void copyKey(KeyStore target, String path, String alias) throws Exception {
        final var source = KeyStore.getInstance(new File(path), PASSWORD.toCharArray());
        target.setKeyEntry(alias, source.getKey(alias, PASSWORD.toCharArray()), PASSWORD.toCharArray(),
            source.getCertificateChain(alias));
    }

    private SpringSslContext context(String keyAlias) throws Exception {
        return context(keyAlias, null);
    }

    private SpringSslContext context(String keyAlias, String keyStoreAlgorithm) throws Exception {
        final var context = new SpringSslContext();
        if (keyStoreAlgorithm != null) {
            context.setKeyStoreAlgorithm(keyStoreAlgorithm);
        }
        context.setKeyStore(twoKeyStore);
        context.setKeyStoreType("PKCS12");
        context.setKeyStorePassword(PASSWORD);
        context.setTrustStore(twoKeyStore);
        context.setTrustStoreType("PKCS12");
        context.setTrustStorePassword(PASSWORD);
        context.setKeyAlias(keyAlias);
        context.afterPropertiesSet();
        return context;
    }

    @Test(timeout = 60000)
    public void testServerPresentsKeyOfAlias() throws Exception {
        assertEquals(BROKER_DN, handshake(context("broker1"), context(null))[0]);
        assertEquals(CLIENT_DN, handshake(context("client"), context(null))[0]);
    }

    @Test(timeout = 60000)
    public void testClientPresentsKeyOfAlias() throws Exception {
        assertEquals(BROKER_DN, handshake(context(null), context("broker1"))[1]);
        assertEquals(CLIENT_DN, handshake(context(null), context("client"))[1]);
    }

    @Test(timeout = 60000)
    public void testAliasIsNotCaseSensitive() throws Exception {
        assertEquals(CLIENT_DN, handshake(context("CLIENT"), context(null))[0]);
    }

    @Test(timeout = 60000)
    public void testAliasWithPkixKeyManager() throws Exception {
        assertEquals(CLIENT_DN, handshake(context("client", "PKIX"), context(null))[0]);
        assertEquals(BROKER_DN, handshake(context(null), context("broker1", "PKIX"))[1]);
    }

    @Test(timeout = 60000)
    public void testContextForAnotherAlias() throws Exception {
        final var broker = context("broker1");
        assertEquals(CLIENT_DN, broker.getCertificateChain("client")[0].getSubjectX500Principal().getName()
            .replace(",", ", "));
        assertThrows(IllegalArgumentException.class, () -> broker.getCertificateChain("missing"));
        assertThrows(IllegalArgumentException.class, () -> broker.withKeyAlias("missing"));
    }

    @Test
    public void testUnknownAliasIsRefused() throws Exception {
        final var ex = assertThrows(IllegalArgumentException.class, () -> context("missing"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("'missing'"));
    }

    // returns the subject the client saw from the server, and the one the server saw from the client
    private static String[] handshake(SpringSslContext server, SpringSslContext client) throws Exception {
        final var executor = Executors.newSingleThreadExecutor();
        try (var serverSocket = (SSLServerSocket) server.getSSLContext().getServerSocketFactory().createServerSocket(0)) {
            serverSocket.setNeedClientAuth(true);
            final var seenByServer = executor.submit(() -> {
                try (var accepted = (SSLSocket) serverSocket.accept()) {
                    accepted.startHandshake();
                    return subject(accepted);
                }
            });
            try (var socket = (SSLSocket) client.getSSLContext().getSocketFactory()
                    .createSocket("localhost", serverSocket.getLocalPort())) {
                socket.startHandshake();
                final var seenByClient = subject(socket);
                return new String[] {seenByClient, seenByServer.get(30, TimeUnit.SECONDS)};
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static String subject(SSLSocket socket) throws Exception {
        return ((X509Certificate) socket.getSession().getPeerCertificates()[0]).getSubjectX500Principal().getName()
            .replace(",", ", ");
    }
}
