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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagementContextSslTest {

    private static final Logger LOG = LoggerFactory.getLogger(ManagementContextSslTest.class);
    private static final String KEYSTORE_PASSWORD = "password";

    private static Path tempDir;
    private static Path keystoreFile;
    private static SSLContext testSslContext;
    private ManagementContext context;
    private SSLContext savedDefaultSslContext;
    private String savedTrustStore;
    private String savedTrustStorePassword;

    @BeforeClass
    public static void createKeyStore() throws Exception {
        tempDir = Files.createTempDirectory("test-jmx-ssl");
        keystoreFile = tempDir.resolve("keystore.p12");
        final Process p = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-keystore", keystoreFile.toString(),
                "-storetype", "PKCS12",
                "-storepass", KEYSTORE_PASSWORD,
                "-keypass", KEYSTORE_PASSWORD,
                "-alias", "test",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-dname", "CN=localhost,O=Test",
                "-validity", "1"
        ).inheritIO().start();
        assertEquals("keytool should succeed", 0, p.waitFor());

        // Build a reusable SSLContext from the generated keystore
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        try (final InputStream fis = Files.newInputStream(keystoreFile)) {
            ks.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, KEYSTORE_PASSWORD.toCharArray());
        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        testSslContext = SSLContext.getInstance("TLS");
        testSslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    }

    @AfterClass
    public static void cleanupKeyStore() throws Exception {
        if (keystoreFile != null) {
            Files.deleteIfExists(keystoreFile);
        }
        if (tempDir != null) {
            Files.deleteIfExists(tempDir);
        }
    }

    @Before
    public void setUp() throws Exception {
        savedDefaultSslContext = SSLContext.getDefault();
        savedTrustStore = System.getProperty("javax.net.ssl.trustStore");
        savedTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    }

    @After
    public void tearDown() throws Exception {
        if (context != null) {
            context.stop();
        }
        SSLContext.setDefault(savedDefaultSslContext);
        restoreSystemProperty("javax.net.ssl.trustStore", savedTrustStore);
        restoreSystemProperty("javax.net.ssl.trustStorePassword", savedTrustStorePassword);
    }

    @Test
    public void testSslContextProperty() {
        context = new ManagementContext();
        assertNull("sslContext should be null by default", context.getSslContext());

        final SslContext ssl = new SslContext();
        context.setSslContext(ssl);
        assertSame("sslContext should be the one we set", ssl, context.getSslContext());
    }

    @Test
    public void testEphemeralPortResolution() throws Exception {
        context = new ManagementContext();
        context.setCreateConnector(true);
        context.setConnectorPort(0);
        context.setConnectorHost("localhost");

        assertEquals("Before start, port should be 0", 0, context.getConnectorPort());

        context.start();

        assertTrue("Connector should be started",
                Wait.waitFor(context::isConnectorStarted, 10_000, 100));

        final int resolvedPort = context.getConnectorPort();
        assertTrue("After start, port should be resolved to a real port (got " + resolvedPort + ")",
                resolvedPort > 0);
        LOG.info("Ephemeral port resolved to {}", resolvedPort);
    }

    @Test
    public void testEphemeralPortsAreDifferentPerInstance() throws Exception {
        context = new ManagementContext();
        context.setCreateConnector(true);
        context.setConnectorPort(0);
        context.setConnectorHost("localhost");
        context.start();

        assertTrue("First connector should be started",
                Wait.waitFor(context::isConnectorStarted, 10_000, 100));
        final int port1 = context.getConnectorPort();

        // Start a second context with ephemeral port
        final ManagementContext context2 = new ManagementContext();
        context2.setCreateConnector(true);
        context2.setConnectorPort(0);
        context2.setConnectorHost("localhost");
        try {
            context2.start();

            assertTrue("Second connector should be started",
                    Wait.waitFor(context2::isConnectorStarted, 10_000, 100));
            final int port2 = context2.getConnectorPort();

            assertTrue("Both ports should be > 0", port1 > 0 && port2 > 0);
            assertTrue("Ports should be different (port1=" + port1 + ", port2=" + port2 + ")",
                    port1 != port2);
            LOG.info("Two ephemeral ports: {} and {}", port1, port2);
        } finally {
            context2.stop();
        }
    }

    @Test
    public void testConnectorStartsWithSsl() throws Exception {
        // SslRMIClientSocketFactory uses SSLSocketFactory.getDefault() which relies on
        // SSLContext.getDefault(). Setting system properties alone is insufficient if the
        // default SSLContext was already cached by a previous test in the same JVM.
        SSLContext.setDefault(testSslContext);
        System.setProperty("javax.net.ssl.trustStore", keystoreFile.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", KEYSTORE_PASSWORD);

        context = createSslManagementContext();
        context.start();

        assertTrue("Connector should be started",
                Wait.waitFor(context::isConnectorStarted, 10_000, 100));
        assertTrue("SSL connector port should be resolved", context.getConnectorPort() > 0);
    }

    @Test
    public void testSslJmxConnectionSucceeds() throws Exception {
        // SslRMIClientSocketFactory uses SSLSocketFactory.getDefault() which relies on
        // SSLContext.getDefault(). We must set our test SSLContext as the JVM default so
        // both the server-side JNDI binding (daemon thread) and client connections trust
        // our self-signed certificate. System properties alone are insufficient if the
        // default SSLContext was already cached by a previous test in the same JVM.
        SSLContext.setDefault(testSslContext);
        System.setProperty("javax.net.ssl.trustStore", keystoreFile.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", KEYSTORE_PASSWORD);

        context = createSslManagementContext();
        context.start();

        assertTrue("Connector should be started",
                Wait.waitFor(context::isConnectorStarted, 20_000, 100));

        final int port = context.getConnectorPort();
        assertTrue("SSL connector port should be resolved", port > 0);

        final JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");
        final Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
        final AtomicReference<Exception> lastError = new AtomicReference<>();

        // Retry connection: isConnectorStarted() can return true (via isActive()) before
        // the RMI server stub is fully registered in the registry
        final boolean connected = Wait.waitFor(() -> {
            try (final JMXConnector connector = JMXConnectorFactory.connect(url, env)) {
                final MBeanServerConnection connection = connector.getMBeanServerConnection();
                LOG.info("Successfully connected to SSL JMX on port {}, found {} MBeans",
                        port, connection.getMBeanCount());
                return connection.getMBeanCount() > 0;
            } catch (final Exception e) {
                lastError.set(e);
                LOG.debug("JMX SSL connection attempt failed: {}", e.getMessage());
                return false;
            }
        }, 30_000, 500);
        final Exception error = lastError.get();
        assertTrue("Should connect to SSL JMX"
                        + (error == null ? "" : " (last error: " + error + ")"),
                connected);
    }

    @Test
    public void testConnectorStartsWithoutSsl() throws Exception {
        context = new ManagementContext();
        context.setCreateConnector(true);
        context.setConnectorPort(0);
        context.setConnectorHost("localhost");
        context.start();

        final int port = context.getConnectorPort();
        assertTrue("Port should be resolved", port > 0);

        final JMXServiceURL url = new JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");

        // Retry connection: isConnectorStarted() can return true (via isActive()) before
        // the RMI server stub is fully registered in the registry
        assertTrue("Should connect to non-SSL JMX", Wait.waitFor(() -> {
            try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
                final MBeanServerConnection connection = connector.getMBeanServerConnection();
                LOG.info("Successfully connected to non-SSL JMX on port {}", port);
                return connection.getMBeanCount() > 0;
            } catch (final IOException e) {
                LOG.debug("JMX connection not yet available: {}", e.getMessage());
                return false;
            }
        }, 10_000, 500));
    }

    private ManagementContext createSslManagementContext() {
        final SslContext sslContext = new SslContext();
        sslContext.setSSLContext(testSslContext);

        final ManagementContext ctx = new ManagementContext();
        ctx.setCreateConnector(true);
        ctx.setConnectorPort(0);
        ctx.setConnectorHost("localhost");
        ctx.setSslContext(sslContext);

        return ctx;
    }

    private static void restoreSystemProperty(final String key, final String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
