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
package org.apache.activemq.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.security.JaasDualAuthenticationPlugin;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * The local broker's key store holds two keys. remoteKeyAlias picks the one
 * shown to the remote broker and localLogin decides how the bridge logs in
 * to the local broker.
 */
public class NetworkConnectorLocalLoginTest {

    private static final String LOGIN_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static final String PASSWORD = "password";
    private static final String CLIENT_DN = "CN=client, OU=activemq, O=apache";
    private static final String BROKER1_DN = "CN=broker1, OU=activemq, O=apache";
    private static final String REMOTE_DN =
        "CN=localhost, OU=activemq.org, O=activemq.org, L=Unknown, ST=Unknown, C=Unknown";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final Map<String, String> localLogins = new ConcurrentHashMap<>();
    private final Map<String, String> remoteLogins = new ConcurrentHashMap<>();

    private String oldLoginConfig;
    private String twoKeyStore;
    private BrokerService remoteBroker;
    private BrokerService localBroker;
    private NetworkConnector networkConnector;

    @Before
    public void setUp() throws Exception {
        oldLoginConfig = System.getProperty(LOGIN_CONFIG_PROPERTY);
        System.setProperty(LOGIN_CONFIG_PROPERTY,
            "src/test/resources/org/apache/activemq/network/locallogin/login.config");

        final var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        copyKey(store, "src/test/resources/org/apache/activemq/security/broker1.ks", "broker1");
        copyKey(store, "src/test/resources/org/apache/activemq/security/client.ks", "client");
        final var file = folder.newFile("two-keys.p12");
        try (var out = new FileOutputStream(file)) {
            store.store(out, PASSWORD.toCharArray());
        }
        twoKeyStore = file.toURI().toString();

        remoteBroker = createBroker("remote", remoteLogins);
        remoteBroker.setSslContext(sslContext("src/test/resources/server.keystore", "jks", twoKeyStore, "PKCS12"));
        remoteBroker.addConnector("ssl://localhost:0?transport.needClientAuth=true");
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        localBroker = createBroker("local", localLogins);
        final int port = remoteBroker.getTransportConnectors().get(0).getConnectUri().getPort();
        networkConnector = localBroker.addNetworkConnector("static:(ssl://localhost:" + port + ")");
        networkConnector.setSslContext(sslContext(twoKeyStore, "PKCS12", "src/test/resources/client.keystore", "jks"));
        networkConnector.setUserName("system");
        networkConnector.setPassword("manager");
        networkConnector.setRemoteKeyAlias("client");
        networkConnector.setLocalKeyAlias("broker1");
    }

    @After
    public void tearDown() throws Exception {
        for (final var broker : new BrokerService[] {localBroker, remoteBroker}) {
            if (broker != null) {
                broker.stop();
                broker.waitUntilStopped();
            }
        }
        if (oldLoginConfig != null) {
            System.setProperty(LOGIN_CONFIG_PROPERTY, oldLoginConfig);
        } else {
            System.clearProperty(LOGIN_CONFIG_PROPERTY);
        }
    }

    @Test(timeout = 60000)
    public void testDefaultUsesCertificateOfRemoteBroker() throws Exception {
        startBridge();
        assertEquals(REMOTE_DN, login(localLogins, "inbound"));
        assertEquals(CLIENT_DN, login(remoteLogins, "outbound"));
    }

    @Test(timeout = 60000)
    public void testUserPassword() throws Exception {
        networkConnector.setLocalLogin("userPassword");
        startBridge();
        assertEquals("system", login(localLogins, "inbound"));
        assertEquals(CLIENT_DN, login(remoteLogins, "outbound"));
    }

    @Test(timeout = 60000)
    public void testLocalKeyAlias() throws Exception {
        networkConnector.setLocalLogin("localKeyAlias");
        startBridge();
        assertEquals(BROKER1_DN, login(localLogins, "inbound"));
        assertEquals(CLIENT_DN, login(remoteLogins, "outbound"));
    }

    @Test(timeout = 60000)
    public void testRemoteKeyAlias() throws Exception {
        networkConnector.setLocalLogin("remoteKeyAlias");
        startBridge();
        assertEquals(CLIENT_DN, login(localLogins, "inbound"));
        assertEquals(CLIENT_DN, login(remoteLogins, "outbound"));
    }

    @Test(timeout = 60000)
    public void testRemoteKeyAliasPicksTheOtherKey() throws Exception {
        networkConnector.setRemoteKeyAlias("broker1");
        networkConnector.setLocalLogin("remoteKeyAlias");
        startBridge();
        assertEquals(BROKER1_DN, login(localLogins, "inbound"));
        assertEquals(BROKER1_DN, login(remoteLogins, "outbound"));
    }

    // the accepting broker of a duplex bridge keeps using the certificate of the broker that connected
    @Test(timeout = 60000)
    public void testDuplexAcceptingSideIsNotAffected() throws Exception {
        networkConnector.setDuplex(true);
        networkConnector.setLocalLogin("userPassword");
        startBridge();
        assertEquals("system", login(localLogins, "inbound"));
        assertEquals(CLIENT_DN, login(remoteLogins, "inbound"));
    }

    @Test
    public void testUnknownLocalLoginStopsTheConnector() {
        networkConnector.setLocalLogin("userpassword");
        final var ex = assertThrows(Exception.class, () -> localBroker.start());
        assertTrue(String.valueOf(ex), String.valueOf(ex).contains("localLogin must be one of"));
        assertTrue("no bridge may be created", networkConnector.activeBridges().isEmpty());
        assertTrue("no login may reach the remote broker", remoteLogins.isEmpty());
    }

    @Test
    public void testUnknownKeyAliasStopsTheConnector() {
        networkConnector.setRemoteKeyAlias("missing");
        final var ex = assertThrows(Exception.class, () -> localBroker.start());
        assertTrue(String.valueOf(ex), String.valueOf(ex).contains("No key with alias 'missing'"));
        assertTrue("no login may reach the remote broker", remoteLogins.isEmpty());
    }

    @Test
    public void testUnknownLocalKeyAliasStopsTheConnector() {
        networkConnector.setLocalKeyAlias("missing");
        networkConnector.setLocalLogin("localKeyAlias");
        final var ex = assertThrows(Exception.class, () -> localBroker.start());
        assertTrue(String.valueOf(ex), String.valueOf(ex).contains("No key with alias 'missing'"));
        assertTrue("no login may reach the remote broker", remoteLogins.isEmpty());
    }

    @Test
    public void testLocalLoginNeedsItsAlias() {
        networkConnector.setLocalKeyAlias(null);
        networkConnector.setLocalLogin("localKeyAlias");
        final var ex = assertThrows(Exception.class, () -> localBroker.start());
        assertTrue(String.valueOf(ex), String.valueOf(ex).contains("localKeyAlias"));
        assertTrue("no login may reach the remote broker", remoteLogins.isEmpty());
    }

    private void startBridge() throws Exception {
        localBroker.start();
        localBroker.waitUntilStarted();
        assertTrue("bridge should start", Wait.waitFor(() -> !networkConnector.activeBridges().isEmpty(), 30000, 10));
    }

    private static String login(Map<String, String> logins, String clientIdPart) throws Exception {
        assertTrue("no " + clientIdPart + " login in " + logins, Wait.waitFor(
            () -> logins.keySet().stream().anyMatch(clientId -> clientId.contains(clientIdPart)), 30000, 10));
        return logins.entrySet().stream().filter(entry -> entry.getKey().contains(clientIdPart))
            .findFirst().orElseThrow().getValue();
    }

    private static BrokerService createBroker(String name, Map<String, String> logins) {
        // installed first, so it runs after the login and sees its result
        final var recorder = new BrokerPluginSupport() {
            @Override
            public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                logins.put(info.getClientId(), context.getSecurityContext().getUserName());
                super.addConnection(context, info);
            }
        };
        final var broker = new BrokerService();
        broker.setBrokerName(name);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setPlugins(new BrokerPlugin[] {recorder, new JaasDualAuthenticationPlugin()});
        return broker;
    }

    private static SpringSslContext sslContext(String keyStore, String keyStoreType, String trustStore,
            String trustStoreType) throws Exception {
        final var context = new SpringSslContext();
        context.setKeyStore(keyStore);
        context.setKeyStoreType(keyStoreType);
        context.setKeyStorePassword(PASSWORD);
        context.setTrustStore(trustStore);
        context.setTrustStoreType(trustStoreType);
        context.setTrustStorePassword(PASSWORD);
        context.afterPropertiesSet();
        return context;
    }

    private static void copyKey(KeyStore target, String path, String alias) throws Exception {
        final var source = KeyStore.getInstance(new File(path), PASSWORD.toCharArray());
        target.setKeyEntry(alias, source.getKey(alias, PASSWORD.toCharArray()), PASSWORD.toCharArray(),
            source.getCertificateChain(alias));
    }
}
