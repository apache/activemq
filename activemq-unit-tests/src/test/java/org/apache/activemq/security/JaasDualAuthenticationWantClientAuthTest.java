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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.transport.SslMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A connector with wantClientAuth accepts a client certificate or a user
 * name and password.
 */
@RunWith(Parameterized.class)
public class JaasDualAuthenticationWantClientAuthTest {

    private static final String LOGIN_CONFIG_PROPERTY = "java.security.auth.login.config";
    private static final String CERT_BASE = "src/test/resources/org/apache/activemq/security";
    private static final String CLIENT_DN = "CN=client, OU=activemq, O=apache";

    @Parameters(name = "{0} {1}")
    public static Collection<Object[]> schemes() {
        return Arrays.asList(new Object[][] {
            {"ssl", "transport.wantClientAuth"}, {"nio+ssl", "transport.wantClientAuth"},
            {"auto+ssl", "transport.wantClientAuth"}, {"auto+nio+ssl", "transport.wantClientAuth"},
            {"ssl", "wantClientAuth"}, {"nio+ssl", "wantClientAuth"},
            {"auto+ssl", "wantClientAuth"}, {"auto+nio+ssl", "wantClientAuth"}});
    }

    private final String scheme;
    private final String option;
    private final Map<String, String> userNameByClientId = new ConcurrentHashMap<>();

    private BrokerService broker;
    private String oldLoginConfig;
    private String connectUri;

    public JaasDualAuthenticationWantClientAuthTest(String scheme, String option) {
        this.scheme = scheme;
        this.option = option;
    }

    @Before
    public void setUp() throws Exception {
        oldLoginConfig = System.getProperty(LOGIN_CONFIG_PROPERTY);
        System.setProperty(LOGIN_CONFIG_PROPERTY, "src/test/resources/org/apache/activemq/bugs/amq4126/login.config");

        final var sslContext = new SpringSslContext();
        sslContext.setKeyStore(CERT_BASE + "/broker1.ks");
        sslContext.setKeyStorePassword("password");
        sslContext.setTrustStore(CERT_BASE + "/client.ks");
        sslContext.setTrustStorePassword("password");
        sslContext.afterPropertiesSet();

        // installed first, so it runs after the login and sees its result
        final var recorder = new BrokerPluginSupport() {
            @Override
            public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                userNameByClientId.put(info.getClientId(), context.getSecurityContext().getUserName());
                super.addConnection(context, info);
            }
        };

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setSslContext(sslContext);
        broker.setPlugins(new BrokerPlugin[] {recorder, new JaasDualAuthenticationPlugin()});
        final var connector = broker.addConnector(scheme + "://localhost:0?" + option + "=true");
        broker.start();
        broker.waitUntilStarted();

        assertEquals(SslMode.WANT_CLIENT_AUTH, connector.getSslMode());
        connectUri = "ssl://localhost:" + connector.getConnectUri().getPort() + "?socket.verifyHostName=false";
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        if (oldLoginConfig != null) {
            System.setProperty(LOGIN_CONFIG_PROPERTY, oldLoginConfig);
        } else {
            System.clearProperty(LOGIN_CONFIG_PROPERTY);
        }
    }

    @Test(timeout = 60000)
    public void testCertificateLogin() throws Exception {
        assertEquals(CLIENT_DN, connect(true, null, null));
    }

    @Test(timeout = 60000)
    public void testCertificateWinsOverUserNameAndPassword() throws Exception {
        assertEquals(CLIENT_DN, connect(true, "system", "manager"));
    }

    @Test(timeout = 60000)
    public void testUserNameAndPasswordLogin() throws Exception {
        assertEquals("system", connect(false, "system", "manager"));
    }

    @Test(timeout = 60000)
    public void testWrongPasswordIsRefused() throws Exception {
        assertThrows(JMSException.class, () -> connect(false, "system", "wrong"));
    }

    @Test(timeout = 60000)
    public void testNoCertificateAndNoCredentialsIsRefused() throws Exception {
        assertThrows(JMSException.class, () -> connect(false, null, null));
    }

    private String connect(boolean withCertificate, String userName, String password) throws Exception {
        final var factory = new ActiveMQSslConnectionFactory(connectUri);
        factory.setTrustStore("org/apache/activemq/security/broker1.ks");
        factory.setTrustStorePassword("password");
        if (withCertificate) {
            factory.setKeyStore("org/apache/activemq/security/client.ks");
            factory.setKeyStorePassword("password");
        }
        try (var connection = userName != null ? factory.createConnection(userName, password)
                : factory.createConnection()) {
            connection.start();
            return userNameByClientId.get(connection.getClientID());
        }
    }
}
