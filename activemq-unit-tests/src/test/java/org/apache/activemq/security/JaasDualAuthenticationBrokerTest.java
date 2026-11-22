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

import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import javax.net.ssl.SSLServerSocket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import junit.framework.TestCase;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.StubBroker;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;
import org.apache.activemq.transport.SslMode;
import org.apache.activemq.transport.tcp.SslTransportServer;
import org.apache.activemq.transport.tcp.StubSSLServerSocket;
import org.apache.activemq.transport.tcp.StubSSLSocketFactory;
import org.apache.activemq.transport.tcp.StubX509Certificate;
import org.apache.activemq.transport.tcp.TcpTransportServer;

/**
 *
 */
public class JaasDualAuthenticationBrokerTest extends TestCase {

    private static final String INSECURE_GROUP = "insecureGroup";
    private static final String INSECURE_USERNAME = "insecureUserName";
    private static final String DN_GROUP = "dnGroup";
    private static final String DN_USERNAME = "dnUserName";

    StubBroker receiveBroker;
    JaasDualAuthenticationBroker authBroker;

    ConnectionContext connectionContext;
    ConnectionInfo connectionInfo;

    SslTransportServer sslTransportServer;
    TcpTransportServer nonSslTransportServer;

    /** create a dual login config, for both SSL and non-SSL connections
     * using the StubLoginModule
     *
     */
    void createLoginConfig() {
        createLoginConfig(true, true);
    }

    void createLoginConfig(boolean allowCertificateLogin, boolean allowUserNameLogin) {
        HashMap<String, String> sslConfigOptions = new HashMap<String, String>();
        HashMap<String, String> configOptions = new HashMap<String, String>();

        sslConfigOptions.put(StubLoginModule.ALLOW_LOGIN_PROPERTY, String.valueOf(allowCertificateLogin));
        sslConfigOptions.put(StubLoginModule.USERS_PROPERTY, DN_USERNAME);
        sslConfigOptions.put(StubLoginModule.GROUPS_PROPERTY, DN_GROUP);
        AppConfigurationEntry sslConfigEntry = new AppConfigurationEntry("org.apache.activemq.security.StubLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, sslConfigOptions);

        configOptions.put(StubLoginModule.ALLOW_LOGIN_PROPERTY, String.valueOf(allowUserNameLogin));
        configOptions.put(StubLoginModule.USERS_PROPERTY, INSECURE_USERNAME);
        configOptions.put(StubLoginModule.GROUPS_PROPERTY, INSECURE_GROUP);
        AppConfigurationEntry configEntry = new AppConfigurationEntry("org.apache.activemq.security.StubLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, configOptions);

        StubDualJaasConfiguration jaasConfig = new StubDualJaasConfiguration(configEntry, sslConfigEntry);

        Configuration.setConfiguration(jaasConfig);
    }

    @Override
    protected void setUp() throws Exception {
        receiveBroker = new StubBroker();

        authBroker = new JaasDualAuthenticationBroker(receiveBroker, "activemq-domain", "activemq-ssl-domain");

        connectionContext = new ConnectionContext();

        SSLServerSocket sslServerSocket = new StubSSLServerSocket();
        StubSSLSocketFactory socketFactory = new StubSSLSocketFactory(sslServerSocket);

        try {
            sslTransportServer = new SslTransportServer(null, new URI("ssl://localhost:61616?needClientAuth=true"),
                    socketFactory);
        } catch (Exception e) {
            fail("Unable to create SslTransportServer.");
        }
        sslTransportServer.setNeedClientAuth(true);
        sslTransportServer.bind();

        try {
            nonSslTransportServer = new TcpTransportServer(null, new URI("tcp://localhost:61613"), socketFactory);
        } catch (Exception e) {
            fail("Unable to create TcpTransportServer.");
        }


        connectionInfo = new ConnectionInfo();

        createLoginConfig();
    }

    @Override
    protected void tearDown() throws Exception {
            super.tearDown();
    }


    private Connector sslConnector(boolean needClientAuth, boolean wantClientAuth) throws Exception {
        SslTransportServer server = new SslTransportServer(null, new URI("ssl://localhost:61616"),
                new StubSSLSocketFactory(new StubSSLServerSocket()));
        server.setNeedClientAuth(needClientAuth);
        server.setWantClientAuth(wantClientAuth);
        server.bind();
        return new TransportConnector(server);
    }

    private String loginUserName(Connector connector, boolean withCertificate) throws Exception {
        connectionContext.setConnector(connector);
        connectionInfo.setUserName(INSECURE_USERNAME);
        if (withCertificate) {
            connectionInfo.setTransportContext(new StubX509Certificate[] {});
        }
        authBroker.addConnection(connectionContext, connectionInfo);
        return receiveBroker.addConnectionData.getFirst().connectionContext.getSecurityContext().getUserName();
    }

    public void testSslMode() throws Exception {
        assertEquals(SslMode.NONE, sslConnector(false, false).getSslMode());
        assertEquals(SslMode.WANT_CLIENT_AUTH, sslConnector(false, true).getSslMode());
        assertEquals(SslMode.NEED_CLIENT_AUTH, sslConnector(true, false).getSslMode());
        assertEquals(SslMode.NEED_CLIENT_AUTH, sslConnector(true, true).getSslMode());
        assertEquals(SslMode.NONE, new TransportConnector(nonSslTransportServer).getSslMode());
        assertEquals(SslMode.NONE, new TransportConnector().getSslMode());
    }

    public void testSslModeFromTransportOption() throws Exception {
        SslTransportServer server = new SslTransportServer(null, new URI("ssl://localhost:61616"),
                new StubSSLSocketFactory(new StubSSLServerSocket()));
        HashMap<String, Object> transportOptions = new HashMap<String, Object>();
        transportOptions.put("wantClientAuth", "true");
        server.setTransportOption(transportOptions);
        assertEquals(SslMode.WANT_CLIENT_AUTH, server.getSslMode());

        transportOptions.put("needClientAuth", "true");
        assertEquals(SslMode.NEED_CLIENT_AUTH, server.getSslMode());
    }

    public void testWantClientAuthWithCertificate() throws Exception {
        assertEquals(DN_USERNAME, loginUserName(sslConnector(false, true), true));
    }

    public void testWantClientAuthWithoutCertificate() throws Exception {
        assertEquals(INSECURE_USERNAME, loginUserName(sslConnector(false, true), false));

        authBroker.removeConnection(connectionContext, connectionInfo, null);
        assertNull(connectionContext.getSecurityContext());
    }

    public void testWantClientAuthFallsBackWhenCertificateLoginFails() throws Exception {
        createLoginConfig(false, true);
        assertEquals(INSECURE_USERNAME, loginUserName(sslConnector(false, true), true));

        authBroker.removeConnection(connectionContext, connectionInfo, null);
        assertNull(connectionContext.getSecurityContext());

        // the login that succeeded has to forget the connection too
        java.lang.reflect.Field field = JaasDualAuthenticationBroker.class.getDeclaredField("nonSslBroker");
        field.setAccessible(true);
        assertTrue(((AbstractAuthenticationBroker) field.get(authBroker)).securityContexts.isEmpty());
    }

    public void testWantClientAuthRefusedWhenBothLoginsFail() throws Exception {
        createLoginConfig(false, false);
        try {
            loginUserName(sslConnector(false, true), true);
            fail("both logins refuse");
        } catch (SecurityException expected) {
            assertEquals("the certificate login failure is kept", 1, expected.getSuppressed().length);
        }
        assertNull(connectionContext.getSecurityContext());
    }

    public void testNeedClientAuthDoesNotFallBack() throws Exception {
        createLoginConfig(false, true);
        try {
            loginUserName(sslConnector(true, false), true);
            fail("a connector that needs a client certificate must not fall back to the user name and password");
        } catch (SecurityException expected) {
        }
    }

    public void testCertificateOnInsecureConnectorDoesNotFallBack() throws Exception {
        createLoginConfig(false, true);
        try {
            loginUserName(new TransportConnector(nonSslTransportServer), true);
            fail("only a connector with wantClientAuth falls back");
        } catch (SecurityException expected) {
        }
    }

    public void testNeedClientAuthWithoutCertificate() throws Exception {
        try {
            loginUserName(sslConnector(true, false), false);
            fail("a connector that needs a client certificate must not fall back to the user name and password");
        } catch (SecurityException expected) {
        }
    }

    public void testSslWithoutClientAuthUsesUserNameAndPassword() throws Exception {
        assertEquals(INSECURE_USERNAME, loginUserName(sslConnector(false, false), false));
    }

    public void testCertificateOnInsecureConnector() throws Exception {
        assertEquals(DN_USERNAME, loginUserName(new TransportConnector(nonSslTransportServer), true));
    }

    public void testSecureConnector() {
        Connector connector = new TransportConnector(sslTransportServer);
        connectionContext.setConnector(connector);
        connectionInfo.setTransportContext(new StubX509Certificate[] {});

        try {
            authBroker.addConnection(connectionContext, connectionInfo);
        } catch (Exception e) {
            fail("Call to addConnection failed: " + e.getMessage());
        }

        assertEquals("Number of addConnection calls to underlying Broker must match number of calls made to " +
                "AuthenticationBroker.", 1, receiveBroker.addConnectionData.size());

        ConnectionContext receivedContext = receiveBroker.addConnectionData.getFirst().connectionContext;

        assertEquals("The SecurityContext's userName must be set to that of the UserPrincipal.",
                DN_USERNAME, receivedContext.getSecurityContext().getUserName());

        Set<Principal> receivedPrincipals = receivedContext.getSecurityContext().getPrincipals();


        assertEquals("2 Principals received", 2, receivedPrincipals.size());

        for (Iterator<Principal> iter = receivedPrincipals.iterator(); iter.hasNext();) {
            Principal currentPrincipal = iter.next();

            if (currentPrincipal instanceof UserPrincipal) {
                assertEquals("UserPrincipal is '" + DN_USERNAME + "'", DN_USERNAME, currentPrincipal.getName());
            } else if (currentPrincipal instanceof GroupPrincipal) {
                assertEquals("GroupPrincipal is '" + DN_GROUP + "'", DN_GROUP, currentPrincipal.getName());
            } else {
                fail("Unexpected Principal subclass found.");
            }
        }

        try {
            authBroker.removeConnection(connectionContext, connectionInfo, null);
        } catch (Exception e) {
            fail("Call to removeConnection failed: " + e.getMessage());
        }
        assertEquals("Number of removeConnection calls to underlying Broker must match number of calls made to " +
                "AuthenticationBroker.", 1, receiveBroker.removeConnectionData.size());
    }

    public void testInsecureConnector() {
        Connector connector = new TransportConnector(nonSslTransportServer);
        connectionContext.setConnector(connector);
        connectionInfo.setUserName(INSECURE_USERNAME);

        try {
            authBroker.addConnection(connectionContext, connectionInfo);
        } catch (Exception e) {
            fail("Call to addConnection failed: " + e.getMessage());
        }

        assertEquals("Number of addConnection calls to underlying Broker must match number of calls made to " +
                "AuthenticationBroker.", 1, receiveBroker.addConnectionData.size());

        ConnectionContext receivedContext = receiveBroker.addConnectionData.getFirst().connectionContext;

        assertEquals("The SecurityContext's userName must be set to that of the UserPrincipal.",
                INSECURE_USERNAME, receivedContext.getSecurityContext().getUserName());

        Set<Principal> receivedPrincipals = receivedContext.getSecurityContext().getPrincipals();

        assertEquals("2 Principals received", 2, receivedPrincipals.size());
        for (Iterator<Principal> iter = receivedPrincipals.iterator(); iter.hasNext();) {
            Principal currentPrincipal = iter.next();

            if (currentPrincipal instanceof UserPrincipal) {
                assertEquals("UserPrincipal is '" + INSECURE_USERNAME + "'",
                        INSECURE_USERNAME, currentPrincipal.getName());
            } else if (currentPrincipal instanceof GroupPrincipal) {
                assertEquals("GroupPrincipal is '" + INSECURE_GROUP + "'",
                        INSECURE_GROUP, currentPrincipal.getName());
            } else {
                fail("Unexpected Principal subclass found.");
            }
        }

        try {
            authBroker.removeConnection(connectionContext, connectionInfo, null);
        } catch (Exception e) {
            fail("Call to removeConnection failed: " + e.getMessage());
        }
        assertEquals("Number of removeConnection calls to underlying Broker must match number of calls made to " +
                "AuthenticationBroker.", 1, receiveBroker.removeConnectionData.size());
    }
}
