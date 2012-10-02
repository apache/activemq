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

import junit.framework.TestCase;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.StubBroker;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;
import org.apache.activemq.transport.tcp.*;

import javax.net.ssl.SSLServerSocket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.net.URI;
import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        HashMap<String, String> sslConfigOptions = new HashMap<String, String>();
        HashMap<String, String> configOptions = new HashMap<String, String>();

        sslConfigOptions.put(StubLoginModule.ALLOW_LOGIN_PROPERTY, "true");
        sslConfigOptions.put(StubLoginModule.USERS_PROPERTY, DN_USERNAME);
        sslConfigOptions.put(StubLoginModule.GROUPS_PROPERTY, DN_GROUP);
        AppConfigurationEntry sslConfigEntry = new AppConfigurationEntry("org.apache.activemq.security.StubLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, sslConfigOptions);

        configOptions.put(StubLoginModule.ALLOW_LOGIN_PROPERTY, "true");
        configOptions.put(StubLoginModule.USERS_PROPERTY, INSECURE_USERNAME);
        configOptions.put(StubLoginModule.GROUPS_PROPERTY, INSECURE_GROUP);
        AppConfigurationEntry configEntry = new AppConfigurationEntry("org.apache.activemq.security.StubLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, configOptions);

        StubDualJaasConfiguration jaasConfig = new StubDualJaasConfiguration(configEntry, sslConfigEntry);

        Configuration.setConfiguration(jaasConfig);
    }

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

    protected void tearDown() throws Exception {
            super.tearDown();
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

        Set receivedPrincipals = receivedContext.getSecurityContext().getPrincipals();


        assertEquals("2 Principals received", 2, receivedPrincipals.size());
        
        for (Iterator iter = receivedPrincipals.iterator(); iter.hasNext();) {
            Principal currentPrincipal = (Principal)iter.next();

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

        Set receivedPrincipals = receivedContext.getSecurityContext().getPrincipals();

        assertEquals("2 Principals received", 2, receivedPrincipals.size());
        for (Iterator iter = receivedPrincipals.iterator(); iter.hasNext();) {
            Principal currentPrincipal = (Principal)iter.next();

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
