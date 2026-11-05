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
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.UserPrincipal;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;

import jakarta.jms.Connection;

/**
 * Confirms how the connection MBeans report UserName when
 * populateUserNameInMBeans is enabled: a client-supplied user name is shown
 * as-is, and with useAuthenticatedPrincipalForUserNameInMBeans a connection
 * that did not supply one (for example SSL certificate or anonymous
 * authentication) shows the authenticated principal instead of a blank.
 */
public class ManagedConnectionUserNameTest {

    private BrokerService broker;
    private Connection connection;

    /**
     * Authenticates credential-less connections the way certificate-based
     * authenticators do: the SecurityContext user name holds the
     * certificate DN and the principals hold the mapped user name.
     */
    static class CertificateStyleAuthenticationPlugin implements BrokerPlugin {
        @Override
        public Broker installPlugin(Broker next) {
            return new BrokerFilter(next) {
                @Override
                public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                    if (info.getUserName() == null || info.getUserName().isEmpty()) {
                        context.setSecurityContext(new SecurityContext("CN=client, O=Example") {
                            @Override
                            public Set<Principal> getPrincipals() {
                                return Set.of(new UserPrincipal("ssl-client"));
                            }
                        });
                    }
                    super.addConnection(context, info);
                }
            };
        }
    }

    private void startBroker(BrokerPlugin plugin, boolean useAuthenticatedPrincipal) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setPopulateUserNameInMBeans(true);
        broker.setUseAuthenticatedPrincipalForUserNameInMBeans(useAuthenticatedPrincipal);
        broker.setPlugins(new BrokerPlugin[] { plugin });
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    private String connectAndReadUserName(String userName, String password) throws Exception {
        var brokerUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
        var factory = new ActiveMQConnectionFactory(brokerUri);
        connection = userName != null
                ? factory.createConnection(userName, password)
                : factory.createConnection();
        connection.start();

        var managementContext = broker.getManagementContext();
        var pattern = new ObjectName(managementContext.getJmxDomainName() + ":type=Broker,brokerName="
                + broker.getBrokerName()
                + ",connector=clientConnectors,connectorName=*,connectionViewType=clientId,connectionName=*");
        assertTrue("connection MBean should be registered",
                Wait.waitFor(() -> !managementContext.queryNames(pattern, null).isEmpty(), 5_000L, 50L));

        var name = managementContext.queryNames(pattern, null).iterator().next();
        return (String) managementContext.getMBeanServer().getAttribute(name, "UserName");
    }

    @Test
    public void testPrincipalFillsMissingUserName() throws Exception {
        startBroker(new CertificateStyleAuthenticationPlugin(), true);
        assertEquals("ssl-client", connectAndReadUserName(null, null));
    }

    @Test
    public void testMissingUserNameStaysBlankByDefault() throws Exception {
        startBroker(new CertificateStyleAuthenticationPlugin(), false);
        assertNull(connectAndReadUserName(null, null));
    }

    @Test
    public void testClientSuppliedUserNameIsKept() throws Exception {
        startBroker(new CertificateStyleAuthenticationPlugin(), true);
        assertEquals("client-app", connectAndReadUserName("client-app", "secret"));
    }

    @Test
    public void testSecurityContextUserNameUsedWhenNoUserPrincipal() throws Exception {
        var authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setAnonymousAccessAllowed(true);
        startBroker(authenticationPlugin, true);
        assertEquals("anonymous", connectAndReadUserName(null, null));
    }
}
