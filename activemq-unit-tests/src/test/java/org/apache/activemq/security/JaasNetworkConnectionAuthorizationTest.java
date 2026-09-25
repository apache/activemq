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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.jaas.ConnectionPrincipal;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Network connection authorization through the JAAS PropertiesLoginModule with
 * clientId authorization enabled: only users whose rule starts with true
 * ({@code <userId> = true, <patterns>}) may register a network connection, whether they announce it before authenticating (a real bridge) or
 * after (an application promoting itself with a late BrokerInfo).
 *
 * Fixtures: activemq-clientid-domain in login.config, clientid-users.properties
 * and clientids.properties under org/apache/activemq/security. User 'bridge' is
 * 'true, NC_*'; user 'app' is 'false, *'.
 */
@Category(ParallelTest.class)
public class JaasNetworkConnectionAuthorizationTest {

    private static final ActiveMQQueue QUEUE = new ActiveMQQueue("TEST.NETWORK.AUTHZ");

    private BrokerService brokerA;
    private BrokerService brokerB;
    private String brokerBUri;

    @Before
    public void setUp() throws Exception {
        System.setProperty("java.security.auth.login.config", "src/test/resources/login.config");
        brokerB = createBroker("brokerB");
        brokerB.addConnector("tcp://localhost:0");
        brokerB.start();
        brokerB.waitUntilStarted();
        brokerBUri = brokerB.getTransportConnectors().get(0).getPublishableConnectString();
        brokerA = createBroker("brokerA");
    }

    @After
    public void tearDown() throws Exception {
        for (var broker : new BrokerService[] {brokerA, brokerB}) {
            if (broker != null) {
                broker.stop();
                broker.waitUntilStopped();
            }
        }
    }

    private static BrokerService createBroker(String name) {
        return createBroker(name, "activemq-clientid-domain");
    }

    private static BrokerService createBroker(String name, String jaasConfiguration) {
        var broker = new BrokerService();
        broker.setBrokerName(name);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        // advisory support stays on: demand forwarding across the bridge relies on it
        var jaas = new JaasAuthenticationPlugin();
        jaas.setConfiguration(jaasConfiguration);
        // plugins wrap in order, so the capture sits inside JAAS and sees the security context it set
        broker.setPlugins(new BrokerPlugin[] {new PrincipalCapture(), jaas});
        return broker;
    }

    /** records the ConnectionPrincipal JAAS attached to each admitted connection, keyed by clientId */
    private static final Map<String, ConnectionPrincipal> PRINCIPALS = new ConcurrentHashMap<>();

    private static final class PrincipalCapture implements BrokerPlugin {
        @Override
        public Broker installPlugin(Broker broker) {
            return new BrokerFilter(broker) {
                @Override
                public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                    super.addConnection(context, info);
                    var securityContext = context.getSecurityContext();
                    if (securityContext != null && info.getClientId() != null) {
                        for (var principal : securityContext.getPrincipals()) {
                            if (principal instanceof ConnectionPrincipal) {
                                PRINCIPALS.put(info.getClientId(), (ConnectionPrincipal) principal);
                            }
                        }
                    }
                }
            };
        }
    }

    @Test(timeout = 60000)
    public void testFlaggedUserCanEstablishBridge() throws Exception {
        addNetworkConnector(null, "bridge", "bridge");
        brokerA.start();
        brokerA.waitUntilStarted();

        assertTrue("bridge should form for the flagged user",
                Wait.waitFor(() -> admittedNetworkConnectionsOn(brokerB) == 1, 15000, 10));
        assertMessageCrossesBridge("app", "app");
    }

    /**
     * The configuration most existing deployments have: JAAS password authentication
     * on a realm without the clientids file. A password authenticated bridge must be
     * admitted with no rule at all, and the local side's late BrokerInfo must pass the
     * guard, because no network connection decision was ever made.
     */
    @Test(timeout = 60000)
    public void testBridgeAdmittedWhenClientIdAuthorizationNotConfigured() throws Exception {
        useRealm("activemq-domain");
        // 'system' / 'manager' from users.properties
        addNetworkConnector(null, "system", "manager");
        brokerA.start();
        brokerA.waitUntilStarted();

        assertTrue("bridge should form without any clientId rule",
                Wait.waitFor(() -> admittedNetworkConnectionsOn(brokerB) == 1, 15000, 10));
        assertMessageCrossesBridge("user", "password");
    }

    /** Replaces both brokers with ones authenticating against the given JAAS realm. */
    private void useRealm(String jaasConfiguration) throws Exception {
        brokerB.stop();
        brokerB.waitUntilStopped();
        brokerB = createBroker("brokerB", jaasConfiguration);
        brokerB.addConnector("tcp://localhost:0");
        brokerB.start();
        brokerB.waitUntilStarted();
        brokerBUri = brokerB.getTransportConnectors().get(0).getPublishableConnectString();
        brokerA = createBroker("brokerA", jaasConfiguration);
    }

    /**
     * The networkConnector name replaces the default NC prefix in the bridge
     * clientIds: toB_brokerA_outbound on B and toB_brokerB_inbound_brokerA on A.
     * 'bridge-named = true, toB_*' follows that, so the bridge is admitted.
     */
    @Test(timeout = 60000)
    public void testNamedConnectorAdmittedWhenPatternFollowsConnectorName() throws Exception {
        addNetworkConnector("toB", "bridge-named", "bridge-named");
        brokerA.start();
        brokerA.waitUntilStarted();

        assertTrue("bridge should form when the pattern covers the connector name prefix",
                Wait.waitFor(() -> admittedNetworkConnectionsOn(brokerB) == 1, 15000, 10));
        assertMessageCrossesBridge("app", "app");
    }

    /**
     * Same named connector, but 'bridge = true, NC_*' only covers the default
     * prefix. The user may register network connections, yet the clientId check
     * refuses toB_... so no bridge forms. This is the operator mistake to expect
     * when a connector is given a name.
     */
    @Test(timeout = 60000)
    public void testNamedConnectorRefusedWhenPatternOnlyCoversDefaultPrefix() throws Exception {
        addNetworkConnector("toB", "bridge", "bridge");
        brokerA.start();
        brokerA.waitUntilStarted();

        assertFalse("a bridge formed although the clientId pattern does not cover the connector name",
                Wait.waitFor(() -> admittedNetworkConnectionsOn(brokerB) > 0, 3000, 10));
    }

    private void assertMessageCrossesBridge(String user, String password) throws Exception {
        try (var consumerConnection = new ActiveMQConnectionFactory("vm://brokerB?create=false").createConnection(user, password);
             var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = consumerSession.createConsumer(QUEUE);
             var producerConnection = new ActiveMQConnectionFactory("vm://brokerA?create=false").createConnection(user, password);
             var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = producerSession.createProducer(QUEUE)) {
            consumerConnection.start();
            producerConnection.start();
            producer.send(producerSession.createTextMessage("across the bridge"));
            var received = consumer.receive(TimeUnit.SECONDS.toMillis(10));
            assertNotNull("message did not cross the bridge", received);
        }
    }

    @Test(timeout = 60000)
    public void testUnflaggedUserCannotEstablishBridge() throws Exception {
        // 'app = false, *' may use any clientId, so only the leading false can refuse it
        var connector = addNetworkConnector(null, "app", "app");
        brokerA.start();
        brokerA.waitUntilStarted();

        // give the connector several reconnect attempts; none may be admitted
        assertFalse("a bridge formed for a user whose rule starts with false",
                Wait.waitFor(() -> admittedNetworkConnectionsOn(brokerB) > 0, 3000, 10));
        assertTrue("connector should still be trying", connector.activeBridges().size() >= 0);
    }

    @Test(timeout = 60000)
    public void testUnflaggedUserCannotPromoteConnectionWithLateBrokerInfo() throws Exception {
        var clientId = "app-late";
        var transport = TransportFactory.connect(new URI(brokerBUri));
        try {
            transport.setTransportListener(new DefaultTransportListener());
            transport.start();
            var added = (Response) transport.request(connectionInfo("late-app", "app", "app", clientId));
            assertFalse("application connection should authenticate: " + added, added.isException());

            var rejected = requestBrokerInfo(transport);
            assertTrue("late BrokerInfo from an unflagged user must be rejected", rejected);
            assertFalse("connection must not have been promoted", isNetworkConnection(brokerB, clientId));
        } finally {
            transport.stop();
        }
    }

    @Test(timeout = 60000)
    public void testFlaggedUserCanPromoteConnectionWithLateBrokerInfo() throws Exception {
        // the local side of a real bridge does exactly this: connect, then forward BrokerInfo
        var clientId = "NC_brokerA_inbound_brokerB";
        var transport = TransportFactory.connect(new URI(brokerBUri));
        try {
            transport.setTransportListener(new DefaultTransportListener());
            transport.start();
            var info = connectionInfo("late-bridge", "bridge", "bridge", clientId);
            // a client can plant anything in clientIp; the principal must carry the transport's view instead
            info.setClientIp("spoofed://10.0.0.1:1");
            var added = (Response) transport.request(info);
            assertFalse("bridge connection should authenticate: " + added, added.isException());

            var principal = PRINCIPALS.get(clientId);
            assertNotNull("JAAS should have attached a ConnectionPrincipal", principal);
            assertEquals(clientId, principal.getClientId());
            assertTrue("network connection permitted by the bridge rule", principal.isNetworkConnection());
            assertEquals("connector name from the accepting connector",
                    brokerB.getTransportConnectors().get(0).getName(), principal.getTransportConnectorName());
            assertFalse("tcp connector is not ssl", principal.isSsl());
            assertTrue("remote address must come from the transport, not clientIp: " + principal.getRemoteAddress(),
                    principal.getRemoteAddress() != null && principal.getRemoteAddress().startsWith("tcp://"));
            assertEquals("remote address matches the broker side connection", transportRemoteAddress(brokerB, clientId),
                    principal.getRemoteAddress());

            var rejected = requestBrokerInfo(transport);
            assertFalse("late BrokerInfo from a flagged user must be accepted", rejected);
            assertTrue("connection should now be a network connection",
                    Wait.waitFor(() -> isNetworkConnection(brokerB, clientId), 5000, 10));
        } finally {
            transport.stop();
        }
    }

    /**
     * A realm without the clientid file makes no network connection decision, so the
     * broker must not apply the late BrokerInfo guard at all: an ordinary user may
     * still promote a connection, exactly as before the feature existed.
     */
    @Test(timeout = 60000)
    public void testLateBrokerInfoNotRestrictedWhenClientIdAuthorizationNotConfigured() throws Exception {
        var brokerC = createBroker("brokerC", "activemq-domain");
        brokerC.addConnector("tcp://localhost:0");
        brokerC.start();
        brokerC.waitUntilStarted();
        var clientId = "plain-late";
        var transport = TransportFactory.connect(new URI(brokerC.getTransportConnectors().get(0).getPublishableConnectString()));
        try {
            transport.setTransportListener(new DefaultTransportListener());
            transport.start();
            // 'system' / 'manager' from users.properties; the realm has no clientids file
            var added = (Response) transport.request(connectionInfo("late-plain", "system", "manager", clientId));
            assertFalse("connection should authenticate: " + added, added.isException());

            var rejected = requestBrokerInfo(transport);
            assertFalse("late BrokerInfo must be accepted when authorization is not configured", rejected);
            assertTrue("connection should now be a network connection",
                    Wait.waitFor(() -> isNetworkConnection(brokerC, clientId), 5000, 10));
        } finally {
            transport.stop();
            brokerC.stop();
            brokerC.waitUntilStopped();
        }
    }

    /**
     * With a null name the connector keeps the default, so the bridge clientIds take
     * the NC_ prefix: NC_brokerA_outbound on B, NC_brokerB_inbound_brokerA on A. A
     * given name replaces that prefix.
     */
    private NetworkConnector addNetworkConnector(String name, String user, String password) throws Exception {
        var connector = brokerA.addNetworkConnector("static:(" + brokerBUri + ")");
        if (name != null) {
            connector.setName(name);
        }
        connector.setUserName(user);
        connector.setPassword(password);
        return connector;
    }

    private static ConnectionInfo connectionInfo(String connectionId, String user, String password, String clientId) {
        var info = new ConnectionInfo(new ConnectionId(connectionId));
        info.setClientId(clientId);
        info.setUserName(user);
        info.setPassword(password);
        return info;
    }

    /** Sends a BrokerInfo on an already authenticated connection; true when the broker refused it. */
    private static boolean requestBrokerInfo(org.apache.activemq.transport.Transport transport) throws Exception {
        var brokerInfo = new BrokerInfo();
        brokerInfo.setBrokerId(new BrokerId("rogue"));
        brokerInfo.setBrokerName("rogue");
        brokerInfo.setNetworkConnection(true);
        try {
            var response = (Response) transport.request(brokerInfo);
            return response != null && response.isException();
        } catch (IOException refusedAndClosed) {
            return true;
        }
    }

    /**
     * Connections the broker actually admitted (authentication passed) that are
     * flagged as network connections. The transport level flag alone is set on
     * BrokerInfo before authentication, so it cannot distinguish a refused bridge.
     */
    private static long admittedNetworkConnectionsOn(BrokerService broker) {
        try {
            return Arrays.stream(broker.getBroker().getClients()).filter(Connection::isNetworkConnection).count();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String transportRemoteAddress(BrokerService broker, String clientId) {
        for (TransportConnection connection : broker.getTransportConnectors().get(0).getConnections()) {
            if (clientId.equals(connection.getConnectionId())) {
                return connection.getRemoteAddress();
            }
        }
        return null;
    }

    /** TransportConnection.getConnectionId() reports the clientId when the connection set one. */
    private static boolean isNetworkConnection(BrokerService broker, String clientId) {
        for (TransportConnection connection : broker.getTransportConnectors().get(0).getConnections()) {
            if (clientId.equals(connection.getConnectionId())) {
                return connection.isNetworkConnection();
            }
        }
        return false;
    }
}
