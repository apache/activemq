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
package org.apache.activemq.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Remote address allow/deny lists on a transport connector: configuration reaches
 * the bound server through the managed connector, sockets are admitted or refused,
 * and the counters and list metrics report it.
 */
@Category(ParallelTest.class)
public class TransportConnectorCidrTest {

    private static final String LOOPBACK = "127.0.0.1/32";

    private BrokerService broker;

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /** JMX on so the connector that runs is the managed copy, which must carry the settings over. */
    private TransportConnector startBroker(String allowList, String denyList, boolean enabled) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        var connector = new TransportConnector();
        connector.setName("cidr");
        connector.setUri(new URI("tcp://127.0.0.1:0"));
        connector.setAllowList(allowList);
        connector.setDenyList(denyList);
        connector.setAllowDenyValidationEnabled(enabled);
        broker.addConnector(connector);
        broker.start();
        broker.waitUntilStarted();
        return broker.getTransportConnectors().get(0);
    }

    private static String clientUri(TransportConnector connector) throws Exception {
        return "tcp://127.0.0.1:" + connector.getServer().getSocketAddress().getPort();
    }

    private static void connectAndClose(TransportConnector connector) throws Exception {
        try (var connection = new ActiveMQConnectionFactory(clientUri(connector)).createConnection()) {
            connection.start();
        }
    }

    private static void assertRefused(TransportConnector connector) throws Exception {
        assertThrows(JMSException.class, () -> connectAndClose(connector));
    }

    @Test(timeout = 60000)
    public void testAllowedAddressConnectsAndIsCounted() throws Exception {
        var connector = startBroker(LOOPBACK, null, true);
        connectAndClose(connector);
        assertTrue(Wait.waitFor(() -> connector.getAllowedCount() == 1, 5000, 10));
        assertEquals(0, connector.getDeniedCount());
    }

    @Test(timeout = 60000)
    public void testDeniedAddressIsRefusedAndCounted() throws Exception {
        var connector = startBroker(null, LOOPBACK, true);
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> connector.getDeniedCount() == 1, 5000, 10));
        assertEquals(0, connector.getAllowedCount());
        assertEquals("refusal must not count as a connection", 0, connector.getConnections().size());
    }

    @Test(timeout = 60000)
    public void testAddressOutsideAllowListIsRefused() throws Exception {
        var connector = startBroker("10.0.0.0/8", null, true);
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> connector.getDeniedCount() == 1, 5000, 10));
    }

    /** Validation is off unless enabled, so lists alone change nothing. The default may move in a future major release. */
    @Test(timeout = 60000)
    public void testValidationIsDisabledByDefault() throws Exception {
        assertFalse(new TransportConnector().isAllowDenyValidationEnabled());

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        var configured = new TransportConnector();
        configured.setName("cidr");
        configured.setUri(new URI("tcp://127.0.0.1:0"));
        configured.setDenyList(LOOPBACK);
        broker.addConnector(configured);
        broker.start();
        broker.waitUntilStarted();
        var connector = broker.getTransportConnectors().get(0);

        assertFalse(connector.isAllowDenyValidationEnabled());
        assertEquals("list is still loaded", 1, connector.getDenyListCount());
        connectAndClose(connector);
        assertEquals(0, connector.getDeniedCount());
        assertEquals(0, connector.getAllowedCount());
    }

    @Test(timeout = 60000)
    public void testDisabledValidationKeepsListsButDoesNotCheck() throws Exception {
        var connector = startBroker(null, LOOPBACK, false);
        connectAndClose(connector);
        assertEquals(LOOPBACK, connector.getDenyList());
        assertEquals(1, connector.getDenyListCount());
        assertEquals(0, connector.getAllowedCount());
        assertEquals(0, connector.getDeniedCount());
    }

    @Test(timeout = 60000)
    public void testValidationCanBeToggledAtRuntimeThroughJmxView() throws Exception {
        var connector = startBroker(null, LOOPBACK, false);
        var view = new ConnectorView(connector);
        assertFalse(view.isAllowDenyValidationEnabled());
        connectAndClose(connector);

        view.setAllowDenyValidationEnabled(true);
        assertTrue(connector.isAllowDenyValidationEnabled());
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> view.getDeniedCount() == 1, 5000, 10));

        view.setAllowDenyValidationEnabled(false);
        connectAndClose(connector);
        assertEquals(1, view.getDeniedCount());
    }

    @Test(timeout = 60000)
    public void testDenyListLoadedFromFile() throws Exception {
        var dir = Files.createDirectories(Path.of("target", "transport-connector-cidr-test"));
        var file = Files.writeString(dir.resolve("deny.txt"), "# refuse loopback\n" + LOOPBACK + "\n", StandardCharsets.UTF_8);
        var connector = startBroker(null, "file:" + file.toAbsolutePath(), true);
        assertEquals(1, connector.getDenyListCount());
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> connector.getDeniedCount() == 1, 5000, 10));
    }

    @Test(timeout = 60000)
    public void testListMetricsReportValidAndInvalidEntries() throws Exception {
        var connector = startBroker("10.0.0.0/8,not-a-cidr," + LOOPBACK, null, true);
        var view = new ConnectorView(connector);
        assertEquals(2, view.getAllowListCount());
        assertEquals(1, view.getAllowListInvalidCount());
        assertEquals(0, view.getDenyListCount());
        assertEquals(0, view.getDenyListInvalidCount());
        assertEquals("10.0.0.0/8,not-a-cidr," + LOOPBACK, view.getAllowList());
        // the two valid entries still work
        connectAndClose(connector);
        assertTrue(Wait.waitFor(() -> view.getAllowedCount() == 1, 5000, 10));
    }

    /** allowed() runs the full decision even while enforcement is off, so lists can be checked first. */
    @Test(timeout = 60000)
    public void testAllowedOperationThroughJmxView() throws Exception {
        // class B allowed, core router and DNS server carved out
        var connector = startBroker("10.20.0.0/16," + LOOPBACK, "10.20.0.1/32,10.20.0.53/32", false);
        var view = new ConnectorView(connector);
        assertTrue(view.allowed("127.0.0.1"));
        assertTrue(view.allowed("10.20.7.8"));
        assertTrue("the network as a whole is allowed despite the carve outs", view.allowed("10.20.0.0/16"));
        assertFalse("deny entry wins over the allow entry", view.allowed("10.20.0.53"));
        assertFalse("a block entirely inside a deny entry", view.allowed("10.20.0.1/32"));
        assertFalse("outside the allow list", view.allowed("192.168.1.1"));
        assertThrows(IllegalArgumentException.class, () -> view.allowed("not-an-address"));
    }

    @Test(timeout = 60000)
    public void testResetStatisticsClearsConnectionCountersNotListMetrics() throws Exception {
        var connector = startBroker(LOOPBACK, null, true);
        connectAndClose(connector);
        assertTrue(Wait.waitFor(() -> connector.getAllowedCount() == 1, 5000, 10));
        connector.resetStatistics();
        assertEquals(0, connector.getAllowedCount());
        assertEquals(1, connector.getAllowListCount());
    }
}
