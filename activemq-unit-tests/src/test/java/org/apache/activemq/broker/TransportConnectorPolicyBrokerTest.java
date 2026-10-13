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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.management.Attribute;
import javax.management.JMX;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;

import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.RemoteAddressConnectorPolicyMBean;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A transport server policy on a transport connector: the managed connector
 * carries the policy to the bound server, sockets are admitted or refused, and
 * the policy's own MBean reports and controls it.
 */
@Category(ParallelTest.class)
public class TransportConnectorPolicyBrokerTest {

    private static final String LOOPBACK = "127.0.0.1/32";
    private static final String CONNECTOR_NAME = "policy";

    private BrokerService broker;

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /** JMX on so the connector that runs is the managed copy, which must carry the policy over. */
    private TransportConnector startBroker(RemoteAddressConnectorPolicy policy) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        var connector = new TransportConnector();
        connector.setName(CONNECTOR_NAME);
        connector.setUri(new URI("tcp://127.0.0.1:0"));
        connector.setTransportConnectorPolicy(policy);
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

    private ObjectName connectorObjectName() throws Exception {
        var brokerName = BrokerMBeanSupport.createBrokerObjectName(
                broker.getManagementContext().getJmxDomainName(), broker.getBrokerName());
        return BrokerMBeanSupport.createConnectorName(brokerName, "clientConnectors", CONNECTOR_NAME);
    }

    private ObjectName policyObjectName(String policyName) throws Exception {
        return BrokerMBeanSupport.createTransportConnectorPolicyName(connectorObjectName(), policyName);
    }

    private Object invokeAllowed(ObjectName objectName, String addressOrCidr) throws Exception {
        return broker.getManagementContext().getMBeanServer()
                .invoke(objectName, "allowed", new Object[] { addressOrCidr }, new String[] { String.class.getName() });
    }

    private RemoteAddressConnectorPolicyMBean policyMBean(String policyName) throws Exception {
        return JMX.newMBeanProxy(broker.getManagementContext().getMBeanServer(),
                policyObjectName(policyName), RemoteAddressConnectorPolicyMBean.class);
    }

    @Test(timeout = 60000)
    public void testAllowedAddressConnectsAndIsCounted() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList(LOOPBACK);
        var connector = startBroker(policy);
        connectAndClose(connector);
        assertTrue(Wait.waitFor(() -> policy.getAllowedCount() == 1, 5000, 10));
        assertEquals(0, policy.getDeniedCount());
    }

    @Test(timeout = 60000)
    public void testDeniedAddressIsRefusedAndCounted() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        var connector = startBroker(policy);
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> policy.getDeniedCount() == 1, 5000, 10));
        assertEquals(0, policy.getAllowedCount());
        assertEquals("refusal must not count as a connection", 0, connector.getConnections().size());
    }

    @Test(timeout = 60000)
    public void testAddressOutsideAllowListIsRefused() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.0.0.0/8");
        var connector = startBroker(policy);
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> policy.getDeniedCount() == 1, 5000, 10));
    }

    @Test(timeout = 60000)
    public void testManagedConnectorCarriesThePolicy() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        var connector = startBroker(policy);
        assertSame(policy, connector.getTransportConnectorPolicy());
    }

    @Test(timeout = 60000)
    public void testPolicyMBeanTogglesEnforcementAtRuntime() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        policy.setEnabled(false);
        var connector = startBroker(policy);
        connectAndClose(connector);

        var mbean = policyMBean("remoteAddress");
        assertFalse(mbean.isEnabled());
        assertEquals(LOOPBACK, mbean.getDenyList());
        assertEquals(1, mbean.getDenyListCount());

        mbean.setEnabled(true);
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> mbean.getDeniedCount() == 1, 5000, 10));

        mbean.setEnabled(false);
        connectAndClose(connector);
        assertEquals(1, mbean.getDeniedCount());
    }

    /** The dry run operation answers for an address or a whole block without connecting. */
    @Test(timeout = 60000)
    public void testPolicyMBeanDryRunAndListMetrics() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.20.0.0/16,not-a-cidr," + LOOPBACK);
        policy.setDenyList("10.20.0.53/32");
        startBroker(policy);

        var mbean = policyMBean("remoteAddress");
        assertEquals(2, mbean.getAllowListCount());
        assertEquals(1, mbean.getAllowListInvalidCount());
        assertEquals(1, mbean.getDenyListCount());
        assertTrue(mbean.allowed("127.0.0.1"));
        assertTrue(mbean.allowed("10.20.7.8"));
        assertTrue("the block as a whole is allowed", mbean.allowed("10.20.0.0/16"));
        assertFalse("deny entry wins", mbean.allowed("10.20.0.53"));
        assertFalse("outside the allow list", mbean.allowed("192.0.2.1"));
    }

    @Test(timeout = 60000)
    public void testFileDenyListAppliedThroughConnector() throws Exception {
        var dir = Files.createDirectories(Path.of("target", "transport-server-policy-test"));
        var file = Files.writeString(dir.resolve("deny.txt"), "# refuse loopback\n" + LOOPBACK + "\n", StandardCharsets.UTF_8);
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList("file:" + file.toAbsolutePath());
        var connector = startBroker(policy);
        assertEquals(1, policy.getDenyListCount());
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> policy.getDeniedCount() == 1, 5000, 10));
    }

    @Test(timeout = 60000)
    public void testPolicyMBeanResetStatistics() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList(LOOPBACK);
        var connector = startBroker(policy);
        connectAndClose(connector);
        var mbean = policyMBean("remoteAddress");
        assertTrue(Wait.waitFor(() -> mbean.getAllowedCount() == 1, 5000, 10));
        mbean.resetStatistics();
        assertEquals(0, mbean.getAllowedCount());
        assertEquals(1, mbean.getAllowListCount());
    }

    /** A connector without a policy registers its connector MBean and no policy MBean. */
    @Test(timeout = 60000)
    public void testNoPolicyMeansNoPolicyMBean() throws Exception {
        var connector = startBroker(null);
        var mbeanServer = broker.getManagementContext().getMBeanServer();
        assertTrue(mbeanServer.isRegistered(connectorObjectName()));
        var policyNames = mbeanServer.queryNames(new ObjectName(connectorObjectName() + ",transportConnectorPolicy=*"), null);
        assertTrue("no policy MBean expected, found " + policyNames, policyNames.isEmpty());
        connectAndClose(connector);
    }

    /** Every configured value is readable through plain JMX attribute access. */
    @Test(timeout = 60000)
    public void testJmxAttributesReportConfiguredData() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.20.0.0/16," + LOOPBACK);
        policy.setDenyList("10.20.0.53/32,not-a-cidr");
        startBroker(policy);

        var mbeanServer = broker.getManagementContext().getMBeanServer();
        var objectName = policyObjectName("remoteAddress");
        assertEquals(1, mbeanServer.queryNames(new ObjectName(connectorObjectName() + ",transportConnectorPolicy=*"), null).size());

        assertEquals("remoteAddress", mbeanServer.getAttribute(objectName, "Name"));
        assertEquals(Boolean.TRUE, mbeanServer.getAttribute(objectName, "Enabled"));
        assertEquals("10.20.0.0/16," + LOOPBACK, mbeanServer.getAttribute(objectName, "AllowList"));
        assertEquals("10.20.0.53/32,not-a-cidr", mbeanServer.getAttribute(objectName, "DenyList"));
        assertEquals(2L, mbeanServer.getAttribute(objectName, "AllowListCount"));
        assertEquals(1L, mbeanServer.getAttribute(objectName, "DenyListCount"));
        assertEquals(0L, mbeanServer.getAttribute(objectName, "AllowListInvalidCount"));
        assertEquals(1L, mbeanServer.getAttribute(objectName, "DenyListInvalidCount"));
        assertEquals(0L, mbeanServer.getAttribute(objectName, "AllowedCount"));
        assertEquals(0L, mbeanServer.getAttribute(objectName, "DeniedCount"));
    }

    /** The allowed operation invoked through the MBean server covers the decision matrix. */
    @Test(timeout = 60000)
    public void testJmxAllowedOperationPassAndFailScenarios() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.20.0.0/16," + LOOPBACK);
        policy.setDenyList("10.20.0.53/32");
        startBroker(policy);
        var objectName = policyObjectName("remoteAddress");

        assertEquals("in the allow list", Boolean.TRUE, invokeAllowed(objectName, "127.0.0.1"));
        assertEquals("inside an allowed block", Boolean.TRUE, invokeAllowed(objectName, "10.20.7.8"));
        assertEquals("the allowed block as a whole", Boolean.TRUE, invokeAllowed(objectName, "10.20.0.0/16"));
        assertEquals("not in the allow list", Boolean.FALSE, invokeAllowed(objectName, "192.0.2.1"));
        assertEquals("in the deny list", Boolean.FALSE, invokeAllowed(objectName, "10.20.0.53"));
        assertEquals("a block inside a deny entry", Boolean.FALSE, invokeAllowed(objectName, "10.20.0.53/32"));

        var invalid = assertThrows(RuntimeMBeanException.class, () -> invokeAllowed(objectName, "not-an-address"));
        assertTrue(invalid.getCause() instanceof IllegalArgumentException);
    }

    /** With only a deny list configured, anything not denied is allowed. */
    @Test(timeout = 60000)
    public void testJmxAllowedOperationWithDenyOnlyPolicy() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        startBroker(policy);
        var objectName = policyObjectName("remoteAddress");
        assertEquals("in the deny list", Boolean.FALSE, invokeAllowed(objectName, "127.0.0.1"));
        assertEquals("empty allow list admits anything not denied", Boolean.TRUE, invokeAllowed(objectName, "192.0.2.1"));
    }

    /** Enabled written as a JMX attribute turns live enforcement off and back on. */
    @Test(timeout = 60000)
    public void testJmxEnabledAttributeControlsLiveConnections() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        var connector = startBroker(policy);
        var mbeanServer = broker.getManagementContext().getMBeanServer();
        var objectName = policyObjectName("remoteAddress");

        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> policy.getDeniedCount() == 1, 5000, 10));

        mbeanServer.setAttribute(objectName, new Attribute("Enabled", Boolean.FALSE));
        assertFalse(policy.isEnabled());
        connectAndClose(connector);

        mbeanServer.setAttribute(objectName, new Attribute("Enabled", Boolean.TRUE));
        assertRefused(connector);
        assertTrue(Wait.waitFor(() -> (Long) mbeanServer.getAttribute(objectName, "DeniedCount") == 2L, 5000, 10));
    }

    /** A custom name flows into the policy's object name, and stop unregisters the MBean. */
    @Test(timeout = 60000)
    public void testNamedPolicyMBeanLifecycle() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setName("edge");
        policy.setDenyList(LOOPBACK);
        startBroker(policy);

        var mbeanServer = broker.getManagementContext().getMBeanServer();
        var objectName = policyObjectName("edge");
        assertTrue(mbeanServer.isRegistered(objectName));
        assertEquals("edge", policyMBean("edge").getName());

        broker.stop();
        broker.waitUntilStopped();
        assertFalse(mbeanServer.isRegistered(objectName));
    }
}
