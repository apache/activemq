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
package org.apache.activemq.transport.tcp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;

import javax.management.JMX;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A connector with a key exchange setting the running JDK cannot honour must
 * refuse to start with a message naming the JDK needed, on the blocking and the
 * NIO ssl transports alike; on a JDK that can, it starts.
 */
@RunWith(Parameterized.class)
public class SslNamedGroupOptionsTest {

    private static final int JAVA = Runtime.version().feature();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> transports() {
        return Arrays.asList(new Object[][] {{"ssl"}, {"nio+ssl"}});
    }

    @Parameterized.Parameter
    public String transport;

    private BrokerService broker;

    @Before
    public void setUp() {
        System.setProperty("javax.net.ssl.keyStore", SslTransportBrokerTest.SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", SslTransportBrokerTest.PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", SslTransportBrokerTest.KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.trustStore", SslTransportBrokerTest.TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", SslTransportBrokerTest.PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", SslTransportBrokerTest.KEYSTORE_TYPE);
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void requirePostQuantumKeyExchangeNeedsJava27() throws Exception {
        String uri = transport + "://localhost:0?transport.requirePostQuantumKeyExchange=true";
        if (JAVA >= 27) {
            assertNotNull(broker.addConnector(uri));
            broker.start();
            assertTrue(broker.isStarted());
        } else {
            assertRefused(uri, "Java 27");
        }
    }

    @Test
    public void namedGroupsNeedJava21() throws Exception {
        String uri = transport + "://localhost:0?transport.namedGroups=x25519,secp256r1";
        if (JAVA >= 21) {
            assertNotNull(broker.addConnector(uri));
            broker.start();
            assertTrue(broker.isStarted());
        } else {
            assertRefused(uri, "Java 21");
        }
    }

    @Test
    public void jmxShowsTheConfiguredValues() throws Exception {
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        TransportConnector plain = broker.addConnector(transport + "://localhost:0");
        TransportConnector configured = JAVA >= 21
                ? broker.addConnector(transport + "://localhost:0?transport.namedGroups=x25519,secp256r1&transport.signatureSchemes=ed25519,rsa_pss_rsae_sha256")
                : null;
        TransportConnector postQuantum = JAVA >= 27
                ? broker.addConnector(transport + "://localhost:0?transport.requirePostQuantumKeyExchange=true")
                : null;
        broker.start();

        ConnectorViewMBean plainView = view(plain);
        assertNull(plainView.getNamedGroups());
        assertNull(plainView.getSignatureSchemes());
        assertFalse(plainView.isRequirePostQuantumKeyExchange());
        if (configured != null) {
            ConnectorViewMBean view = view(configured);
            assertArrayEquals(new String[] {"x25519", "secp256r1"}, view.getNamedGroups());
            assertArrayEquals(new String[] {"ed25519", "rsa_pss_rsae_sha256"}, view.getSignatureSchemes());
            assertFalse(view.isRequirePostQuantumKeyExchange());
        }
        if (postQuantum != null) {
            ConnectorViewMBean view = view(postQuantum);
            assertTrue(view.isRequirePostQuantumKeyExchange());
            assertNull("the switch, not an explicit list", view.getNamedGroups());
        }
    }

    private ConnectorViewMBean view(TransportConnector connector) throws Exception {
        ObjectName name = BrokerMBeanSupport.createConnectorName(broker.getBrokerObjectName().toString(), "clientConnectors", connector.getName());
        return JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), name, ConnectorViewMBean.class);
    }

    @Test
    public void requireAndNamedGroupsTogetherAreRefused() {
        assertRefused(transport + "://localhost:0?transport.requirePostQuantumKeyExchange=true&transport.namedGroups=x25519", "cannot both be set");
    }

    private void assertRefused(String uri, String expectedText) {
        Exception thrown = assertThrows(Exception.class, () -> broker.addConnector(uri));
        String messages = "";
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            messages += t.getMessage() + " | ";
        }
        assertTrue(messages, messages.contains(expectedText));
    }
}
