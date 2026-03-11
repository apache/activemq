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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class DuplexNetworkMBeanTest {

    protected static final Logger LOG = LoggerFactory.getLogger(DuplexNetworkMBeanTest.class);
    protected final int numRestarts = 3;

    private final MBeanServer mBeanServer = new ManagementContext().getMBeanServer();

    /**
     * Creates a primary broker with an ephemeral port transport connector.
     * Call {@code broker.start()} then use {@code getTransportConnectors().get(0).getConnectUri()}
     * to retrieve the actual assigned port.
     */
    private BrokerService createBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("broker");
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    /**
     * Creates a primary broker bound to a specific URI (for restart scenarios where
     * the networked broker expects a known port).
     */
    private BrokerService createBrokerOnPort(final URI bindURI) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("broker");
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector(bindURI.toString() + "?transport.reuseAddress=true");
        return broker;
    }

    private BrokerService createNetworkedBroker(final URI primaryBrokerURI) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setBrokerName("networkedBroker");
        broker.addConnector("tcp://localhost:0");
        broker.getManagementContext().setCreateConnector(false);
        final NetworkConnector networkConnector =
            broker.addNetworkConnector("static:(" + primaryBrokerURI + "?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        networkConnector.setDuplex(true);
        return broker;
    }

    @Test
    public void testMbeanPresenceOnNetworkBrokerRestart() throws Exception {
        final BrokerService broker = createBroker();
        try {
            broker.start();
            broker.waitUntilStarted();
            final URI primaryBrokerURI = broker.getTransportConnectors().get(0).getConnectUri();

            assertEquals(1, waitForMbeanCount(broker, "connector", 1, 30000));
            assertEquals(0, countMbeans(broker, "connectionName"));
            BrokerService networkedBroker = null;
            for (int i = 0; i < numRestarts; i++) {
                networkedBroker = createNetworkedBroker(primaryBrokerURI);
                try {
                    networkedBroker.start();
                    networkedBroker.waitUntilStarted();
                    assertEquals(1, waitForMbeanCount(networkedBroker, "networkBridge", 1, 2000));
                    assertEquals(1, waitForMbeanCount(broker, "networkBridge", 1, 2000));
                    assertEquals(2, waitForMbeanCount(broker, "connectionName", 2, 5000));
                } finally {
                    networkedBroker.stop();
                    networkedBroker.waitUntilStopped();
                }
                assertEquals(0, waitForMbeanCount(networkedBroker, "stopped", 0, 5000));
                assertEquals(0, waitForMbeanCount(broker, "networkBridge", 0, 5000));
            }

            assertEquals(0, waitForMbeanCount(networkedBroker, "networkBridge", 0, 5000));
            assertEquals(0, waitForMbeanCount(networkedBroker, "connector", 0, 5000));
            assertEquals(0, waitForMbeanCount(networkedBroker, "connectionName", 0, 5000));
            assertEquals(1, countMbeans(broker, "connector"));
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testMbeanPresenceOnBrokerRestart() throws Exception {
        // Start primary broker first to discover its ephemeral port
        BrokerService broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
        final URI primaryBrokerURI = broker.getTransportConnectors().get(0).getConnectUri();

        // Create and start networked broker pointing at the primary's actual port
        final BrokerService networkedBroker = createNetworkedBroker(primaryBrokerURI);
        try {
            networkedBroker.start();
            networkedBroker.waitUntilStarted();
            assertEquals(1, waitForMbeanCount(networkedBroker, "connector=networkConnectors", 1, 30000));
            assertEquals(0, countMbeans(networkedBroker, "connectionName"));

            // Stop the primary broker, then restart it multiple times on the same port
            broker.stop();
            broker.waitUntilStopped();

            for (int i = 0; i < numRestarts; i++) {
                broker = createBrokerOnPort(primaryBrokerURI);
                try {
                    broker.start();
                    broker.waitUntilStarted();
                    assertEquals(1, waitForMbeanCount(networkedBroker, "networkBridge", 1, 5000));
                    assertEquals("restart number: " + i, 2, waitForMbeanCount(broker, "connectionName", 2, 10000));
                } finally {
                    broker.stop();
                    broker.waitUntilStopped();
                }
                assertEquals(0, waitForMbeanCount(broker, "stopped", 0, 5000));
            }

            // After the final broker stop, the duplex bridge on the networked broker needs
            // time to detect the disconnection (via wireFormat.maxInactivityDuration=500ms)
            // and clean up. The connector MBean itself persists but bridge MBeans should go away.
            // The connector=networkConnectors query matches both the connector and any bridge
            // sub-objects, so we need to wait for the bridge cleanup.
            assertEquals(1, waitForMbeanCount(networkedBroker, "connector=networkConnectors", 1, 30000));
            assertEquals(0, waitForMbeanCount(networkedBroker, "connectionName", 0, 10000));
            assertEquals(0, waitForMbeanCount(broker, "connectionName", 0, 10000));
        } finally {
            networkedBroker.stop();
            networkedBroker.waitUntilStopped();
        }
    }

    @Test
    public void testMBeansNotOverwrittenOnCleanup() throws Exception {
        final BrokerService broker = createBroker();

        MessageProducer producerBroker = null;
        MessageConsumer consumerBroker = null;
        Session sessionNetworkBroker = null;
        Session sessionBroker = null;
        MessageProducer producerNetworkBroker = null;
        MessageConsumer consumerNetworkBroker = null;
        try {
            broker.start();
            broker.waitUntilStarted();
            final URI primaryBrokerURI = broker.getTransportConnectors().get(0).getConnectUri();

            final BrokerService networkedBroker = createNetworkedBroker(primaryBrokerURI);
            networkedBroker.start();
            networkedBroker.waitUntilStarted();
            try {
                assertEquals(2, waitForMbeanCount(networkedBroker, "connector=networkConnectors", 2, 10000));
                assertEquals(1, waitForMbeanCount(broker, "connector=duplexNetworkConnectors", 1, 10000));

                final Connection brokerConnection = new ActiveMQConnectionFactory(broker.getVmConnectorURI()).createConnection();
                brokerConnection.start();

                sessionBroker = brokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producerBroker = sessionBroker.createProducer(sessionBroker.createTopic("testTopic"));
                consumerBroker = sessionBroker.createConsumer(sessionBroker.createTopic("testTopic"));
                final Connection netWorkBrokerConnection = new ActiveMQConnectionFactory(networkedBroker.getVmConnectorURI()).createConnection();
                netWorkBrokerConnection.start();
                sessionNetworkBroker = netWorkBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producerNetworkBroker = sessionNetworkBroker.createProducer(sessionBroker.createTopic("testTopic"));
                consumerNetworkBroker = sessionNetworkBroker.createConsumer(sessionBroker.createTopic("testTopic"));

                assertEquals(4, waitForMbeanCount(broker, "destinationType=Topic,destinationName=testTopic", 4, 15000));
                assertEquals(4, waitForMbeanCount(networkedBroker, "destinationType=Topic,destinationName=testTopic", 4, 15000));

                producerBroker.send(sessionBroker.createTextMessage("test1"));
                producerNetworkBroker.send(sessionNetworkBroker.createTextMessage("test2"));

                assertEquals(2, waitForMbeanCount(networkedBroker, "destinationName=testTopic,direction=*", 2, 10000));
                assertEquals(2, waitForMbeanCount(broker, "destinationName=testTopic,direction=*", 2, 10000));
            } finally {
                if (producerBroker != null) {
                    producerBroker.close();
                }
                if (consumerBroker != null) {
                    consumerBroker.close();
                }
                if (sessionBroker != null) {
                    sessionBroker.close();
                }
                if (sessionNetworkBroker != null) {
                    sessionNetworkBroker.close();
                }
                if (producerNetworkBroker != null) {
                    producerNetworkBroker.close();
                }
                if (consumerNetworkBroker != null) {
                    consumerNetworkBroker.close();
                }
                networkedBroker.stop();
                networkedBroker.waitUntilStopped();
            }
            assertEquals(0, waitForMbeanCount(broker, "destinationName=testTopic,direction=*", 0, 1500));
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private int countMbeans(final BrokerService broker, String type) throws Exception {
        if (!type.contains("=")) {
            type = type + "=*";
        }

        final ObjectName beanName = new ObjectName("org.apache.activemq:type=Broker,brokerName="
                + broker.getBrokerName() + "," + type + ",*");

        LOG.info("Query name: " + beanName);
        final Set<ObjectName> mbeans = mBeanServer.queryNames(beanName, null);
        return mbeans != null ? mbeans.size() : 0;
    }

    /**
     * Waits up to {@code timeout} ms for the MBean count matching {@code type} to reach
     * the {@code expectedCount}. Returns the actual count found.
     */
    private int waitForMbeanCount(final BrokerService broker, String type, final int expectedCount, final int timeout) throws Exception {
        if (!type.contains("=")) {
            type = type + "=*";
        }

        final ObjectName beanName = new ObjectName("org.apache.activemq:type=Broker,brokerName="
                + broker.getBrokerName() + "," + type + ",*");

        final int[] resultHolder = new int[1];
        final boolean found = Wait.waitFor(() -> {
            LOG.info("Query name: " + beanName);
            final Set<ObjectName> mbeans = mBeanServer.queryNames(beanName, null);
            final int count = mbeans != null ? mbeans.size() : 0;
            resultHolder[0] = count;
            return count == expectedCount;
        }, timeout, 100);

        if (!found) {
            // If port 1099 is in use when the Broker starts, starting the jmx connector
            // will fail.  So, if we have no mbsc to query, skip the test.
            final Set<ObjectName> mbeans = mBeanServer.queryNames(beanName, null);
            assumeNotNull(mbeans);
            return mbeans != null ? mbeans.size() : 0;
        }
        return resultHolder[0];
    }

    private void logAllMbeans(final BrokerService broker) throws MalformedURLException {
        try {
            // trace all existing MBeans
            final Set<?> all = mBeanServer.queryNames(null, null);
            LOG.info("Total MBean count=" + all.size());
            for (final Object o : all) {
                final ObjectInstance bean = (ObjectInstance) o;
                LOG.info(bean.getObjectName().toString());
            }
        } catch (Exception ignored) {
            LOG.warn("getMBeanServer ex: " + ignored);
        }
    }
}
