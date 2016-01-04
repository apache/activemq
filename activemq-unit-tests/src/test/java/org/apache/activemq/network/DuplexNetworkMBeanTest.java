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
import org.apache.activemq.util.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class DuplexNetworkMBeanTest {

    protected static final Logger LOG = LoggerFactory.getLogger(DuplexNetworkMBeanTest.class);
    protected final int numRestarts = 3;

    private int primaryBrokerPort;
    private int secondaryBrokerPort;
    private MBeanServer mBeanServer = new ManagementContext().getMBeanServer();

    @Before
    public void setUp() throws Exception {
        List<Integer> ports = TestUtils.findOpenPorts(2);

        primaryBrokerPort = ports.get(0);
        secondaryBrokerPort = ports.get(1);
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker");
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector("tcp://localhost:" + primaryBrokerPort + "?transport.reuseAddress=true");

        return broker;
    }

    protected BrokerService createNetworkedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("networkedBroker");
        broker.addConnector("tcp://localhost:" + secondaryBrokerPort + "?transport.reuseAddress=true");
        broker.getManagementContext().setCreateConnector(false);
        NetworkConnector networkConnector =
            broker.addNetworkConnector("static:(tcp://localhost:" + primaryBrokerPort + "?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        networkConnector.setDuplex(true);
        return broker;
    }

    @Test
    public void testMbeanPresenceOnNetworkBrokerRestart() throws Exception {
        BrokerService broker = createBroker();
        try {
            broker.start();
            assertEquals(1, countMbeans(broker, "connector", 30000));
            assertEquals(0, countMbeans(broker, "connectionName"));
            BrokerService networkedBroker = null;
            for (int i=0; i<numRestarts; i++) {
                networkedBroker = createNetworkedBroker();
                try {
                    networkedBroker.start();
                    assertEquals(1, countMbeans(networkedBroker, "networkBridge", 2000));
                    assertEquals(1, countMbeans(broker, "networkBridge", 2000));
                    assertEquals(2, countMbeans(broker, "connectionName"));
                } finally {
                    networkedBroker.stop();
                    networkedBroker.waitUntilStopped();
                }
                assertEquals(0, countMbeans(networkedBroker, "stopped"));
                assertEquals(0, countMbeans(broker, "networkBridge"));
            }

            assertEquals(0, countMbeans(networkedBroker, "networkBridge"));
            assertEquals(0, countMbeans(networkedBroker, "connector"));
            assertEquals(0, countMbeans(networkedBroker, "connectionName"));
            assertEquals(1, countMbeans(broker, "connector"));
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testMbeanPresenceOnBrokerRestart() throws Exception {

        BrokerService networkedBroker = createNetworkedBroker();
        try {
            networkedBroker.start();
            assertEquals(1, countMbeans(networkedBroker, "connector=networkConnectors", 30000));
            assertEquals(0, countMbeans(networkedBroker, "connectionName"));

            BrokerService broker = null;
            for (int i=0; i<numRestarts; i++) {
                broker = createBroker();
                try {
                    broker.start();
                    assertEquals(1, countMbeans(networkedBroker, "networkBridge", 5000));
                    assertEquals("restart number: " + i, 2, countMbeans(broker, "connectionName", 10000));
                } finally {
                    broker.stop();
                    broker.waitUntilStopped();
                }
                assertEquals(0, countMbeans(broker, "stopped"));
            }

            assertEquals(1, countMbeans(networkedBroker, "connector=networkConnectors"));
            assertEquals(0, countMbeans(networkedBroker, "connectionName"));
            assertEquals(0, countMbeans(broker, "connectionName"));
        } finally {
            networkedBroker.stop();
            networkedBroker.waitUntilStopped();
        }
    }

    @Test
    public void testMBeansNotOverwrittenOnCleanup() throws Exception {
        BrokerService broker = createBroker();

        BrokerService networkedBroker = createNetworkedBroker();
        MessageProducer producerBroker = null;
        MessageConsumer consumerBroker = null;
        Session sessionNetworkBroker = null;
        Session sessionBroker = null;
        MessageProducer producerNetworkBroker = null;
        MessageConsumer consumerNetworkBroker = null;
        try {
            broker.start();
            broker.waitUntilStarted();
            networkedBroker.start();
            try {
                assertEquals(2, countMbeans(networkedBroker, "connector=networkConnectors", 10000));
                assertEquals(1, countMbeans(broker, "connector=duplexNetworkConnectors", 10000));

                Connection brokerConnection = new ActiveMQConnectionFactory(broker.getVmConnectorURI()).createConnection();
                brokerConnection.start();

                sessionBroker = brokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producerBroker = sessionBroker.createProducer(sessionBroker.createTopic("testTopic"));
                consumerBroker = sessionBroker.createConsumer(sessionBroker.createTopic("testTopic"));
                Connection netWorkBrokerConnection = new ActiveMQConnectionFactory(networkedBroker.getVmConnectorURI()).createConnection();
                netWorkBrokerConnection.start();
                sessionNetworkBroker = netWorkBrokerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producerNetworkBroker = sessionNetworkBroker.createProducer(sessionBroker.createTopic("testTopic"));
                consumerNetworkBroker = sessionNetworkBroker.createConsumer(sessionBroker.createTopic("testTopic"));

                assertEquals(4, countMbeans(broker, "destinationType=Topic,destinationName=testTopic", 15000));
                assertEquals(4, countMbeans(networkedBroker, "destinationType=Topic,destinationName=testTopic", 15000));

                producerBroker.send(sessionBroker.createTextMessage("test1"));
                producerNetworkBroker.send(sessionNetworkBroker.createTextMessage("test2"));

                assertEquals(2, countMbeans(networkedBroker, "destinationName=testTopic,direction=*", 10000));
                assertEquals(2, countMbeans(broker, "destinationName=testTopic,direction=*", 10000));
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
            assertEquals(0, countMbeans(broker, "destinationName=testTopic,direction=*", 1500));
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private int countMbeans(BrokerService broker, String type) throws Exception {
        return countMbeans(broker, type, 0);
    }

    private int countMbeans(BrokerService broker, String type, int timeout) throws Exception {
        final long expiryTime = System.currentTimeMillis() + timeout;

        if (!type.contains("=")) {
            type = type + "=*";
        }

        final ObjectName beanName = new ObjectName("org.apache.activemq:type=Broker,brokerName="
                + broker.getBrokerName() + "," + type +",*");
        Set<ObjectName> mbeans = null;
        int count = 0;
        do {
            if (timeout > 0) {
                Thread.sleep(100);
            }

            LOG.info("Query name: " + beanName);
            mbeans = mBeanServer.queryNames(beanName, null);
            if (mbeans != null) {
                count = mbeans.size();
            } else {
                logAllMbeans(broker);
            }
        } while ((mbeans == null || mbeans.isEmpty()) && expiryTime > System.currentTimeMillis());

        // If port 1099 is in use when the Broker starts, starting the jmx connector
        // will fail.  So, if we have no mbsc to query, skip the test.
        if (timeout > 0) {
            assumeNotNull(mbeans);
        }

        return count;
    }

    private void logAllMbeans(BrokerService broker) throws MalformedURLException {
        try {
            // trace all existing MBeans
            Set<?> all = mBeanServer.queryNames(null, null);
            LOG.info("Total MBean count=" + all.size());
            for (Object o : all) {
                ObjectInstance bean = (ObjectInstance)o;
                LOG.info(bean.getObjectName().toString());
            }
        } catch (Exception ignored) {
            LOG.warn("getMBeanServer ex: " + ignored);
        }
    }
}
