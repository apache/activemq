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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.net.MalformedURLException;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuplexNetworkMBeanTest {

    protected static final Logger LOG = LoggerFactory.getLogger(DuplexNetworkMBeanTest.class);
    protected final int numRestarts = 3;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker");
        broker.addConnector("tcp://localhost:61617?transport.reuseAddress=true");

        return broker;
    }

    protected BrokerService createNetworkedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("networkedBroker");
        broker.addConnector("tcp://localhost:62617?transport.reuseAddress=true");
        NetworkConnector networkConnector = broker.addNetworkConnector("static:(tcp://localhost:61617?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
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
    
    
    
    
    private static class TestClient {
        private final Session session;
        private final MessageConsumer consumer;
        private final MessageProducer producer;
        
        public TestClient(BrokerService broker) throws Exception {
            Connection connection = new ActiveMQConnectionFactory(broker.getVmConnectorURI()).createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(session.createTopic("testTopic"));
            consumer = session.createConsumer(session.createTopic("testTopic"));
        }
        
        public void sendMsg(String msg) throws Exception {
            producer.send(session.createTextMessage(msg));
        }
        
        public void close() throws Exception {
            producer.close();
            consumer.close();
            session.close();
        }
    }
    
    @Test
    public void testMBeanBridgeDestinationPresenceOnBrokerRestart() throws Exception {
        // Validates fix for AMQ-5265 (https://issues.apache.org/jira/browse/AMQ-5265)
        
        BrokerService broker = createBroker();
        TestClient brokerClient = null, networkedClient = null;
        try {
            broker.start();
            broker.waitUntilStarted();
            
            for (int i=0; i<numRestarts; i++) {
                BrokerService networkedBroker = createNetworkedBroker();
                try {
                    networkedBroker.start();
                    waitForMbeanCount(networkedBroker, "connector=networkConnectors", 1, 10000);
                    waitForMbeanCount(broker, "connector=duplexNetworkConnectors", 1, 10000);
                    
                    // Start up producers/consumers on both brokers and wait for topic to register itself: 
                    brokerClient = new TestClient(broker);
                    networkedClient = new TestClient(networkedBroker);
                    waitForMbeanCount(broker, "destinationType=Topic,destinationName=testTopic", 4, 10000);
                    waitForMbeanCount(networkedBroker, "destinationType=Topic,destinationName=testTopic", 4, 10000);
                    
                    // Send messages to trigger creation of MBeanBridgeDestination mbeans 
                    // (with these producers/consumers, each broker should generate one inbound and one outbound)
                    brokerClient.sendMsg("test message 1");
                    networkedClient.sendMsg("test message 2");
                    waitForMbeanCount(networkedBroker, "destinationName=testTopic,direction=*", 2, 10000);
                    waitForMbeanCount(broker, "destinationName=testTopic,direction=*", 2, 10000);
                } finally {
                    // Shut down the networkedBroker - should trigger cleaning of the mbeans above.
                    brokerClient.close();
                    networkedClient.close();
                    networkedBroker.stop();
                }
                // Make sure the MBeanBridgeDestination mbeans on broker (which is still running) get cleaned up:
                waitForMbeanCount(broker, "destinationName=testTopic,direction=*", 0, 10000);
            }
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
            mbeans = broker.getManagementContext().queryNames(beanName, null);
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
    
    
    private int waitForMbeanCount(BrokerService broker, String queryPart, int expectedCount, int timeout) throws Exception {
        final long expiryTime = System.currentTimeMillis() + timeout;
        
        final ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName="
                + broker.getBrokerName() + "," + queryPart +",*");
        Set<ObjectName> mbeans = null;
        int count = -1;
        do {
            mbeans = broker.getManagementContext().queryNames(query, null);
            if (mbeans != null) {
                if (mbeans.size() == expectedCount) {
                    LOG.info("MBean query success - found {} MBean for query: {}", expectedCount, query);
                    return mbeans.size();
                }
                if (mbeans.size() != count) {
                    count = mbeans.size();
                    LOG.info("MBean query - found {} MBeans (expected {}), waiting {}sec for query: {}", 
                            count, expectedCount, (expiryTime - System.currentTimeMillis()) / 1000, query);
                }
            }
        } while (expiryTime > System.currentTimeMillis());
        
        // If port 1099 is in use when the Broker starts, starting the jmx connector
        // will fail.  So, if we have no mbsc to query, skip the test.
        assumeNotNull(mbeans);
        Assert.fail(String.format("Timed out waiting for mbean count of %s to become %s for query %s",
                count, expectedCount, query));
        return mbeans.size();
    }

    private void logAllMbeans(BrokerService broker) throws MalformedURLException {
        try {
            // trace all existing MBeans
            Set<?> all = broker.getManagementContext().queryNames(null, null);
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
