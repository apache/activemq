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

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;

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
        broker.start();
        assertEquals(1, countMbeans(broker, "Connector", 30000));
        assertEquals(0, countMbeans(broker, "Connection"));
        BrokerService networkedBroker = null;
        for (int i=0; i<numRestarts; i++) {       
            networkedBroker = createNetworkedBroker();
            networkedBroker.start();
            assertEquals(1, countMbeans(networkedBroker, "NetworkBridge", 2000));
            assertEquals(1, countMbeans(broker, "NetworkBridge", 2000));
            assertEquals(1, countMbeans(broker, "Connection"));
            networkedBroker.stop();
            networkedBroker.waitUntilStopped();
            assertEquals(0, countMbeans(networkedBroker, "stopped"));
            assertEquals(0, countMbeans(broker, "NetworkBridge"));
        }
        
        assertEquals(0, countMbeans(networkedBroker, "NetworkBridge"));
        assertEquals(0, countMbeans(networkedBroker, "Connector"));
        assertEquals(0, countMbeans(networkedBroker, "Connection"));
        assertEquals(1, countMbeans(broker, "Connector"));
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testMbeanPresenceOnBrokerRestart() throws Exception {
        
        BrokerService networkedBroker = createNetworkedBroker();
        networkedBroker.start();
        assertEquals(1, countMbeans(networkedBroker, "Connector", 30000));
        assertEquals(0, countMbeans(networkedBroker, "Connection"));
        
        BrokerService broker = null;
        for (int i=0; i<numRestarts; i++) {
            broker = createBroker();
            broker.start();
            assertEquals(1, countMbeans(networkedBroker, "NetworkBridge", 5000));
            assertEquals("restart number: " + i, 1, countMbeans(broker, "Connection", 10000));
            
            broker.stop();
            broker.waitUntilStopped();
            assertEquals(0, countMbeans(broker, "stopped"));
        }
        
        //assertEquals(0, countMbeans(networkedBroker, "NetworkBridge"));
        assertEquals(1, countMbeans(networkedBroker, "Connector"));
        assertEquals(0, countMbeans(networkedBroker, "Connection"));
        assertEquals(0, countMbeans(broker, "Connection"));
        
        networkedBroker.stop();
        networkedBroker.waitUntilStopped();
    }
    
    private int countMbeans(BrokerService broker, String type) throws Exception {
        return countMbeans(broker, type, 0); 
    }

    private int countMbeans(BrokerService broker, String type, int timeout) throws Exception {
        final long expiryTime = System.currentTimeMillis() + timeout;
        final ObjectName beanName = new ObjectName("org.apache.activemq:BrokerName="
                + broker.getBrokerName() + ",Type=" + type +",*");
        Set<?> mbeans = null;
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
        
        // If port 1099 is in use when the Broker starts, starting the jmx
        // connector will fail.  So, if we have no mbsc to query, skip the
        // test.
        if (timeout > 0) {
            assumeNotNull(mbeans);
        }
        
        return count;
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
