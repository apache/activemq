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

import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DuplexNetworkMBeanTest extends TestCase {

    protected static final Log LOG = LogFactory.getLog(DuplexNetworkMBeanTest.class);
    protected final int numRestarts = 5;

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
    
    public void testMbeanPresenceOnNetworkBrokerRestart() throws Exception {
        BrokerService broker = createBroker();
        broker.start();
        assertEquals(1, countMbeans(broker, "Connector"));
        assertEquals(0, countMbeans(broker, "Connection"));
        BrokerService networkedBroker = null;
        for (int i=0; i<numRestarts; i++) {       
            networkedBroker = createNetworkedBroker();
            networkedBroker.start();
            assertEquals(1, countMbeans(networkedBroker, "NetworkBridge", 2000));
            assertEquals(1, countMbeans(broker, "Connection"));
            networkedBroker.stop();
            networkedBroker.waitUntilStopped();
            assertEquals(0, countMbeans(networkedBroker, "stopped"));
        }
        
        assertEquals(0, countMbeans(networkedBroker, "NetworkBridge"));
        assertEquals(0, countMbeans(networkedBroker, "Connector"));
        assertEquals(0, countMbeans(networkedBroker, "Connection"));
        assertEquals(1, countMbeans(broker, "Connector"));
        broker.stop();
    }

    public void testMbeanPresenceOnBrokerRestart() throws Exception {
        
        BrokerService networkedBroker = createNetworkedBroker();
        networkedBroker.start();
        assertEquals(1, countMbeans(networkedBroker, "Connector"));
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
        
        assertEquals(0, countMbeans(networkedBroker, "NetworkBridge"));
        assertEquals(1, countMbeans(networkedBroker, "Connector"));
        assertEquals(0, countMbeans(networkedBroker, "Connection"));
        assertEquals(0, countMbeans(broker, "Connection"));
        
        networkedBroker.stop();
    }
    
    private int countMbeans(BrokerService broker, String type) throws Exception {
        return countMbeans(broker, type, 0); 
    }

    private int countMbeans(BrokerService broker, String type, int timeout) throws Exception {
        final long expiryTime = System.currentTimeMillis() + timeout;
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

        Set all = mbsc.queryMBeans(null, null);
        LOG.info("MBean total=" + all.size());
        for (Object o : all) {
            ObjectInstance bean = (ObjectInstance)o;
            LOG.info(bean.getObjectName());
        }
        ObjectName beanName = new ObjectName("org.apache.activemq:BrokerName="
                + broker.getBrokerName() + ",Type=" + type +",*");
        Set mbeans = null;
        do { 
            if (timeout > 0) {
                Thread.sleep(100);
            }
            mbeans = mbsc.queryMBeans(beanName, null);
        } while (mbeans.isEmpty() && expiryTime > System.currentTimeMillis());
        return mbeans.size();
    }
}
