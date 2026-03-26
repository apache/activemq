/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.jmx;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * This test shows that when we create a network connector via JMX,
 * the NC/bridge shows up in the MBean Server
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class JmxCreateNCTest {

    private static final String BROKER_NAME = "jmx-broker";

    private BrokerService broker;
    private BrokerViewMBean proxy;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        broker.setUseJmx(true); // explicitly set this so no funny issues
        broker.start();
        broker.waitUntilStarted();

        ObjectName brokerObjectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + BROKER_NAME);
        proxy = (BrokerViewMBean) broker.getManagementContext().newProxyInstance(brokerObjectName,
                BrokerViewMBean.class, true);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testBridgeRegistration() throws Exception {
        assertNotNull("We could not retrieve the broker from JMX", proxy);

        // let's add the NC
        String connectorName = proxy.addNetworkConnector("static:(tcp://localhost:61617)");
        assertEquals("NC", connectorName);

        // Make sure we can retrieve the NC through JMX
        ObjectName networkConnectorObjectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + BROKER_NAME +
                ",connector=networkConnectors,networkConnectorName=" + connectorName);
        NetworkConnectorViewMBean nc  = (NetworkConnectorViewMBean) broker.getManagementContext().newProxyInstance(networkConnectorObjectName,
                NetworkConnectorViewMBean.class, true);

        assertNotNull(nc);
        assertEquals("NC", nc.getName());
    }

    @Test
    public void testVmBridgeBlocked() throws Exception {
        // Test composite network connector uri
        try {
            proxy.addNetworkConnector("static:(vm://localhost)");
            fail("Should have failed trying to add vm connector bridge");
        } catch (IllegalArgumentException e) {
            assertEquals("VM scheme is not allowed", e.getMessage());
        }

        try {
            proxy.addNetworkConnector("multicast:(vm://localhost)");
            fail("Should have failed trying to add vm connector bridge");
        } catch (IllegalArgumentException e) {
            assertEquals("VM scheme is not allowed", e.getMessage());
        }

        // verify direct vm as well
        try {
            proxy.addNetworkConnector("vm://localhost");
            fail("Should have failed trying to add vm connector bridge");
        } catch (IllegalArgumentException e) {
            assertEquals("VM scheme is not allowed", e.getMessage());
        }

        try {
            // verify nested composite URI is blocked
            proxy.addNetworkConnector("static:(failover:(failover:(tcp://localhost:0,vm://localhost)))");
            fail("Should have failed trying to add vm connector bridge");
        } catch (IllegalArgumentException e) {
            assertEquals("VM scheme is not allowed", e.getMessage());
        }
    }

    @Test
    public void testAddNetworkConnectorMaxComposite() throws Exception {
        try {
            // verify nested composite URI with more than 5 levels is blocked
            proxy.addNetworkConnector(
                    "static:(failover:(failover:(failover:(failover:(failover:(tcp://localhost:0))))))");
            fail("Should have failed trying to add vm connector bridge");
        } catch (IllegalArgumentException e) {
            assertEquals("URI can't contain more than 5 nested composite URIs", e.getMessage());
        }
    }
}
