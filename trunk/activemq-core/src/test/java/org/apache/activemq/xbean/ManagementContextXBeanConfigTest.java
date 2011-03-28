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
package org.apache.activemq.xbean;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.util.JMXSupport;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ManagementContextXBeanConfigTest extends TestCase {

    protected BrokerService brokerService;
    private static final transient Logger LOG = LoggerFactory.getLogger(ManagementContextXBeanConfigTest.class);

    public void testManagmentContextConfiguredCorrectly() throws Exception {
        assertEquals(2011, brokerService.getManagementContext().getConnectorPort());
        assertEquals("test.domain", brokerService.getManagementContext().getJmxDomainName());
        // Make sure the broker is registered in the right jmx domain.
        Hashtable<String, String> map = new Hashtable<String, String>();
        map.put("Type", "Broker");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart("localhost"));
        ObjectName on = new ObjectName("test.domain", map);
        Object value = brokerService.getManagementContext().getAttribute(on, "TotalEnqueueCount");
        assertNotNull(value);
    }

    public void testSuccessAuthentication() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:2011/jmxrmi");
        Map env = new HashMap();
        env.put(JMXConnector.CREDENTIALS, new String[]{"admin", "activemq"});
        JMXConnector connector = JMXConnectorFactory.connect(url, env);
        assertAuthentication(connector);
    }

    public void testFailAuthentication() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:2011/jmxrmi");
        try {
            JMXConnector connector = JMXConnectorFactory.connect(url, null);
            assertAuthentication(connector);
        } catch (SecurityException e) {
            return;
        }
        fail("Should have thrown an exception");
    }

    public void assertAuthentication(JMXConnector connector) throws Exception {
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("test.domain:BrokerName=localhost,Type=Broker");
        BrokerViewMBean mbean = (BrokerViewMBean) MBeanServerInvocationHandler
                .newProxyInstance(connection, name, BrokerViewMBean.class, true);
        LOG.info("Broker " + mbean.getBrokerId() + " - " + mbean.getBrokerName());
    }

    protected void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
    }

    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        String uri = "org/apache/activemq/xbean/management-context-test.xml";
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

}
