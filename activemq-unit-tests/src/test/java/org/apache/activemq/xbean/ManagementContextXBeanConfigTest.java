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

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ManagementContextXBeanConfigTest extends TestCase {

    private static final String MANAGEMENT_CONTEXT_TEST_XML = "org/apache/activemq/xbean/management-context-test.xml";
    private static final String MANAGEMENT_CONTEXT_TEST_XML_CONNECTOR_PATH =
            "org/apache/activemq/xbean/management-context-test-connector-path.xml";

    private static final transient Logger LOG = LoggerFactory.getLogger(ManagementContextXBeanConfigTest.class);

    protected BrokerService brokerService;

    public void testManagmentContextConfiguredCorrectly() throws Exception {
        brokerService = getBrokerService(MANAGEMENT_CONTEXT_TEST_XML);
        brokerService.start();

        assertEquals(2011, brokerService.getManagementContext().getConnectorPort());
        assertEquals("test.domain", brokerService.getManagementContext().getJmxDomainName());
        // Make sure the broker is registered in the right jmx domain.
        Hashtable<String, String> map = new Hashtable<String, String>();
        map.put("type", "Broker");
        map.put("brokerName", JMXSupport.encodeObjectNamePart("localhost"));
        ObjectName on = new ObjectName("test.domain", map);
        Object value = brokerService.getManagementContext().getAttribute(on, "TotalEnqueueCount");
        assertNotNull(value);
    }

    public void testSuccessAuthentication() throws Exception {
        brokerService = getBrokerService(MANAGEMENT_CONTEXT_TEST_XML);
        brokerService.start();

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:2011/jmxrmi");
        Map<String, Object> env = new HashMap<String, Object>();
        env.put(JMXConnector.CREDENTIALS, new String[]{"admin", "activemq"});
        JMXConnector connector = JMXConnectorFactory.connect(url, env);
        assertAuthentication(connector);
    }

    public void testConnectorPath() throws Exception {
        brokerService = getBrokerService(MANAGEMENT_CONTEXT_TEST_XML_CONNECTOR_PATH);
        brokerService.start();

        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:2011/activemq-jmx");
        Map<String, Object> env = new HashMap<String, Object>();
        env.put(JMXConnector.CREDENTIALS, new String[]{"admin", "activemq"});
        JMXConnector connector = JMXConnectorFactory.connect(url, env);
        assertAuthentication(connector);
    }

    public void testFailAuthentication() throws Exception {
        brokerService = getBrokerService(MANAGEMENT_CONTEXT_TEST_XML);
        brokerService.start();

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
        ObjectName name = new ObjectName("test.domain:type=Broker,brokerName=localhost");
        BrokerViewMBean mbean = MBeanServerInvocationHandler
                .newProxyInstance(connection, name, BrokerViewMBean.class, true);
        LOG.info("Broker " + mbean.getBrokerId() + " - " + mbean.getBrokerName());
    }

    @Override
    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    private BrokerService getBrokerService(String uri) throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

}
