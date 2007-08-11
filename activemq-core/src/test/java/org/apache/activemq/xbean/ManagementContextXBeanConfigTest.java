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
import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.JMXSupport;

/**
 * @version $Revision: 1.1 $
 */
public class ManagementContextXBeanConfigTest extends TestCase {

    protected BrokerService brokerService;

    public void testManagmentContextConfiguredCorrectly() throws Exception {

        assertEquals(2011, brokerService.getManagementContext().getConnectorPort());
        assertEquals("test.domain", brokerService.getManagementContext().getJmxDomainName());

        MBeanServer beanServer = brokerService.getManagementContext().getMBeanServer();

        // Make sure the broker is registered in the right jmx domain.
        Hashtable<String, String> map = new Hashtable<String, String>();
        map.put("Type", "Broker");
        map.put("BrokerName", JMXSupport.encodeObjectNamePart("localhost"));
        ObjectName on = new ObjectName("test.domain", map);

        Object value = beanServer.getAttribute(on, "TotalEnqueueCount");
        assertNotNull(value);
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
