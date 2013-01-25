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
package org.apache.activemq.web;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.web.config.SystemPropertiesConfiguration;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import java.lang.reflect.Field;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 *
 * You can use this class to connect up to a running web console and run some queries.
 * Used to work through https://issues.apache.org/jira/browse/AMQ-4272 but would be useful
 * in any scenario where you need access to the underlying broker in the web-console to hack
 * at it
 *
 */
public class RemoteJMXBrokerTest {


    private BrokerService brokerService;

    @Before
    public void startUp() throws Exception {
        brokerService = BrokerFactory.createBroker("broker:()/remoteBroker?useJmx=true");
        brokerService.start();
        brokerService.waitUntilStarted();

    }

    /**
     * Test that we can query the remote broker...
     * Specifically this tests that the domain and objectnames are correct (type and brokerName
     * instead of Type and BrokerName, which they were)
     * @throws Exception
     */
    @Test
    public void testConnectRemoteBrokerFacade() throws Exception {
        String jmxUri = getJmxUri();
        System.setProperty("webconsole.jmx.url", jmxUri);
        RemoteJMXBrokerFacade brokerFacade = new RemoteJMXBrokerFacade();

        SystemPropertiesConfiguration configuration = new SystemPropertiesConfiguration();
        brokerFacade.setConfiguration(configuration);

        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=remoteBroker");
        Set<ObjectName> queryResult = brokerFacade.queryNames(query, null);

        System.out.println("Number: "  + queryResult.size());
        assertEquals(1, queryResult.size());

    }


    public String  getJmxUri() throws NoSuchFieldException, IllegalAccessException {
        Field field = ManagementContext.class.getDeclaredField("connectorServer");
        field.setAccessible(true);
        JMXConnectorServer server = (JMXConnectorServer) field.get(brokerService.getManagementContext());
        return server.getAddress().toString();
    }
}
