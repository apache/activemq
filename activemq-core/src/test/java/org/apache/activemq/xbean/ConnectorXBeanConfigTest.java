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
import java.util.List;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;

/**
 * @version $Revision: 1.1 $
 */
public class ConnectorXBeanConfigTest extends TestCase {

    protected BrokerService brokerService;

    public void testConnectorConfiguredCorrectly() throws Exception {

        TransportConnector connector = (TransportConnector)brokerService.getTransportConnectors().get(0);

        assertEquals(new URI("tcp://localhost:61636"), connector.getUri());
        assertTrue(connector.getTaskRunnerFactory() == brokerService.getTaskRunnerFactory());

        NetworkConnector netConnector = (NetworkConnector)brokerService.getNetworkConnectors().get(0);
        List excludedDestinations = netConnector.getExcludedDestinations();
        assertEquals(new ActiveMQQueue("exclude.test.foo"), excludedDestinations.get(0));
        assertEquals(new ActiveMQTopic("exclude.test.bar"), excludedDestinations.get(1));

        List dynamicallyIncludedDestinations = netConnector.getDynamicallyIncludedDestinations();
        assertEquals(new ActiveMQQueue("include.test.foo"), dynamicallyIncludedDestinations.get(0));
        assertEquals(new ActiveMQTopic("include.test.bar"), dynamicallyIncludedDestinations.get(1));

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
        String uri = "org/apache/activemq/xbean/connector-test.xml";
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

}
