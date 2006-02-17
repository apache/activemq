/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.xbean;

import java.net.URI;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class ConnectorXBeanConfigTest extends TestCase {

    protected BrokerService brokerService;

    public void testConnectorConfiguredCorrectly() throws Throwable {
        
        TransportConnector connector = (TransportConnector) brokerService.getTransportConnectors().get(0);
        
        assertEquals( new URI("tcp://localhost:61636"), connector.getUri() );
        assertTrue( connector.getTaskRunnerFactory() == brokerService.getTaskRunnerFactory() );
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
        return BrokerFactory.createBroker(new URI("xbean:"+uri));
    }

}
