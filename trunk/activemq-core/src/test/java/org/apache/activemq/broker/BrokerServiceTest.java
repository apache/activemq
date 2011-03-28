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
package org.apache.activemq.broker;

import junit.framework.TestCase;
import org.apache.activemq.network.NetworkConnector;

/**
 * Tests for the BrokerService class
 * 
 * @author chirino
 */
public class BrokerServiceTest extends TestCase {

    public void testAddRemoveTransportsWithJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setUseJmx(true);
        service.setPersistent(false);
        TransportConnector connector = service.addConnector("tcp://localhost:0");
        service.start();

        service.removeConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveTransportsWithoutJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        TransportConnector connector = service.addConnector("tcp://localhost:0");
        service.start();

        service.removeConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveNetworkWithJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(true);
        NetworkConnector connector = service.addNetworkConnector("multicast://default?group=group-"+System.currentTimeMillis());
        service.start();

        service.removeNetworkConnector(connector);
        connector.stop();
        service.stop();
    }

    public void testAddRemoveNetworkWithoutJMX() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);
        NetworkConnector connector = service.addNetworkConnector("multicast://default?group=group-"+System.currentTimeMillis());
        service.start();

        service.removeNetworkConnector(connector);
        connector.stop();
        service.stop();
    }
    
    public void testSystemUsage()
    {
        BrokerService service = new BrokerService();
        assertEquals( service.getSystemUsage().getMemoryUsage().getLimit(), 1024 * 1024 * 64 );
        assertEquals( service.getSystemUsage().getTempUsage().getLimit(), 1024L * 1024 * 1024 * 100 );
        assertEquals( service.getSystemUsage().getStoreUsage().getLimit(), 1024L * 1024 * 1024 * 100 );
    }
}
