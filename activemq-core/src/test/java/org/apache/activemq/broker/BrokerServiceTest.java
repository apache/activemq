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
package org.apache.activemq.broker;

import org.apache.activemq.network.NetworkConnector;

import junit.framework.TestCase;

/**
 * Tests for the BrokerService class 
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
		NetworkConnector connector = service.addNetworkConnector("multicast://default");
		service.start();
		
		service.removeNetworkConnector(connector);
		connector.stop();
		service.stop();
	}
	
	public void testAddRemoveNetworkWithoutJMX() throws Exception {
		BrokerService service = new BrokerService();
		service.setPersistent(false);
		service.setUseJmx(false);
		NetworkConnector connector = service.addNetworkConnector("multicast://default");
		service.start();
		
		service.removeNetworkConnector(connector);
		connector.stop();
		service.stop();
	}
}
