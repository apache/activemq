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

package org.apache.activemq.transport.failover;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class FailoverRandomTest extends TestCase {
	
    BrokerService brokerA, brokerB;
   
    public void setUp() throws Exception {
        brokerA = createBroker("A");
        brokerB = createBroker("B");
    }
    
    public void tearDown() throws Exception {
        brokerA.stop();
        brokerB.stop();
    }
    
	private BrokerService createBroker(String name) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("Broker"+ name);
        broker.addConnector("tcp://localhost:0");
        broker.getManagementContext().setCreateConnector(false);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        return broker;
    }

    public void testRandomConnections() throws Exception {
        String failoverUrl = "failover:("
            + brokerA.getTransportConnectors().get(0).getConnectUri()
            + ","
            + brokerB.getTransportConnectors().get(0).getConnectUri()
            + ")";
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(failoverUrl);
		
		
		ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
		connection.start();
		String brokerName1 = connection.getBrokerName();
		assertNotNull(brokerName1);
		connection.close();
		
		String brokerName2 = brokerName1;
		int attempts = 5;
		while (brokerName1.equals(brokerName2) && attempts-- > 0) {
		    connection = (ActiveMQConnection) cf.createConnection();
		    connection.start();
		    brokerName2 = connection.getBrokerName();
		    assertNotNull(brokerName2);
		    connection.close();
		}
        assertTrue(brokerName1 + "!=" + brokerName2, !brokerName1.equals(brokerName2));
    }
}
