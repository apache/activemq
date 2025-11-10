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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import jakarta.jms.Connection;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests to validate Broker Health View
 * Needs to be Updated for other Mbeans
 */
public class HealthViewTest {

    private BrokerService brokerService;
    private String brokerConnectionUri = null;
    private final List<Connection> connections = new ArrayList<>();
    
    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
		brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerConnectionUri = brokerService.addConnector("tcp://localhost:61616?maximumConnections=12").getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
        
        if(!connections.isEmpty()) {
        	for(Connection connection : connections) {
        		connection.close();
        	}
        }
    }
    
    @Test
    public void testHealthWithHighConnectionCount() throws Exception {
        ObjectName healthViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,service=Health");
        HealthViewMBean healthViewMBean = (HealthViewMBean) brokerService.getManagementContext().newProxyInstance(
        		healthViewMBeanName, HealthViewMBean.class, true);
        String connectionLimitWarningMessage = null;
        String expectedConnectionLimitWarningMessage = "The Current connection count is within  91% of MaximumConnections limit";
        
        String connectionLimitErrorMessage = null;
        String expectedConnectionLimitErrorMessage= "Exceeded the maximum number of allowed client connections: 12";
        
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerConnectionUri);
        
        //Intial Status is Good
        assertEquals("Good",healthViewMBean.getCurrentStatus());
        
        //Creating 11 Connections i.e. 91% of Allowed Connections
        for(int i=0;i<11;i++) {
        	connections.add(connectionFactory.createConnection());
        }
        
        for(HealthStatus hs : healthViewMBean.healthList()) {
        	if(hs.getHealthId().equalsIgnoreCase("org.apache.activemq.transport.tcp.TcpTransportServer")) {
        		connectionLimitWarningMessage = hs.getMessage();
        		break;
        	}	
        }
        assertEquals(expectedConnectionLimitWarningMessage,connectionLimitWarningMessage);
        
        //Testing Max Connection Check by Creating One more connection
        connections.add(connectionFactory.createConnection());
        
        for(HealthStatus hs : healthViewMBean.healthList()) {
        	if(hs.getHealthId().equalsIgnoreCase("org.apache.activemq.transport.tcp.TcpTransportServer")) {
        		connectionLimitErrorMessage = hs.getMessage();
        		break;
        	}	
        }

        assertEquals(expectedConnectionLimitErrorMessage,connectionLimitErrorMessage);
    }
}
