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
 * 
 */
package org.apache.activemq.broker.util;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests TraceBrokerPathPlugin by creating two brokers linked by a network connector, and checking to see if the consuming end receives the expected value in the trace property
 * @author Raul Kripalani
 *
 */
public class TraceBrokerPathPluginTest extends TestCase {

	BrokerService brokerA;
	BrokerService brokerB;
	TransportConnector tcpConnectorA;
	TransportConnector tcpConnectorB;
	MessageProducer producer;
	MessageConsumer consumer;
	Connection connectionA;
	Connection connectionB;
	Session sessionA;
	Session sessionB;
	String queue = "TEST.FOO";
	String traceProperty = "BROKER_PATH";
	
	@Before
	public void setUp() throws Exception {
		TraceBrokerPathPlugin tbppA = new TraceBrokerPathPlugin();
		tbppA.setStampProperty(traceProperty);
		
		TraceBrokerPathPlugin tbppB = new TraceBrokerPathPlugin();
		tbppB.setStampProperty(traceProperty);
    	
        brokerA = new BrokerService();
        brokerA.setBrokerName("brokerA");
        brokerA.setPersistent(false);
        brokerA.setUseJmx(true);
        brokerA.setPlugins(new BrokerPlugin[] {tbppA});
        tcpConnectorA = brokerA.addConnector("tcp://localhost:0");

        brokerB = new BrokerService();
        brokerB.setBrokerName("brokerB");
        brokerB.setPersistent(false);
        brokerB.setUseJmx(true);
        brokerB.setPlugins(new BrokerPlugin[] {tbppB});
        tcpConnectorB = brokerB.addConnector("tcp://localhost:0");
        
        brokerA.addNetworkConnector("static:(" + tcpConnectorB.getConnectUri().toString() + ")");
        
        brokerB.start();
        brokerB.waitUntilStarted();
        brokerA.start();
        brokerA.waitUntilStarted();
        
        // Initialise connection to A and MessageProducer
        connectionA = new ActiveMQConnectionFactory(tcpConnectorA.getConnectUri()).createConnection();
        connectionA.start();
        sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = sessionA.createProducer(sessionA.createQueue(queue));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        
        // Initialise connection to B and MessageConsumer
        connectionB = new ActiveMQConnectionFactory(tcpConnectorB.getConnectUri()).createConnection();
        connectionB.start();
        sessionB = connectionB.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = sessionB.createConsumer(sessionB.createQueue(queue));
        
	}
	
	@After
	public void tearDown() throws Exception {
	     // Clean up
        producer.close();
        consumer.close();
        sessionA.close();
        sessionB.close();
        connectionA.close();
        connectionB.close();
        brokerA.stop();
        brokerB.stop();
	}
	
	@Test
    public void testTraceBrokerPathPlugin() throws Exception {
        Message sentMessage = sessionA.createMessage();
        producer.send(sentMessage);
        Message receivedMessage = consumer.receive(1000);

        // assert we got the message
        assertNotNull(receivedMessage);
        
        // assert we got the same message ID we sent
        assertEquals(sentMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        
        assertEquals("brokerA,brokerB", receivedMessage.getStringProperty(traceProperty));
        
	}
}
