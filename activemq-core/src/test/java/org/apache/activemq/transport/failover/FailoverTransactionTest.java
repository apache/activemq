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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// see https://issues.apache.org/activemq/browse/AMQ-2473
public class FailoverTransactionTest {
	
	private static final String QUEUE_NAME = "test.FailoverTransactionTest";
	private String url = "tcp://localhost:61616";
	BrokerService broker;
	
	@Before
	public void startCleanBroker() throws Exception {
	    startBroker(true);
	}
	
	@After
	public void stopBroker() throws Exception {
	    if (broker != null) {
	        broker.stop();
	    }
	}
	
	public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
	    broker = new BrokerService();
        broker.setUseJmx(false);
        broker.addConnector(url);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
	}
	
	@Test
	public void testFailoverProducerCloseBeforeTransaction() throws Exception {
		
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		Queue destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);
		MessageProducer producer = session.createProducer(destination);
		
		TextMessage message = session.createTextMessage("Test message");
		producer.send(message);

		// close producer before commit, emulate jmstemplate
		producer.close();
		
		// restart to force failover and connection state recovery before the commit
		broker.stop();
		startBroker(false);

		session.commit();
		assertNotNull("we got the message", consumer.receive(20000));
		session.commit();	
		connection.close();
	}
	
	@Test
	public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
	        
	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")?trackTransactionProducers=false");
	    Connection connection = cf.createConnection();
	    connection.start();
	    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = session.createQueue(QUEUE_NAME);
	    
	    MessageConsumer consumer = session.createConsumer(destination);
	    MessageProducer producer = session.createProducer(destination);
	    
	    TextMessage message = session.createTextMessage("Test message");
	    producer.send(message);
	    
	    // close producer before commit, emulate jmstemplate
	    producer.close();
	    
	    // restart to force failover and connection state recovery before the commit
	    broker.stop();
	    startBroker(false);
	    
	    session.commit();
	    
	    // withough tracking producers, message will not be replayed on recovery
	    assertNull("we got the message", consumer.receive(2000));
	    session.commit();   
	    connection.close();
	}
	
	@Test
	public void testFailoverMultipleProducerCloseBeforeTransaction() throws Exception {
	        
	    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + url + ")");
	    Connection connection = cf.createConnection();
	    connection.start();
	    Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
	    Queue destination = session.createQueue(QUEUE_NAME);
	    
	    MessageConsumer consumer = session.createConsumer(destination);
	    MessageProducer producer;
	    TextMessage message;
	    final int count = 10;
	    for (int i=0; i<count; i++) {
	        producer = session.createProducer(destination);	        
	        message = session.createTextMessage("Test message: " + count);
	        producer.send(message);
	        producer.close();
	    }
	    
	    // restart to force failover and connection state recovery before the commit
	    broker.stop();
	    startBroker(false);
	    
	    session.commit();
	    for (int i=0; i<count; i++) {
	        assertNotNull("we got all the message: " + count, consumer.receive(20000));
	    }
	    session.commit();
	    connection.close();
	}  
}
