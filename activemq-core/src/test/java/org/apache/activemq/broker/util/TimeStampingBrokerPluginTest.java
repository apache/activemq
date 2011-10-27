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
package org.apache.activemq.broker.util;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimeStampingBrokerPluginTest extends TestCase {

	BrokerService broker;
	TransportConnector tcpConnector;
	MessageProducer producer;
	MessageConsumer consumer;
	Connection connection;
	Session session;
	Destination destination;
	String queue = "TEST.FOO";
	long expiry = 500;
	
	@Before
	public void setUp() throws Exception {
		TimeStampingBrokerPlugin tsbp = new TimeStampingBrokerPlugin();
    	tsbp.setZeroExpirationOverride(expiry);
    	tsbp.setTtlCeiling(expiry);
    	
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setPlugins(new BrokerPlugin[] {tsbp});
        tcpConnector = broker.addConnector("tcp://localhost:0");
        
        // Add policy and individual DLQ strategy
        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessExpired(true);
        ((IndividualDeadLetterStrategy)strategy).setUseQueueForQueueMessages(true);
        ((IndividualDeadLetterStrategy)strategy).setQueuePrefix("DLQ.");
        strategy.setProcessNonPersistent(true);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);
        
        broker.start();
        // Create a ConnectionFactory
        ActiveMQConnectionFactory connectionFactory =
            new ActiveMQConnectionFactory(tcpConnector.getConnectUri());

        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination Queue
        destination = session.createQueue(queue);

        // Create a MessageProducer from the Session to the Topic or Queue
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	}
	
	@After
	public void tearDown() throws Exception {
	     // Clean up
        producer.close();
        consumer.close();
        session.close();
        connection.close();
        broker.stop();
	}
	@Test
    public void testExpirationSet() throws Exception {
    	
        // Create a messages
        Message sentMessage = session.createMessage();

        // Tell the producer to send the message
        long beforeSend = System.currentTimeMillis();
        producer.send(sentMessage);

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        // Wait for a message
        Message receivedMessage = consumer.receive(1000);

        // assert we got the same message ID we sent
        assertEquals(sentMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        
        // assert message timestamp is in window
        assertTrue("Expiration should be not null" + receivedMessage.getJMSExpiration() + "\n", Long.valueOf(receivedMessage.getJMSExpiration()) != null);

        // assert message expiration is in window
        assertTrue("Before send: " + beforeSend + " Msg ts: " + receivedMessage.getJMSTimestamp() + " Msg Expiry: " + receivedMessage.getJMSExpiration(), beforeSend <= receivedMessage.getJMSExpiration() && receivedMessage.getJMSExpiration() <= (receivedMessage.getJMSTimestamp() + expiry));
    }
    @Test
    public void testExpirationCelingSet() throws Exception {
    	
        // Create a messages
        Message sentMessage = session.createMessage();
        // Tell the producer to send the message
        long beforeSend = System.currentTimeMillis();
        long sendExpiry =  beforeSend + (expiry*22);
        sentMessage.setJMSExpiration(sendExpiry);

        producer.send(sentMessage);

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        // Wait for a message
        Message receivedMessage = consumer.receive(1000);

        // assert we got the same message ID we sent
        assertEquals(sentMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        
        // assert message timestamp is in window
        assertTrue("Expiration should be not null" + receivedMessage.getJMSExpiration() + "\n", Long.valueOf(receivedMessage.getJMSExpiration()) != null);

        // assert message expiration is in window
        assertTrue("Sent expiry: " + sendExpiry + " Recv ts: " + receivedMessage.getJMSTimestamp() + " Recv expiry: " + receivedMessage.getJMSExpiration(), beforeSend <= receivedMessage.getJMSExpiration() && receivedMessage.getJMSExpiration() <= (receivedMessage.getJMSTimestamp() + expiry));
    }
    
    @Test
    public void testExpirationDLQ() throws Exception {
    	
        // Create a messages
        Message sentMessage = session.createMessage();
        // Tell the producer to send the message
        long beforeSend = System.currentTimeMillis();
        long sendExpiry =  beforeSend + expiry;
        sentMessage.setJMSExpiration(sendExpiry);

        producer.send(sentMessage);

        // Create a MessageConsumer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);

        Thread.sleep(expiry+250);
        
        // Wait for a message
        Message receivedMessage = consumer.receive(1000);

        // Message should roll to DLQ
        assertNull(receivedMessage);
                
        // Close old consumer, setup DLQ listener
        consumer.close();
        consumer = session.createConsumer(session.createQueue("DLQ."+queue));
        
        // Get mesage from DLQ
        receivedMessage = consumer.receive(1000);

        // assert we got the same message ID we sent
        assertEquals(sentMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        
        // assert message timestamp is in window
        //System.out.println("Recv: " + receivedMessage.getJMSExpiration());
        assertEquals("Expiration should be zero" + receivedMessage.getJMSExpiration() + "\n", receivedMessage.getJMSExpiration(), 0);
        
    }
}
