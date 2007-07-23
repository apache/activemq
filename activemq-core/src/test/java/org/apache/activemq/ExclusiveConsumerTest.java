/**
*
* Copyright 2005-2006 The Apache Software Foundation
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class ExclusiveConsumerTest extends TestCase {

	private static final String VM_BROKER_URL = "vm://localhost?broker.persistent=false&broker.useJmx=true";
	
	public ExclusiveConsumerTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	private Connection createConnection(final boolean start) throws JMSException {
		ConnectionFactory cf = new ActiveMQConnectionFactory(VM_BROKER_URL);
		Connection conn = cf.createConnection();
		if (start) {
			conn.start();
		}
		return conn;
	}
	
	public void testExclusiveConsumerSelectedCreatedFirst() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE1?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer = exclusiveSession.createConsumer(exclusiveQueue);

			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE1");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
	
	
			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE1");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			//TODO need two send a 2nd message - bug AMQ-1024
			//producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
	
	public void testExclusiveConsumerSelectedCreatedAfter() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE5");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
	
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE5?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer = exclusiveSession.createConsumer(exclusiveQueue);
	
			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE5");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
	
	public void testFailoverToAnotherExclusiveConsumerCreatedFirst() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession1 = null;
		Session exclusiveSession2 = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			exclusiveSession2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// This creates the exclusive consumer first which avoids AMQ-1024 bug.
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE2?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer1 = exclusiveSession1.createConsumer(exclusiveQueue);
			MessageConsumer exclusiveConsumer2 = exclusiveSession2.createConsumer(exclusiveQueue);

			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE2");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
	
			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE2");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer1.receive(100));
			Assert.assertNull(exclusiveConsumer2.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
			// Close the exclusive consumer to verify the non-exclusive consumer takes over
			exclusiveConsumer1.close();
	
			producer.send(msg);
			producer.send(msg);
			
			Assert.assertNotNull(exclusiveConsumer2.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));


		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
	
	public void testFailoverToAnotherExclusiveConsumerCreatedAfter() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession1 = null;
		Session exclusiveSession2 = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			exclusiveSession2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// This creates the exclusive consumer first which avoids AMQ-1024 bug.
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE6?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer1 = exclusiveSession1.createConsumer(exclusiveQueue);

			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE6");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
	
			MessageConsumer exclusiveConsumer2 = exclusiveSession2.createConsumer(exclusiveQueue);

			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE6");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer1.receive(100));
			Assert.assertNull(exclusiveConsumer2.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
			// Close the exclusive consumer to verify the non-exclusive consumer takes over
			exclusiveConsumer1.close();
	
			producer.send(msg);
			producer.send(msg);
			
			Assert.assertNotNull(exclusiveConsumer2.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));


		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
	public void testFailoverToNonExclusiveConsumer() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// This creates the exclusive consumer first which avoids AMQ-1024 bug.
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE3?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer = exclusiveSession.createConsumer(exclusiveQueue);

			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE3");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
	
			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE3");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
			// Close the exclusive consumer to verify the non-exclusive consumer takes over
			exclusiveConsumer.close();
	
			producer.send(msg);
	
			Assert.assertNotNull(fallbackConsumer.receive(100));

		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
	
	public void testFallbackToExclusiveConsumer() throws JMSException, InterruptedException {
		Connection conn = createConnection(true);
		
		Session exclusiveSession = null;
		Session fallbackSession = null;
		Session senderSession = null;

		try {
			
			exclusiveSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			fallbackSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// This creates the exclusive consumer first which avoids AMQ-1024 bug.
			ActiveMQQueue exclusiveQueue = new ActiveMQQueue("TEST.QUEUE4?consumer.exclusive=true");
			MessageConsumer exclusiveConsumer = exclusiveSession.createConsumer(exclusiveQueue);

			ActiveMQQueue fallbackQueue = new ActiveMQQueue("TEST.QUEUE4");
			MessageConsumer fallbackConsumer = fallbackSession.createConsumer(fallbackQueue);
		
			ActiveMQQueue senderQueue = new ActiveMQQueue("TEST.QUEUE4");

			MessageProducer producer = senderSession.createProducer(senderQueue);
		
			Message msg = senderSession.createTextMessage("test");
			producer.send(msg);
			Thread.sleep(100);
			
			//Verify exclusive consumer receives the message.
			Assert.assertNotNull(exclusiveConsumer.receive(100));
			Assert.assertNull(fallbackConsumer.receive(100));
			
			// Close the exclusive consumer to verify the non-exclusive consumer takes over
			exclusiveConsumer.close();
	
			producer.send(msg);

			// Verify other non-exclusive consumer receices the message.
			Assert.assertNotNull(fallbackConsumer.receive(100));

			// Create exclusive consumer to determine if it will start receiving the messages.
			exclusiveConsumer = exclusiveSession.createConsumer(exclusiveQueue);

			producer.send(msg);
			Assert.assertNotNull(exclusiveConsumer.receive(100));	
			Assert.assertNull(fallbackConsumer.receive(100));

		} finally {
			fallbackSession.close();
			senderSession.close();
			conn.close();
		}
		
	}
}
