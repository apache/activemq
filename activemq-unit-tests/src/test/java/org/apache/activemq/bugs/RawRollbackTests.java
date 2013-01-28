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

package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RawRollbackTests {
	
	private static ConnectionFactory connectionFactory;
	private static Destination queue;
	private static BrokerService broker;

	@BeforeClass
	public static void clean() throws Exception {
		broker = new BrokerService();
		broker.setDeleteAllMessagesOnStartup(true);
		broker.setUseJmx(true);
		broker.start();
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
		connectionFactory.setBrokerURL("vm://localhost?async=false&waitForStart=5000&jms.prefetchPolicy.all=0");
		RawRollbackTests.connectionFactory = connectionFactory;
		queue = new ActiveMQQueue("queue");
	}

	@AfterClass
	public static void close() throws Exception {
		broker.stop();
	}

	@Before
	public void clearData() throws Exception {
		getMessages(false); // drain queue
		convertAndSend("foo");
		convertAndSend("bar");
	}


	@After
	public void checkPostConditions() throws Exception {

		Thread.sleep(1000L);
		List<String> list = getMessages(false);
		assertEquals(2, list.size());

	}

	@Test
	public void testReceiveMessages() throws Exception {

		List<String> list = getMessages(true);
		assertEquals(2, list.size());
		assertTrue(list.contains("foo"));

	}
	
	private void convertAndSend(String msg) throws Exception {
		Connection connection = connectionFactory.createConnection();
		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(queue);
		producer.send(session.createTextMessage(msg));
		producer.close();
		session.commit();
		session.close();
		connection.close();
	}

	private List<String> getMessages(boolean rollback) throws Exception {
		Connection connection = connectionFactory.createConnection();
		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		String next = "";
		List<String> msgs = new ArrayList<String>();
		while (next != null) {
			next = (String) receiveAndConvert(session);
			if (next != null)
				msgs.add(next);
		}
		if (rollback) {
			session.rollback();
		} else {
			session.commit();
		}
		session.close();
		connection.close();
		return msgs;
	}

	private String receiveAndConvert(Session session) throws Exception {
		MessageConsumer consumer = session.createConsumer(queue);
		Message message = consumer.receive(100L);
		consumer.close();
		if (message==null) {
			return null;
		}
		return ((TextMessage)message).getText();
	}
}
