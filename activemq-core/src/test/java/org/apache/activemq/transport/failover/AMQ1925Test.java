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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.ServiceStopper;
import org.apache.log4j.Logger;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * TestCase showing the message-destroying described in AMQ-1925
 * 
 * @version $Revision: 1.1 $
 */
public class AMQ1925Test extends TestCase implements ExceptionListener {
	private static final Logger log = Logger.getLogger(AMQ1925Test.class);

	private static final String QUEUE_NAME = "test.amq1925";
	private static final String PROPERTY_MSG_NUMBER = "NUMBER";
	private static final int MESSAGE_COUNT = 10000;

	private BrokerService bs;
	private URI tcpUri;
	private ActiveMQConnectionFactory cf;

    private JMSException exception;

	public void XtestAMQ1925_TXInProgress() throws Exception {
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer = session.createConsumer(session
				.createQueue(QUEUE_NAME));

		// The runnable is likely to interrupt during the session#commit, since
		// this takes the longest
		final Object starter = new Object();
		final AtomicBoolean restarted = new AtomicBoolean();
		new Thread(new Runnable() {
			public void run() {
				try {
					synchronized (starter) {
						starter.wait();
					}

					// Simulate broker failure & restart
					bs.stop();
					bs = new BrokerService();
					bs.setPersistent(true);
					bs.setUseJmx(true);
					bs.addConnector(tcpUri);
					bs.start();

					restarted.set(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();

		synchronized (starter) {
			starter.notifyAll();
		}
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			Message message = consumer.receive(500);
			assertNotNull("No Message " + i + " found", message);

			if (i < 10)
				assertFalse("Timing problem, restarted too soon", restarted
						.get());
			if (i == 10) {
				synchronized (starter) {
					starter.notifyAll();
				}
			}
			if (i > MESSAGE_COUNT - 100) {
				assertTrue("Timing problem, restarted too late", restarted
						.get());
			}

			assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
			session.commit();
		}
		assertNull(consumer.receive(500));

		consumer.close();
		session.close();
		connection.close();

		assertQueueEmpty();
	}

	public void XtestAMQ1925_TXInProgress_TwoConsumers() throws Exception {
		Connection connection = cf.createConnection();
		connection.start();
		Session session1 = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer1 = session1.createConsumer(session1
				.createQueue(QUEUE_NAME));
		Session session2 = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer2 = session2.createConsumer(session2
				.createQueue(QUEUE_NAME));

		// The runnable is likely to interrupt during the session#commit, since
		// this takes the longest
		final Object starter = new Object();
		final AtomicBoolean restarted = new AtomicBoolean();
		new Thread(new Runnable() {
			public void run() {
				try {
					synchronized (starter) {
						starter.wait();
					}

					// Simulate broker failure & restart
					bs.stop();
					bs = new BrokerService();
					bs.setPersistent(true);
					bs.setUseJmx(true);
					bs.addConnector(tcpUri);
					bs.start();

					restarted.set(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();

		synchronized (starter) {
			starter.notifyAll();
		}
		Collection<Integer> results = new ArrayList<Integer>(MESSAGE_COUNT);
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			Message message1 = consumer1.receive(20);
			Message message2 = consumer2.receive(20);
			if (message1 == null && message2 == null) {
				if (results.size() < MESSAGE_COUNT) {
					message1 = consumer1.receive(500);
					message2 = consumer2.receive(500);

					if (message1 == null && message2 == null) {
						// Missing messages
						break;
					}
				}
				break;
			}

			if (i < 10)
				assertFalse("Timing problem, restarted too soon", restarted
						.get());
			if (i == 10) {
				synchronized (starter) {
					starter.notifyAll();
				}
			}
			if (i > MESSAGE_COUNT - 50) {
				assertTrue("Timing problem, restarted too late", restarted
						.get());
			}

			if (message1 != null) {
				results.add(message1.getIntProperty(PROPERTY_MSG_NUMBER));
				session1.commit();
			}
			if (message2 != null) {
				results.add(message2.getIntProperty(PROPERTY_MSG_NUMBER));
				session2.commit();
			}
		}
		assertNull(consumer1.receive(500));
		assertNull(consumer2.receive(500));

		consumer1.close();
		session1.close();
		consumer2.close();
		session2.close();
		connection.close();

		int foundMissingMessages = 0;
		if (results.size() < MESSAGE_COUNT) {
			foundMissingMessages = tryToFetchMissingMessages();
		}
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			assertTrue("Message-Nr " + i + " not found (" + results.size()
					+ " total, " + foundMissingMessages
					+ " have been found 'lingering' in the queue)", results
					.contains(i));
		}
		assertQueueEmpty();
	}

	private int tryToFetchMissingMessages() throws JMSException {
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true, 0);
		MessageConsumer consumer = session.createConsumer(session
				.createQueue(QUEUE_NAME));

		int count = 0;
		while (true) {
			Message message = consumer.receive(500);
			if (message == null)
				break;

			log.info("Found \"missing\" message: " + message);
			count++;
		}

		consumer.close();
		session.close();
		connection.close();

		return count;
	}

	public void testAMQ1925_TXBegin() throws Exception {
		Connection connection = cf.createConnection();
		connection.start();
		connection.setExceptionListener(this);
		Session session = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer = session.createConsumer(session
				.createQueue(QUEUE_NAME));

		for (int i = 0; i < MESSAGE_COUNT; i++) {
			Message message = consumer.receive(500);
			assertNotNull(message);

			if (i == 222) {
				// Simulate broker failure & restart
				bs.stop();
				bs = new BrokerService();
				bs.setPersistent(true);
				bs.setUseJmx(true);
				bs.addConnector(tcpUri);
				bs.start();
			}

			assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
			session.commit();
		}
		assertNull(consumer.receive(500));

		consumer.close();
		session.close();
		connection.close();

		assertQueueEmpty();
		assertNull("no exception on connection listener: " + exception, exception);
	}

	public void testAMQ1925_TXCommited() throws Exception {
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer = session.createConsumer(session
				.createQueue(QUEUE_NAME));

		for (int i = 0; i < MESSAGE_COUNT; i++) {
			Message message = consumer.receive(500);
			assertNotNull(message);

			assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
			session.commit();

			if (i == 222) {
				// Simulate broker failure & restart
				bs.stop();
				bs = new BrokerService();
				bs.setPersistent(true);
				bs.setUseJmx(true);
				bs.addConnector(tcpUri);
				bs.start();
			}
		}
		assertNull(consumer.receive(500));

		consumer.close();
		session.close();
		connection.close();

		assertQueueEmpty();
	}

	private void assertQueueEmpty() throws Exception {
		Connection connection = cf.createConnection();
		connection.start();
		Session session = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageConsumer consumer = session.createConsumer(session
				.createQueue(QUEUE_NAME));

		Message msg = consumer.receive(500);
		if (msg != null) {
			fail(msg.toString());
		}

		consumer.close();
		session.close();
		connection.close();

		assertQueueLength(0);
	}

	private void assertQueueLength(int len) throws Exception, IOException {
		Set<Destination> destinations = bs.getBroker().getDestinations(
				new ActiveMQQueue(QUEUE_NAME));
		Queue queue = (Queue) destinations.iterator().next();
		assertEquals(len, queue.getMessageStore().getMessageCount());
	}

	private void sendMessagesToQueue() throws Exception {
		Connection connection = cf.createConnection();
		Session session = connection.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageProducer producer = session.createProducer(session
				.createQueue(QUEUE_NAME));

		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			TextMessage message = session
					.createTextMessage("Test message " + i);
			message.setIntProperty(PROPERTY_MSG_NUMBER, i);
			producer.send(message);
		}
		session.commit();

		producer.close();
		session.close();
		connection.close();

		assertQueueLength(MESSAGE_COUNT);
	}

	protected void setUp() throws Exception {
	    exception = null;
		bs = new BrokerService();
		bs.setDeleteAllMessagesOnStartup(true);
		bs.setPersistent(true);
		bs.setUseJmx(true);
		TransportConnector connector = bs.addConnector("tcp://localhost:0");
		bs.start();
		tcpUri = connector.getConnectUri();

		cf = new ActiveMQConnectionFactory("failover://(" + tcpUri + ")");

		sendMessagesToQueue();
	}

	protected void tearDown() throws Exception {
		new ServiceStopper().stop(bs);
	}

    public void onException(JMSException exception) {
        this.exception = exception;    
    }

}
