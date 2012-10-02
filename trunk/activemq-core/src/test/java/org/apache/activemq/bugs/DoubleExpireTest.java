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

import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Assert;

public class DoubleExpireTest extends EmbeddedBrokerTestSupport {

	private static final long MESSAGE_TTL_MILLIS = 1000;
	private static final long MAX_TEST_TIME_MILLIS = 60000;

	public void setUp() throws Exception {
		setAutoFail(true);
		setMaxTestTime(MAX_TEST_TIME_MILLIS);
		super.setUp();
	}

	/**
	 * This test verifies that a message that expires can be be resent to queue
	 * with a new expiration and that it will be processed as a new message and
	 * allowed to re-expire.
	 * <p>
	 * <b>NOTE:</b> This test fails on AMQ 5.4.2 because the originalExpiration
	 * timestamp is not cleared when the message is resent.
	 */
	public void testDoubleExpireWithoutMove() throws Exception {
		// Create the default dead letter queue.
		final ActiveMQDestination DLQ = createDestination("ActiveMQ.DLQ");

		Connection conn = createConnection();
		try {
			conn.start();
			Session session = conn.createSession(false,
					Session.AUTO_ACKNOWLEDGE);

			// Verify that the test queue and DLQ are empty.
			Assert.assertEquals(0, getSize(destination));
			Assert.assertEquals(0, getSize(DLQ));

			// Enqueue a message to the test queue that will expire after 1s.
			MessageProducer producer = session.createProducer(destination);
			Message testMessage = session.createTextMessage("test message");
			producer.send(testMessage, Message.DEFAULT_DELIVERY_MODE,
					Message.DEFAULT_PRIORITY, MESSAGE_TTL_MILLIS);
			Assert.assertEquals(1, getSize(destination));

			// Wait for the message to expire.
			waitForSize(destination, 0, MAX_TEST_TIME_MILLIS);
			Assert.assertEquals(1, getSize(DLQ));

			// Consume the message from the DLQ and re-enqueue it to the test
			// queue so that it expires after 1s.
			MessageConsumer consumer = session.createConsumer(DLQ);
			Message expiredMessage = consumer.receive();
			Assert.assertEquals(testMessage.getJMSMessageID(), expiredMessage
					.getJMSMessageID());

			producer.send(expiredMessage, Message.DEFAULT_DELIVERY_MODE,
					Message.DEFAULT_PRIORITY, MESSAGE_TTL_MILLIS);
			Assert.assertEquals(1, getSize(destination));
			Assert.assertEquals(0, getSize(DLQ));

			// Verify that the resent message is "different" in that it has
			// another ID.
			Assert.assertNotSame(testMessage.getJMSMessageID(), expiredMessage
					.getJMSMessageID());

			// Wait for the message to re-expire.
			waitForSize(destination, 0, MAX_TEST_TIME_MILLIS);
			Assert.assertEquals(1, getSize(DLQ));

			// Re-consume the message from the DLQ.
			Message reexpiredMessage = consumer.receive();
			Assert.assertEquals(expiredMessage.getJMSMessageID(), reexpiredMessage
					.getJMSMessageID());
		} finally {
			conn.close();
		}
	}

	/**
	 * A helper method that returns the embedded broker's implementation of a
	 * JMS queue.
	 */
	private Queue getPhysicalDestination(ActiveMQDestination destination)
			throws Exception {
		return (Queue) broker.getAdminView().getBroker().getDestinationMap()
				.get(destination);
	}

	/**
	 * A helper method that returns the size of the specified queue/topic.
	 */
	private long getSize(ActiveMQDestination destination) throws Exception {
		return getPhysicalDestination(destination) != null ? getPhysicalDestination(
				destination).getDestinationStatistics().getMessages()
				.getCount()
				: 0;
	}

	/**
	 * A helper method that waits for a destination to reach a certain size.
	 */
	private void waitForSize(ActiveMQDestination destination, int size,
			long timeoutMillis) throws Exception, TimeoutException {
		long startTimeMillis = System.currentTimeMillis();

		while (getSize(destination) != size
				&& System.currentTimeMillis() < (startTimeMillis + timeoutMillis)) {
			Thread.sleep(250);
		}

		if (getSize(destination) != size) {
			throw new TimeoutException("Destination "
					+ destination.getPhysicalName() + " did not reach size "
					+ size + " within " + timeoutMillis + "ms.");
		}
	}
}
