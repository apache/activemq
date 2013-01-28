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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.Queue;
import org.junit.Assert;

/**
 * This test demonstrates a bug in which calling
 * Queue#removeMatchingMessages("") generates an exception, whereas the JMS
 * specification states that an empty selector is valid.
 */
public class AMQ2528Test extends EmbeddedBrokerTestSupport {

	/**
	 * Setup the test so that the destination is a queue.
	 */
	protected void setUp() throws Exception {
		useTopic = false;
		super.setUp();
	}

	/**
	 * This test enqueues test messages to destination and then verifies that
	 * {@link Queue#removeMatchingMessages("")} removes all the messages.
	 */
	public void testRemoveMatchingMessages() throws Exception {
		final int NUM_MESSAGES = 100;
		final String MESSAGE_ID = "id";

		// Enqueue the test messages.
		Connection conn = createConnection();
		try {
			conn.start();
			Session session = conn.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(destination);
			for (int id = 0; id < NUM_MESSAGES; id++) {
				Message message = session.createMessage();
				message.setIntProperty(MESSAGE_ID, id);
				producer.send(message);
			}
			producer.close();
			session.close();
		} finally {
			conn.close();
		}

		// Verify that half of the messages can be removed by selector.
		Queue queue = (Queue) broker.getRegionBroker().getDestinations(
				destination).iterator().next();

		Assert.assertEquals(NUM_MESSAGES / 2, queue
				.removeMatchingMessages(MESSAGE_ID + " < " + NUM_MESSAGES / 2));

		// Verify that the remainder of the messages can be removed by empty
		// selector.
		Assert.assertEquals(NUM_MESSAGES - NUM_MESSAGES / 2, queue
				.removeMatchingMessages(""));
	}
}
