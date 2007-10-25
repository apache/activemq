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
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConnectionPerMessageTest extends EmbeddedBrokerTestSupport {

	private static final Log LOG = LogFactory.getLog(ConnectionPerMessageTest.class);
	private static final int COUNT = 20000;
	protected String bindAddress;

	public void testConnectionPerMessage() throws Exception {
		final String topicName = "test.topic";

		LOG.info("Initializing pooled connection factory for JMS to URL: "
				+ bindAddress);
		final ActiveMQConnectionFactory normalFactory = new ActiveMQConnectionFactory();
		normalFactory.setBrokerURL(bindAddress);
		for (int i = 0; i < COUNT; i++) {

			if (i % 1000 == 0) {
				LOG.info(i);
			}

			Connection conn = null;
			try {

				conn = normalFactory.createConnection();
				final Session session = conn.createSession(false,
						Session.AUTO_ACKNOWLEDGE);
				final Topic topic = session.createTopic(topicName);
				final MessageProducer producer = session.createProducer(topic);
				producer.setDeliveryMode(DeliveryMode.PERSISTENT);

				final MapMessage m = session.createMapMessage();
				m.setInt("hey", i);

				producer.send(m);

			} catch (JMSException e) {
				LOG.warn(e.getMessage(), e);
			} finally {
				if (conn != null)
					try {
						conn.close();
					} catch (JMSException e) {
						LOG.warn(e.getMessage(), e);
					}
			}
		}
	}

	protected void setUp() throws Exception {
		bindAddress = "vm://localhost";
		super.setUp();
	}

	protected BrokerService createBroker() throws Exception {
		BrokerService answer = new BrokerService();
		answer.setUseJmx(false);
		answer.setPersistent(isPersistent());
		answer.addConnector(bindAddress);
		return answer;
	}

	protected boolean isPersistent() {
		return true;
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

}
