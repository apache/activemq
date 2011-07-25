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
package org.apache.activemq.console.command;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.apache.activemq.console.util.JmxMBeansUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestPurgeCommand extends TestCase {
	private static final Logger LOG = LoggerFactory
			.getLogger(TestPurgeCommand.class);

	protected static final int MESSAGE_COUNT = 10;
	protected static final String PROPERTY_NAME = "XTestProperty";
	protected static final String PROPERTY_VALUE = "1";
	protected static final String MSG_SEL_WITH_PROPERTY = PROPERTY_NAME
			+ " is not null";

	protected static final String MSG_SEL_WITHOUT_PROPERTY = PROPERTY_NAME
			+ " is null";
	protected static final String QUEUE_NAME = "org.apache.activemq.network.jms.QueueBridgeTest";

	protected AbstractApplicationContext context;
	protected QueueConnection localConnection;
	protected QueueRequestor requestor;
	protected QueueSession requestServerSession;
	protected MessageConsumer requestServerConsumer;
	protected MessageProducer requestServerProducer;
	protected Queue theQueue;

	protected void setUp() throws Exception {
		super.setUp();

		context = createApplicationContext();

		createConnections();

		requestServerSession = localConnection.createQueueSession(false,
				Session.AUTO_ACKNOWLEDGE);
		theQueue = requestServerSession.createQueue(QUEUE_NAME);
		requestServerConsumer = requestServerSession.createConsumer(theQueue);
		requestServerProducer = requestServerSession.createProducer(null);

		QueueSession session = localConnection.createQueueSession(false,
				Session.AUTO_ACKNOWLEDGE);
		requestor = new QueueRequestor(session, theQueue);
	}

	protected void createConnections() throws JMSException {
		ActiveMQConnectionFactory fac = (ActiveMQConnectionFactory) context
				.getBean("localFactory");
		localConnection = fac.createQueueConnection();
		localConnection.start();
	}

	protected AbstractApplicationContext createApplicationContext() {
		return new ClassPathXmlApplicationContext("activemq.xml");
	}

	protected void tearDown() throws Exception {
		localConnection.close();
		BrokerService broker = (BrokerService) context.getBean("localbroker");
		broker.stop();
		broker = (BrokerService) context.getBean("default");
		broker.stop();
		super.tearDown();
	}

	@SuppressWarnings("unchecked")
	public int getMessageCount(QueueBrowser browser, String prefix)
			throws JMSException {
		Enumeration e = browser.getEnumeration();
		int with = 0;
		while (e.hasMoreElements()) {
			Object o = e.nextElement();
			System.out.println(prefix + o);
			with++;
		}
		return with;
	}

	public void cleanup() throws JMSException {
		for (int i = 0; i < MESSAGE_COUNT * 2; i++) {
			requestServerConsumer.receive();
		}
	}

	protected MBeanServerConnection createJmxConnection() throws IOException {
		return ManagementFactory.getPlatformMBeanServer();
	}

	@SuppressWarnings("unchecked")
	public void purgeAllMessages() throws IOException, Exception {
		List<String> tokens = Arrays.asList(new String[] { "*" });
		for (String token : tokens) {
			List<ObjectInstance> queueList = JmxMBeansUtil.queryMBeans(
					createJmxConnection(), "Type=Queue,Destination=" + token
							+ ",*");
			for (ObjectInstance oi : queueList) {
				ObjectName queueName = oi.getObjectName();
				LOG.info("Purging all messages in queue: "
						+ queueName.getKeyProperty("Destination"));
				createJmxConnection().invoke(queueName, "purge",
						new Object[] {}, new String[] {});
			}
		}
	}

	public void addMessages() throws IOException, Exception {
		// first clean out any messages that may exist.
		purgeAllMessages();

		for (int i = 0; i < MESSAGE_COUNT; i++) {
			TextMessage msg = requestServerSession
					.createTextMessage("test msg: " + i);
			msg.setStringProperty(PROPERTY_NAME, PROPERTY_VALUE);
			requestServerProducer.send(theQueue, msg);
		}
		for (int i = 0; i < MESSAGE_COUNT; i++) {
			TextMessage msg = requestServerSession
					.createTextMessage("test msg: " + i);
			requestServerProducer.send(theQueue, msg);
		}

	}

	public void validateCounts(int expectedWithCount, int expectedWithoutCount,
			int expectedAllCount) throws JMSException {
		QueueBrowser withPropertyBrowser = requestServerSession.createBrowser(
				theQueue, MSG_SEL_WITH_PROPERTY);
		QueueBrowser withoutPropertyBrowser = requestServerSession
				.createBrowser(theQueue, MSG_SEL_WITHOUT_PROPERTY);
		QueueBrowser allBrowser = requestServerSession.createBrowser(theQueue);

		int withCount = getMessageCount(withPropertyBrowser, "withProperty ");
		int withoutCount = getMessageCount(withoutPropertyBrowser,
				"withoutProperty ");
		int allCount = getMessageCount(allBrowser, "allMessages ");

		withPropertyBrowser.close();
		withoutPropertyBrowser.close();
		allBrowser.close();

		assertEquals("Expected withCount to be " + expectedWithCount + " was "
				+ withCount, expectedWithCount, withCount);
		assertEquals("Expected withoutCount to be " + expectedWithoutCount
				+ " was " + withoutCount, expectedWithoutCount, withoutCount);
		assertEquals("Expected allCount to be " + expectedAllCount + " was "
				+ allCount, expectedAllCount, allCount);
		LOG.info("withCount = " + withCount + "\n withoutCount = "
				+ withoutCount + "\n allCount = " + allCount + "\n  = " + "\n");
	}

	/**
	 * This test ensures that the queueViewMbean will work.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void testQueueViewMbean() throws Exception {

		try {
			addMessages();

			validateCounts(MESSAGE_COUNT, MESSAGE_COUNT, MESSAGE_COUNT * 2);

			List<String> tokens = Arrays.asList(new String[] { "*" });
			for (String token : tokens) {
				List<ObjectInstance> queueList = JmxMBeansUtil.queryMBeans(
						createJmxConnection(), "Type=Queue,Destination="
								+ token + ",*");

				for (ObjectInstance queue : queueList) {
					ObjectName queueName = ((ObjectInstance) queue)
							.getObjectName();
					QueueViewMBean proxy = (QueueViewMBean) MBeanServerInvocationHandler
							.newProxyInstance(createJmxConnection(), queueName,
									QueueViewMBean.class, true);
					int removed = proxy
							.removeMatchingMessages(MSG_SEL_WITH_PROPERTY);
					LOG.info("Removed: " + removed);
				}
			}

			validateCounts(0, MESSAGE_COUNT, MESSAGE_COUNT);

		} finally {
			purgeAllMessages();
		}
	}

	public void testPurgeCommand() throws Exception {
		try {
			PurgeCommand purgeCommand = new PurgeCommand();
			CommandContext context = new CommandContext();

			context.setFormatter(new CommandShellOutputFormatter(System.out));

			purgeCommand.setCommandContext(context);
			purgeCommand.setJmxUseLocal(true);

			List<String> tokens = new ArrayList<String>();
			tokens.add("--msgsel");
			tokens.add(MSG_SEL_WITH_PROPERTY);
			
			addMessages();
			validateCounts(MESSAGE_COUNT, MESSAGE_COUNT, MESSAGE_COUNT * 2);

			purgeCommand.execute(tokens);

			validateCounts(0, MESSAGE_COUNT, MESSAGE_COUNT);
		} finally {
			purgeAllMessages();
		}
	}
}
