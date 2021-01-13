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
package org.apache.activemq.broker.virtual;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.CompositeQueue;
import org.apache.activemq.broker.region.virtual.FilteredDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.spring.ConsumerBean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeDestinationSendWhenNotMatchedTest extends EmbeddedBrokerTestSupport {

	private static final Logger LOG = LoggerFactory.getLogger(CompositeQueueTest.class);

	protected int total = 10;
	protected Connection connection;
	

	@Test
	public void testSendWhenNotMatched() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);
		// messageList1.waitForMessagesToArrive(0);
		// messageList2.waitForMessagesToArrive(1);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.B");
		Destination destination1 =new ActiveMQQueue("A.B");
		Destination destination2 = new ActiveMQQueue("A.C");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tet13"));
		
		messageList1.assertMessagesArrived(1);
		messageList2.assertMessagesArrived(0);
	}

	@Test
	public void testSendWhenMatched() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);
		// messageList1.waitForMessagesToArrive(0);
		// messageList2.waitForMessagesToArrive(1);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.B");
		Destination destination1 =new ActiveMQQueue("A.B");
		Destination destination2 = new ActiveMQQueue("A.C");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "test13"));
		
		messageList2.assertMessagesArrived(1);
		messageList1.assertMessagesArrived(0);

	}
	@Test
	public void testForwardOnlyFalseSendWhenMatchedTrue1() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.D");
		Destination destination1 =new ActiveMQQueue("A.D");
		Destination destination2 = new ActiveMQQueue("A.E");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tes13"));
	
		messageList1.assertMessagesArrived(1);
		messageList2.assertMessagesArrived(0);	

	}
	
	public void testForwardOnlyFalseSendWhenMatchedTrue2() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.D");
		Destination destination1 =new ActiveMQQueue("A.D");
		Destination destination2 = new ActiveMQQueue("A.E");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "test13"));
		Thread.sleep(1*1000);
		messageList2.assertMessagesArrived(1);
		messageList1.assertMessagesArrived(0);

	} 
	@Test
	public void testForwardOnlyFalseBackwardCompatability1() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.X");
		Destination destination1 =new ActiveMQQueue("A.X");
		Destination destination2 = new ActiveMQQueue("A.Y");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "test13"));
	
		messageList2.assertMessagesArrived(1);
		messageList1.assertMessagesArrived(1);

	}
	@Test
	public void testForwardOnlyFalseBackwardCompatability2() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.X");
		Destination destination1 =new ActiveMQQueue("A.X");
		Destination destination2 = new ActiveMQQueue("A.Y");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tet13"));
	
		
		messageList1.assertMessagesArrived(1);
		messageList2.assertMessagesArrived(0);

	}
	
	@Test
	public void testForwardOnlyTrueBackwardCompatability1() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.W");
		Destination destination1 =new ActiveMQQueue("A.W");
		Destination destination2 = new ActiveMQQueue("A.V");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "test13"));
	
		messageList2.assertMessagesArrived(1);
		messageList1.assertMessagesArrived(0);

	}
	@Test
	public void testForwardOnlyTrueBackwardCompatability2() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("A.W");
		Destination destination1 =new ActiveMQQueue("A.W");
		Destination destination2 = new ActiveMQQueue("A.V");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tet13"));	
		Thread.sleep(2*1000);
		messageList1.assertMessagesArrived(0);
		messageList2.assertMessagesArrived(0);

	}
	
	@Test
	public void testForwardOnlySendWhenNotMatchedSetToFalse() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);
		// messageList1.waitForMessagesToArrive(0);
		// messageList2.waitForMessagesToArrive(1);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("X.Y");
		Destination destination1 =  new ActiveMQQueue("X.Y");
		Destination destination2 =  new ActiveMQQueue("X.Z");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tet13"));
		messageList2.assertMessagesArrived(1);
		messageList1.assertMessagesArrived(0);

	}
	@Test
	public void testForwardOnlyFalseSendWhenNotMatchedSetToFalse() throws Exception {
		if (connection == null) {
			connection = createConnection();
		}
		connection.start();

		ConsumerBean messageList1 = new ConsumerBean();
		ConsumerBean messageList2 = new ConsumerBean();
		messageList1.setVerbose(true);
		messageList2.setVerbose(true);
		// messageList1.waitForMessagesToArrive(0);
		// messageList2.waitForMessagesToArrive(1);

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination producerDestination = new ActiveMQQueue("R.S");
		Destination destination1 =  new ActiveMQQueue("R.S");
		Destination destination2 =  new ActiveMQQueue("R.T");

		LOG.info("Sending to: " + producerDestination);
		LOG.info("Consuming from: " + destination1 + " and " + destination2);

		MessageConsumer c1 = session.createConsumer(destination1);
		MessageConsumer c2 = session.createConsumer(destination2);

		c1.setMessageListener(messageList1);
		c2.setMessageListener(messageList2);

		// create topic producer
		MessageProducer producer = session.createProducer(producerDestination);
		assertNotNull(producer);

		producer.send(createMessage(session, "tet13"));		
		messageList1.assertMessagesArrived(1);
		messageList2.assertMessagesArrived(1);
	}

	protected Destination getConsumer1Dsetination() {
		return new ActiveMQQueue("A.B");
	}

	protected Destination getConsumer2Dsetination() {
		return new ActiveMQQueue("A.C");
	}

	protected Destination getProducerDestination() {
		return new ActiveMQQueue("A.B");
	}

	protected TextMessage createMessage(Session session, String testid) throws JMSException {
		TextMessage textMessage = session.createTextMessage("testMessage");
		textMessage.setStringProperty("testid", testid);
		return textMessage;
	}

	protected BrokerService createBroker() throws Exception {
		BrokerService answer = new BrokerService();
		answer.setPersistent(isPersistent());
		answer.getManagementContext().setCreateConnector(false);
		answer.addConnector(bindAddress);
		/*
		 * <destinationInterceptors> <virtualDestinationInterceptor>
		 * <virtualDestinations> <compositeQueue name="A.B" forwardOnly="false">
		 * <forwardTo> <filteredDestination selector="testid LIKE 'test%'" queue="A.C"/>
		 * </forwardTo> </compositeQueue> </virtualDestinations>
		 * </virtualDestinationInterceptor> </destinationInterceptors>
		 */
		/*
		 * SendWhenNotMatched = true A message will be always forwarded to  if not matched to filtered destination 
		 * ForwardOnly setting has no impact
		 */
		
		CompositeQueue compositeQueue = new CompositeQueue();
		compositeQueue.setName("A.B");
		compositeQueue.setForwardOnly(true); // By default it is true
		compositeQueue.setSendWhenNotMatched(true);// By default it is false
		FilteredDestination filteredQueue = new FilteredDestination();
		filteredQueue.setQueue("A.C");
		filteredQueue.setSelector("testid LIKE 'test%'");
		final ArrayList<Object> forwardDestinations = new ArrayList<Object>();
		forwardDestinations.add(filteredQueue);
		compositeQueue.setForwardTo(forwardDestinations);
		
	
		CompositeQueue compositeQueue0 = new CompositeQueue();
		compositeQueue0.setName("A.D");
		compositeQueue0.setForwardOnly(false); // By default it is true
		compositeQueue0.setSendWhenNotMatched(true);// By default it is false
		FilteredDestination filteredQueue0 = new FilteredDestination();
		filteredQueue0.setQueue("A.E");
		filteredQueue0.setSelector("testid LIKE 'test%'");
		final ArrayList<Object> forwardDestinations0 = new ArrayList<Object>();
		forwardDestinations0.add(filteredQueue0);
		compositeQueue0.setForwardTo(forwardDestinations0);
		
		//Back compatibility test 1
		CompositeQueue compositeQueue01 = new CompositeQueue();
		compositeQueue01.setName("A.X");
		compositeQueue01.setForwardOnly(false); // By default it is true
		//compositeQueue01.setSendWhenNotMatched(false);// By default it is false
		FilteredDestination filteredQueue01 = new FilteredDestination();
		filteredQueue01.setQueue("A.Y");
		filteredQueue01.setSelector("testid LIKE 'test%'");
		final ArrayList<Object> forwardDestinations01 = new ArrayList<Object>();
		forwardDestinations01.add(filteredQueue01);
		compositeQueue01.setForwardTo(forwardDestinations01);
		
		//Back compatibility test 2
				CompositeQueue compositeQueue02 = new CompositeQueue();
				compositeQueue02.setName("A.W");
				//compositeQueue02.setForwardOnly(true); // By default it is true
				//compositeQueue01.setSendWhenNotMatched(false);// By default it is false
				FilteredDestination filteredQueue02 = new FilteredDestination();
				filteredQueue02.setQueue("A.V");
				filteredQueue02.setSelector("testid LIKE 'test%'");
				final ArrayList<Object> forwardDestinations02 = new ArrayList<Object>();
				forwardDestinations02.add(filteredQueue02);
				compositeQueue02.setForwardTo(forwardDestinations02);
		
		CompositeQueue compositeQueue1 = new CompositeQueue();
		compositeQueue1.setName("X.Y");
		compositeQueue1.setForwardOnly(true); // By default it is true
		ActiveMQQueue forwardQueue1 =new ActiveMQQueue("X.Z");	
		final ArrayList<Object> forwardDestinations1 = new ArrayList<Object>();		
		forwardDestinations1.add(forwardQueue1);
		compositeQueue1.setForwardTo(forwardDestinations1);
		
		
		CompositeQueue compositeQueue2 = new CompositeQueue();
		compositeQueue2.setName("R.S");
		compositeQueue2.setForwardOnly(false);
		ActiveMQQueue forwardQueue2 =new ActiveMQQueue("R.T");	
		final ArrayList<Object> forwardDestinations2 = new ArrayList<Object>();		
		forwardDestinations2.add(forwardQueue2);
		compositeQueue2.setForwardTo(forwardDestinations2);

		VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
		interceptor.setVirtualDestinations(new VirtualDestination[] { compositeQueue,compositeQueue0,compositeQueue01,compositeQueue02,compositeQueue1,compositeQueue2 });
		answer.setDestinationInterceptors(new DestinationInterceptor[] { interceptor });
		return answer;
	}
}
