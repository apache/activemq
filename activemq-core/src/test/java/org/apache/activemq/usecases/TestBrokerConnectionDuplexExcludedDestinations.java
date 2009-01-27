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

package org.apache.activemq.usecases;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

public class TestBrokerConnectionDuplexExcludedDestinations extends TestCase {

	public void testBrokerConnectionDuplexPropertiesPropagation()
			throws Exception {

		// Hub broker
		String configFileName = "org/apache/activemq/usecases/receiver-duplex.xml";
		URI uri = new URI("xbean:" + configFileName);
		BrokerService receiverBroker = BrokerFactory.createBroker(uri);
		receiverBroker.setPersistent(false);
		receiverBroker.setBrokerName("Hub");

		// Spoke broker
		configFileName = "org/apache/activemq/usecases/sender-duplex.xml";
		uri = new URI("xbean:" + configFileName);
		BrokerService senderBroker = BrokerFactory.createBroker(uri);
		senderBroker.setPersistent(false);
		receiverBroker.setBrokerName("Spoke");

		// Start both Hub and Spoke broker
		receiverBroker.start();
		senderBroker.start();

		final ConnectionFactory cfHub = new ActiveMQConnectionFactory(
				"tcp://localhost:62002");
		final Connection hubConnection = cfHub.createConnection();
		hubConnection.start();
		final Session hubSession = hubConnection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		final MessageProducer hubProducer = hubSession.createProducer(null);
		hubProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		hubProducer.setDisableMessageID(true);
		hubProducer.setDisableMessageTimestamp(true);

		final Queue excludedQueueHub = hubSession.createQueue("exclude.test.foo");
		final TextMessage excludedMsgHub = hubSession.createTextMessage();
		excludedMsgHub.setText(excludedQueueHub.toString());
		
		final Queue includedQueueHub = hubSession.createQueue("include.test.foo");

		final TextMessage includedMsgHub = hubSession.createTextMessage();
		excludedMsgHub.setText(includedQueueHub.toString());		

		// Sending from Hub queue
		hubProducer.send(excludedQueueHub, excludedMsgHub);
		hubProducer.send(includedQueueHub, includedMsgHub);

		final ConnectionFactory cfSpoke = new ActiveMQConnectionFactory(
				"tcp://localhost:62001");
		final Connection spokeConnection = cfSpoke.createConnection();
		spokeConnection.start();
		final Session spokeSession = spokeConnection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		final Queue excludedQueueSpoke = spokeSession.createQueue("exclude.test.foo");
		final MessageConsumer excludedConsumerSpoke = spokeSession
				.createConsumer(excludedQueueSpoke);

		final Queue includedQueueSpoke = spokeSession.createQueue("include.test.foo");
		final MessageConsumer includedConsumerSpoke = spokeSession
				.createConsumer(includedQueueSpoke);		
		
		// Receiving from excluded Spoke queue
		Message msg = excludedConsumerSpoke.receive(200);
		assertNull(msg);
		
		// Receiving from included Spoke queue
		msg = includedConsumerSpoke.receive(200);
		assertEquals(msg, includedMsgHub);

		excludedConsumerSpoke.close();
		hubSession.close();
		hubConnection.stop();
		hubConnection.close();
		hubProducer.close();
		spokeSession.close();
		spokeConnection.stop();
		spokeConnection.close();

		senderBroker.stop();
		receiverBroker.stop();

	}
}
