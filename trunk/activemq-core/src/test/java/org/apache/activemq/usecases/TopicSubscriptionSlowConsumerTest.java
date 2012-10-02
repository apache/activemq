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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Assert;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;


/**
 * Checks to see if "slow consumer advisory messages" are generated when 
 * small number of messages (2) are published to a topic which has a subscriber 
 * with a prefetch of one set.
 * 
 */

public class TopicSubscriptionSlowConsumerTest extends TestCase {

	private static final String TOPIC_NAME = "slow.consumer";
	Connection connection;
	private Session session;
	private ActiveMQTopic destination;
	private MessageProducer producer;
	private MessageConsumer consumer;
	private BrokerService brokerService;

	
	public void setUp() throws Exception {

		brokerService = createBroker();
		
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
		
		activeMQConnectionFactory.setWatchTopicAdvisories(true);
		connection = activeMQConnectionFactory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		destination = new ActiveMQTopic(TOPIC_NAME);
		producer = session.createProducer(destination);
		
		connection.start();
	}

	
	
	public void testPrefetchValueOne() throws Exception{
		
		ActiveMQTopic consumerDestination = new ActiveMQTopic(TOPIC_NAME+"?consumer.prefetchSize=1");
		consumer = session.createConsumer(consumerDestination);
		
		//add a consumer to the slow consumer advisory topic. 
		ActiveMQTopic slowConsumerAdvisoryTopic = AdvisorySupport.getSlowConsumerAdvisoryTopic(destination);
		MessageConsumer slowConsumerAdvisory = session.createConsumer(slowConsumerAdvisoryTopic);
		
		//publish 2 messages
		Message txtMessage = session.createTextMessage("Sample Text Message");
		for(int i= 0; i<2; i++){
			producer.send(txtMessage);
		}
		
		//consume 2 messages
		for(int i= 0; i<2; i++){
			Message receivedMsg = consumer.receive(100);
			Assert.assertNotNull("received msg "+i+" should not be null",receivedMsg);
		}

		//check for "slow consumer" advisory message
		Message slowAdvisoryMessage = slowConsumerAdvisory.receive(100);
		Assert.assertNull( "should not have received a slow consumer advisory message",slowAdvisoryMessage);
		
	}

	

	public void tearDown() throws Exception {
		consumer.close();
		producer.close();
		session.close();
		connection.close();
		brokerService.stop();
	}
	
	
	//helper method to create a broker with slow consumer advisory turned on
	private BrokerService createBroker() throws Exception {
		BrokerService broker = new BrokerService();
		broker.setBrokerName("localhost");
		broker.setUseJmx(true);
		broker.setDeleteAllMessagesOnStartup(true);
		broker.addConnector("vm://localhost");

		PolicyMap policyMap = new PolicyMap();
		PolicyEntry defaultEntry = new PolicyEntry();
		defaultEntry.setAdvisoryForSlowConsumers(true);

		policyMap.setDefaultEntry(defaultEntry);

		broker.setDestinationPolicy(policyMap);
		broker.start();
		broker.waitUntilStarted();
		return broker;
	}

}
