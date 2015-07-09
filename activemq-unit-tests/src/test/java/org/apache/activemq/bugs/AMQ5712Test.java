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

import java.io.File;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.util.BrokerSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This test will check that a deadlock does not occur if the memory storage
 * limit is greater than the temporary size when publishing non-persistent messages.
 * If this test is run before the patch is applied from AMQ-5712, 
 * the broker will deadlock when trying to clearPendingMessages and purge. 
 * This test runs successfully with the patch applied.
 * 
 * This test demonstrates this deadlock issue by publishing a bunch of messages until
 * the memory store fills up and dumps to disk (to temporary storage).  At that point
 * the temporary store is greater than 100% of its max size allowed.
 * In another thread, the messages are purged and a few seconds later the 
 * producer will continue sending until the loop is finished and the test 
 * ends successfully.  If this is run against 5.11.1, this test will time
 * out because the new thread that calls clearPendingMessages and purge 
 * in the deadlocks.
 * 
 */
public class AMQ5712Test {
	protected static final Logger LOG = LoggerFactory
			.getLogger(AMQ5712Test.class);

	File dataFileDir = new File("target/test-amq-5712/datadb");
	private BrokerService broker;
	private URI brokerConnectURI;

	private ScheduledExecutorService purgeEexecutorService = Executors
			.newScheduledThreadPool(1);

	@Before
	public void startBroker() throws Exception {
		broker = new BrokerService();
		broker.setPersistent(true);
		broker.setDataDirectoryFile(dataFileDir);
		
		final SystemUsage systemUsage = broker.getSystemUsage();
		final TempUsage tempUsage = new TempUsage();
		
		//Configure store limits
		//The key is that the memory usage has been configured
		//to be greater than the temporary usage limit which causes this problem
		tempUsage.setLimit(50000000);
		systemUsage.setTempUsage(tempUsage);
		systemUsage.setSendFailIfNoSpace(false);
		
		StoreUsage storeUsage = new StoreUsage();
		storeUsage.setLimit(80000000);
		systemUsage.setStoreUsage(storeUsage);

		//this is a lower amount than temp usage
		final MemoryUsage memoryUsage = new MemoryUsage();
		memoryUsage.setLimit(100000000);
		systemUsage.setMemoryUsage(memoryUsage);

		//set up a transport
		TransportConnector connector = broker
				.addConnector(new TransportConnector());
		connector.setUri(new URI("tcp://0.0.0.0:0"));
		connector.setName("tcp");

		broker.start();
		broker.waitUntilStarted();
		brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
	}

	@After
	public void stopBroker() throws Exception {
		broker.stop();
		broker.waitUntilStopped();
	}

	/**
	 * This test will timeout with a deadlock on the existing 5.11.1 code without
	 * this patch.  With the patch applied this works correctly.
	 * 
	 * @throws Exception
	 */
	@Test(timeout=90000)
	public void testMaximumProducersAllowedPerConnection() throws Exception {
		
		//create a new queue
		final ActiveMQDestination activeMqQueue = new ActiveMQQueue("test.queue");
		
		//set the policies for the test queue
		setUpPolicies(activeMqQueue);
		
		Destination d = broker.getDestination(activeMqQueue);
		final org.apache.activemq.broker.region.Queue internalQueue = (org.apache.activemq.broker.region.Queue) d;

		//Start the connection
		Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
		connection.start();
		Session session = connection.createSession(false,
				QueueSession.AUTO_ACKNOWLEDGE);
		Queue queue = session.createQueue("test.queue");

		//Schedule a task to clear pending and to purge after a few seconds
		//This is enough time to let the temporary store fill up
		purgeEexecutorService.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					LOG.info("Starting purge....");
					//broker deadlocks here without patch
					internalQueue.clearPendingMessages();
					internalQueue.purge();
					
					//this line will only print after the patch is applied
					LOG.info("Deleted all messages for queue:"
							+ internalQueue.getName());
				} catch (Exception e) {
					LOG.error(e.getMessage());
				} finally {
					
				}
			}
		}, 6, TimeUnit.SECONDS);

		
		try {
			//publish a bunch of non-persistent messages to fill up the temp store
			MessageProducer prod = session.createProducer(queue);
			prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			for (int i = 0; i < 200; i++) {
				LOG.info("Sending..." + i);
				prod.send(createMessage(session));
			}

		} finally {
			connection.stop();
		}
	}

	
	/**
	 * Configure policies for test
	 * 
	 * @param activeMqQueue
	 * @throws Exception
	 */
	protected void setUpPolicies(final ActiveMQDestination activeMqQueue) throws Exception {
		//Setup queue policy
		PolicyEntry queueEntry = new PolicyEntry();
		queueEntry.setCursorMemoryHighWaterMark(70);
		queueEntry.setExpireMessagesPeriod(30000);
		queueEntry.setDestination(activeMqQueue);
		
		broker.getBroker().addDestination(
                BrokerSupport.getConnectionContext(broker.getBroker()),
                activeMqQueue, false);

		//Setup default policy
		PolicyMap policyMap = new PolicyMap();
		PolicyEntry defaultEntry = new PolicyEntry();
		defaultEntry.setCursorMemoryHighWaterMark(70);
		defaultEntry.setExpireMessagesPeriod(30000);
		defaultEntry.setMaxPageSize(200);
		defaultEntry.setMaxAuditDepth(2048);
		defaultEntry.setMaxExpirePageSize(400);

		//set policy on the broker
		policyMap.setDefaultEntry(defaultEntry);
		policyMap.setPolicyEntries(Lists.newArrayList(queueEntry));
		broker.setDestinationPolicy(policyMap);
	}
	
	/**
	 * Generate random 1 megabyte messages
	 * @param session
	 * @return
	 * @throws JMSException
	 */
	protected BytesMessage createMessage(Session session) throws JMSException {
		final BytesMessage message = session.createBytesMessage();
		final byte[] data = new byte[1000000];
		final Random rng = new Random();
		rng.nextBytes(data);
		message.writeBytes(data);
		return message;
	}

}
