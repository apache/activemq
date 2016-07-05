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

import static org.junit.Assert.assertFalse;

import java.io.InterruptedIOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5426Test {

	private static final Logger LOG = LoggerFactory
			.getLogger(AMQ5426Test.class);

	private BrokerService brokerService;
	private String connectionUri;
	private AtomicBoolean hasFailureInProducer = new AtomicBoolean(false);
	private Thread producerThread;
	private AtomicBoolean hasErrorInLogger;
	private Appender errorDetectorAppender;

	protected ConnectionFactory createConnectionFactory() throws Exception {
		ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(
				connectionUri);
		conFactory.setWatchTopicAdvisories(false);
		conFactory.setOptimizeAcknowledge(true);
		return conFactory;
	}

	@Before
	public void setUp() throws Exception {
		hasFailureInProducer = new AtomicBoolean(false);
		hasErrorInLogger = new AtomicBoolean(false);
		brokerService = BrokerFactory.createBroker(new URI(
				"broker://()/localhost?persistent=false&useJmx=true"));

		PolicyEntry policy = new PolicyEntry();
		policy.setTopicPrefetch(100);
		PolicyMap pMap = new PolicyMap();
		pMap.setDefaultEntry(policy);
		brokerService.addConnector("tcp://0.0.0.0:0");
		brokerService.start();
		connectionUri = brokerService.getTransportConnectorByScheme("tcp")
				.getPublishableConnectString();

		// Register an error listener to LOG4J
		// The NPE will not be detectable as of V5.10 from
		// ActiveMQConnection.setClientInternalExceptionListener
		// since ActiveMQMessageConsumer.dispatch will silently catch and
		// discard any RuntimeException
		errorDetectorAppender = new AppenderSkeleton() {
			@Override
			public void close() {
				// Do nothing
			}

			@Override
			public boolean requiresLayout() {
				return false;
			}

			@Override
			protected void append(LoggingEvent event) {
				if (event.getLevel().isGreaterOrEqual(Level.ERROR))
					hasErrorInLogger.set(true);
			}
		};

		org.apache.log4j.Logger.getRootLogger().addAppender(errorDetectorAppender);
		producerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Connection connection = createConnectionFactory()
							.createConnection();
					connection.start();
					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					Topic destination = session.createTopic("test.AMQ5426");
					LOG.debug("Created topic: {}", destination);
					MessageProducer producer = session.createProducer(destination);
					producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
					producer.setTimeToLive(1000);
					LOG.debug("Created producer: {}", producer);

					int i = 1;
					while (!Thread.interrupted()) {
						try {
							TextMessage msg = session.createTextMessage(" testMessage " + i);
							producer.send(msg);
							try {
								// Sleep for some nano seconds
								Thread.sleep(0, 100);
							} catch (InterruptedException e) {
								// Restore the interrupt
								Thread.currentThread().interrupt();
							}
							LOG.debug("message sent: {}", i);
							i++;
						} catch (JMSException e) {
							// Sometimes, we will gt a JMSException with nested
							// InterruptedIOException when we interrupt the thread
							if (!(e.getCause() != null && e.getCause() instanceof InterruptedIOException)) {
								throw e;
							}
						}
					}

					producer.close();
					session.close();
					connection.close();
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
					hasFailureInProducer.set(true);
				}
			}
		});

		producerThread.start();
	}

	@Test(timeout = 2 * 60 * 1000)
	public void testConsumerProperlyClosedWithoutError() throws Exception {
		Random rn = new Random();

		final int NUMBER_OF_RUNS = 1000;

		for (int run = 0; run < NUMBER_OF_RUNS; run++) {
			final AtomicInteger numberOfMessagesReceived = new AtomicInteger(0);
			LOG.info("Starting run {} of {}", run, NUMBER_OF_RUNS);

			// Starts a consumer
			Connection connection = createConnectionFactory().createConnection();
			connection.start();

			Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
			Topic destination = session.createTopic("test.AMQ5426");

			LOG.debug("Created topic: {}", destination);
			MessageConsumer consumer = session.createConsumer(destination);
			consumer.setMessageListener(new MessageListener() {

				@Override
				public void onMessage(Message message) {
					LOG.debug("Received message");
					numberOfMessagesReceived.getAndIncrement();
				}
			});
			LOG.debug("Created consumer: {}", consumer);

			try {
				// Sleep for a random time
				Thread.sleep(rn.nextInt(5) + 1);
			} catch (InterruptedException e) {
				// Restore the interrupt
				Thread.currentThread().interrupt();
			}

			// Close the consumer
			LOG.debug("Closing consumer");
			consumer.close();
			session.close();
			connection.close();

			assertFalse("Exception in Producer Thread", hasFailureInProducer.get());
			assertFalse("Error detected in Logger", hasErrorInLogger.get());
			LOG.info("Run {} of {} completed, message received: {}", run,
					NUMBER_OF_RUNS, numberOfMessagesReceived.get());
		}
	}

	@After
	public void tearDown() throws Exception {
		// Interrupt the producer thread
		LOG.info("Shutdown producer thread");
		producerThread.interrupt();
		producerThread.join();
		brokerService.stop();
		brokerService.waitUntilStopped();

		assertFalse("Exception in Producer Thread", hasFailureInProducer.get());
		assertFalse("Error detected in Logger", hasErrorInLogger.get());
	}
}
