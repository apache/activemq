/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs;


import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;

public class AMQ6094Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ6094Test.class);

    private BrokerService brokerService;
    private String connectionUri;

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        connectionUri = connector.getPublishableConnectString();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
	public void testQueueMemoryUsage() throws Exception {

        final ArrayList<ThreadSlot> producerThreads = new ArrayList<>();

		final ArrayList<ThreadSlot> consumerThreads = new ArrayList<>();

		for (int i = 0; i < 4; i++)
			producerThreads.add(runInThread(new UnsafeRunnable() {

                @Override
                public void run() throws Exception {
                    producer(connectionUri, "queueA");
                }

			}));

		for (int i = 0; i < 4; i++)
			consumerThreads.add(runInThread(new UnsafeRunnable() {

                @Override
                public void run() throws Exception {
                    consumer(connectionUri, "queueA", 2500);
                }
            }));

		// kill and restart random threads
		for (int count = 0; count < 10; count++) {
			Thread.sleep(5000);
			final int i = (int) (Math.random() * consumerThreads.size());
			final ThreadSlot slot = consumerThreads.get(i);
			slot.thread.interrupt();
			consumerThreads.remove(i);
			consumerThreads.add(runInThread(slot.runnable));

			Queue queue = (Queue) brokerService.getDestination(new ActiveMQQueue("queueA"));
			LOG.info("cursorMemoryUsage: " + queue.getMessages().
			        getSystemUsage().getMemoryUsage().getUsage());
            LOG.info("messagesStat: " + queue.getDestinationStatistics().getMessages().getCount());

		}

        // verify usage
        Queue queue = (Queue) brokerService.getDestination(new ActiveMQQueue("queueA"));
        LOG.info("cursorMemoryUsage: " + queue.getMessages().
                getSystemUsage().getMemoryUsage().getUsage());
        LOG.info("messagesStat: " + queue.getDestinationStatistics().getMessages().getCount());

        // drain the queue
        for (ThreadSlot threadSlot: producerThreads) {
            threadSlot.thread.interrupt();
            threadSlot.thread.join(4000);
        }

        for (ThreadSlot threadSlot : consumerThreads) {
            threadSlot.thread.interrupt();
            threadSlot.thread.join(4000);
        }

        consumer(connectionUri, "queueA", 2500, true);

        LOG.info("After drain, cursorMemoryUsage: " + queue.getMessages().
                getSystemUsage().getMemoryUsage().getUsage());
        LOG.info("messagesStat: " + queue.getDestinationStatistics().getMessages().getCount());

        assertEquals("Queue memory usage to 0", 0, queue.getMessages().
                getSystemUsage().getMemoryUsage().getUsage());

	}

	public static void producer(String uri, String topic) throws Exception {
		final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
		        uri + "?jms.useCompression=true&jms.useAsyncSend=true&daemon=true");

		Connection connection = factory.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(topic));

		producer.setTimeToLive(6000);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		while (true) {
			producer.send(session.createTextMessage(msg()));
			if (Math.random() > 0.5)
				Thread.sleep(1);
		}

	}

    public static void consumer(String uri, String queue, int prefetchSize) throws Exception {
        consumer(uri, queue, prefetchSize, false);
    }

	public static void consumer(String uri, String queue, int prefetchSize, boolean drain) throws Exception {
		final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
				uri + "?jms.prefetchPolicy.queuePrefetch=" + prefetchSize + "&jms.useAsyncSend=true");

		Connection connection = null;
		try {
			connection = factory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(queue));
            if (drain) {
                Message message = null;
                do {
                    message = consumer.receive(4000);
                } while (message != null);
            } else {
                // block
                while (true) {
                    consumer.receive();
                }
            }
		} finally {

            Thread.interrupted();
			if (!drain) {
                Thread.sleep(5000); // delay closing of connection
            }

			LOG.info("Now closing");
			if (connection != null)
				connection.close();
		}

	}

	private static String msg() {
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 100; i++)
			builder.append("123457890");

		return builder.toString();
	}

	private static interface UnsafeRunnable {
		public void run() throws Exception;
	}

	public static class ThreadSlot {
		private UnsafeRunnable runnable;
		private Thread thread;
	}

	public static ThreadSlot runInThread(final UnsafeRunnable runnable) {
		final Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (Exception e) {
                    //e.printStackTrace();
                }

            }
        });

		thread.start();

		final ThreadSlot result = new ThreadSlot();
		result.thread = thread;
		result.runnable = runnable;
		return result;
	}
}
