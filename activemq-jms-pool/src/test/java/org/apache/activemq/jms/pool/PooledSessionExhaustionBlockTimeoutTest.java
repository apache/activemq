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
package org.apache.activemq.jms.pool;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.log4j.Logger;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PooledSessionExhaustionBlockTimeoutTest extends TestCase {
    private static final String QUEUE = "FOO";
    private static final int NUM_MESSAGES = 500;

    private Logger logger = Logger.getLogger(getClass());

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;
    private int numReceived = 0;
    private final List<Exception> exceptionList = new ArrayList<Exception>();


    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
        pooledFactory.setBlockIfSessionPoolIsFull(true);
        pooledFactory.setBlockIfSessionPoolIsFullTimeout(500);
        pooledFactory.setMaximumActiveSessionPerConnection(1);
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = null;
    }

    class TestRunner implements Runnable {

        CyclicBarrier barrier;
        CountDownLatch latch;
        TestRunner(CyclicBarrier barrier, CountDownLatch latch) {
            this.barrier = barrier;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                barrier.await();
                sendMessages(pooledFactory);
                this.latch.countDown();
            } catch (Exception e) {
                exceptionList.add(e);
                throw new RuntimeException(e);
            }
        }
    }

    public void sendMessages(ConnectionFactory connectionFactory) throws Exception {
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE);
            MessageProducer producer = session.createProducer(destination);

            String msgTo = "hello";
            TextMessage message = session.createTextMessage(msgTo);
            producer.send(message);
            connection.close();
            logger.info("sent " + i + " messages using " + connectionFactory.getClass());
        }
    }

    public void testCanExhaustSessions() throws Exception {
        final int totalMessagesExpected =  NUM_MESSAGES * 2;
        final CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);
                    Connection connection = connectionFactory.createConnection();
                    connection.start();

                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = session.createQueue(QUEUE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    for (int i = 0; i < totalMessagesExpected; ++i) {
                        Message msg = consumer.receive(5000);
                        if (msg == null) {
                            return;
                        }
                        numReceived++;
                        if (numReceived % 20 == 0) {
                            logger.debug("received " + numReceived + " messages ");
                            System.runFinalization();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();

        ExecutorService threads = Executors.newFixedThreadPool(2);
        final CyclicBarrier barrier = new CyclicBarrier(2, new Runnable() {

            @Override
            public void run() {
                System.out.println("Starting threads to send messages!");
            }
        });

        threads.execute(new TestRunner(barrier, latch));
        threads.execute(new TestRunner(barrier, latch));

        latch.await(2, TimeUnit.SECONDS);
        thread.join();

        assertEquals(totalMessagesExpected, numReceived);

    }
}
