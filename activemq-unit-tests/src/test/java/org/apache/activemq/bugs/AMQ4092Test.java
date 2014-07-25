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

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4092Test extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(AMQ4092Test.class);

    static final String QUEUE_NAME = "TEST";

    // increase limits to expedite failure
    static final int NUM_TO_SEND_PER_PRODUCER = 1000; // 10000
    static final int NUM_PRODUCERS = 5; // 40

    static final ActiveMQQueue[] DESTINATIONS = new ActiveMQQueue[]{
            new ActiveMQQueue("A"),
            new ActiveMQQueue("B")
            // A/B seems to be sufficient for concurrentStoreAndDispatch=true
    };

    static final boolean debug = false;

    private BrokerService brokerService;

    private ActiveMQQueue destination;
    private HashMap<Thread, Throwable> exceptions = new HashMap<Thread, Throwable>();
    private ExceptionListener exceptionListener = new ExceptionListener() {
        @Override
        public void onException(JMSException exception) {
            exception.printStackTrace();
            exceptions.put(Thread.currentThread(), exception);
        }
    };

    @Override
    protected void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        ((KahaDBPersistenceAdapter)brokerService.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        destination = new ActiveMQQueue();
        destination.setCompositeDestinations(DESTINATIONS);
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                exceptions.put(t, e);
            }
        });
    }

    @Override
    protected void tearDown() throws Exception {
        // Stop any running threads.
        brokerService.stop();
    }


    public void testConcurrentGroups() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new TestConsumer());
        for (int i=0; i<NUM_PRODUCERS; i++) {
            executorService.submit(new TestProducer());
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    
    class TestProducer implements Runnable {

        public void produceMessages() throws Exception {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    brokerService.getTransportConnectors().get(0).getConnectUri().toString()
            );
            connectionFactory.setExceptionListener(exceptionListener);
            connectionFactory.setUseAsyncSend(true);
            Connection connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

                String name = new String(new byte[2*1024]);
                for (int i = 1; i <= NUM_TO_SEND_PER_PRODUCER; i++) {

                    TextMessage message = session.createTextMessage(name + "_" + i);
                    for (int j=0; j<100; j++) {
                        message.setStringProperty("Prop" + j, ""+j);
                    }
                    message.setStringProperty("JMSXGroupID", Thread.currentThread().getName()+i);
                    message.setIntProperty("JMSXGroupSeq", 1);
                    producer.send(message);
                }

            producer.close();
            session.close();
            connection.close();
        }

        @Override
        public void run() {
            try {
                produceMessages();
            } catch (Exception e) {
                e.printStackTrace();
                exceptions.put(Thread.currentThread(), e);
            }
        }
    }

    class TestConsumer implements Runnable {

        private CountDownLatch finishLatch = new CountDownLatch(1);



        public void consume() throws Exception {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    brokerService.getTransportConnectors().get(0).getConnectUri().toString()
            );

            connectionFactory.setExceptionListener(exceptionListener);
            final int totalMessageCount = NUM_TO_SEND_PER_PRODUCER * DESTINATIONS.length * NUM_PRODUCERS;
            final AtomicInteger counter = new AtomicInteger();
            final MessageListener listener = new MessageListener() {
                public void onMessage(Message message) {

                    if (debug) {
                        try {
                            log.info(((TextMessage) message).getText());
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }

                    boolean first = false;
                    try {
                        first = message.getBooleanProperty("JMSXGroupFirstForConsumer");
                    } catch (JMSException e) {
                        e.printStackTrace();
                        exceptions.put(Thread.currentThread(), e);
                    }
                    assertTrue("Always is first message", first);
                    if (counter.incrementAndGet() == totalMessageCount) {
                            log.info("Got all:" + counter.get());
                            finishLatch.countDown();

                    }
                }
            };

            int consumerCount = DESTINATIONS.length * 100;
            Connection[] connections = new Connection[consumerCount];

            Session[] sessions = new Session[consumerCount];
            MessageConsumer[] consumers = new MessageConsumer[consumerCount];

            for (int i = 0; i < consumerCount; i++) {
                connections[i] = connectionFactory.createConnection();
                connections[i].start();

                sessions[i] = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);

                consumers[i] = sessions[i].createConsumer(DESTINATIONS[i%DESTINATIONS.length], null);
                consumers[i].setMessageListener(listener);
            }


            log.info("received " + counter.get() + " messages");

            assertTrue("got all messages in time", finishLatch.await(4, TimeUnit.MINUTES));

            log.info("received " + counter.get() + " messages");

            for (MessageConsumer consumer : consumers) {
                consumer.close();
            }

            for (Session session : sessions) {
                session.close();
            }

            for (Connection connection : connections) {
                connection.close();
            }
        }

        @Override
        public void run() {
            try {
                consume();
            } catch (Exception e) {
                e.printStackTrace();
                exceptions.put(Thread.currentThread(), e);
            }
        }
    }

}
