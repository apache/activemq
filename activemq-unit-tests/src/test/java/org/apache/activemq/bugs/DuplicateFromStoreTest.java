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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class DuplicateFromStoreTest {
    static Logger LOG = LoggerFactory.getLogger(DuplicateFromStoreTest.class);
    String activemqURL;
    BrokerService broker;

    protected final static String DESTNAME = "TEST";
    protected final static int NUM_PRODUCERS = 100;
    protected final static int NUM_CONSUMERS = 20;

    protected final static int NUM_MSGS = 20000;
    protected final static int CONSUMER_SLEEP = 0;
    protected final static int PRODUCER_SLEEP = 10;

    public static CountDownLatch producersFinished = new CountDownLatch(NUM_PRODUCERS);
    public static CountDownLatch consumersFinished = new CountDownLatch(NUM_CONSUMERS );

    public AtomicInteger totalMessagesToSend = new AtomicInteger(NUM_MSGS);
    public AtomicInteger totalMessagesSent = new AtomicInteger(NUM_MSGS);

    public AtomicInteger totalReceived = new AtomicInteger(0);

    public int messageSize = 16*1000;


    @Before
    public void startBroker() throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://0.0.0.0:0");

        // Create <policyEntry>
        PolicyEntry policy = new PolicyEntry();
        ActiveMQDestination dest = new ActiveMQQueue(">");
        policy.setDestination(dest);
        policy.setMemoryLimit(10 * 1024 * 1024); // 10 MB
        policy.setExpireMessagesPeriod(0);
        policy.setEnableAudit(false); // allow any duplicates from the store to bubble up to the q impl
        policy.setQueuePrefetch(100);
        PolicyMap policies = new PolicyMap();
        policies.put(dest, policy);
        broker.setDestinationPolicy(policies);

        // configure <systemUsage>
        MemoryUsage memoryUsage = new MemoryUsage();
        memoryUsage.setPercentOfJvmHeap(50);

        StoreUsage storeUsage = new StoreUsage();
        storeUsage.setLimit(8 * 1024 * 1024 * 1024); // 8 gb

        SystemUsage memoryManager = new SystemUsage();
        memoryManager.setMemoryUsage(memoryUsage);
        memoryManager.setStoreUsage(storeUsage);
        broker.setSystemUsage(memoryManager);

        // configure KahaDB persistence
        PersistenceAdapter kahadb = new KahaDBStore();
        ((KahaDBStore) kahadb).setConcurrentStoreAndDispatchQueues(true);
        broker.setPersistenceAdapter(kahadb);

        // start broker
        broker.start();
        broker.waitUntilStarted();

        activemqURL = broker.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    public void testDuplicateMessage() throws Exception {
        LOG.info("Testing for duplicate messages.");

        //create producer and consumer threads
        ExecutorService producers = Executors.newFixedThreadPool(NUM_PRODUCERS);
        ExecutorService consumers = Executors.newFixedThreadPool(NUM_CONSUMERS);

        createOpenwireClients(producers, consumers);

        LOG.info("All producers and consumers got started. Awaiting their termination");
        producersFinished.await(100, TimeUnit.MINUTES);
        LOG.info("All producers have terminated. remaining to send: " + totalMessagesToSend.get() + ", sent:" + totalMessagesSent.get());

        consumersFinished.await(100, TimeUnit.MINUTES);
        LOG.info("All consumers have terminated.");

        producers.shutdownNow();
        consumers.shutdownNow();

        assertEquals("no messages pending, i.e. dlq empty", 0l, ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics().getMessages().getCount());

        // validate cache can be enabled if disabled

    }


    protected void createOpenwireClients(ExecutorService producers, ExecutorService consumers) {
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            LOG.trace("Creating consumer for destination " + DESTNAME);
            Consumer consumer = new Consumer(DESTNAME, false);
            consumers.submit(consumer);
            // wait for consumer to signal it has fully initialized
            synchronized(consumer.init) {
                try {
                    consumer.init.wait();
                } catch (InterruptedException e) {
                    LOG.error(e.toString(), e);
                }
            }
        }

        for (int i = 0; i < NUM_PRODUCERS; i++) {
            LOG.trace("Creating producer for destination " + DESTNAME );
            Producer producer = new Producer(DESTNAME, false, 0);
            producers.submit(producer);
        }
    }

    class Producer implements Runnable {

        Logger log = LOG;
        protected String destName = "TEST";
        protected boolean isTopicDest = false;


        public Producer(String dest, boolean isTopic, int ttl) {
            this.destName = dest;
            this.isTopicDest = isTopic;
        }


        /**
         * Connect to broker and constantly send messages
         */
        public void run() {

            Connection connection = null;
            Session session = null;
            MessageProducer producer = null;

            try {
                ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(activemqURL);
                connection = amq.createConnection();

                connection.setExceptionListener(new javax.jms.ExceptionListener() {
                    public void onException(javax.jms.JMSException e) {
                        e.printStackTrace();
                    }
                });
                connection.start();

                // Create a Session
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination;
                if (isTopicDest) {
                    // Create the destination (Topic or Queue)
                    destination = session.createTopic(destName);
                } else {
                    destination = session.createQueue(destName);
                }
                // Create a MessageProducer from the Session to the Topic or Queue
                producer = session.createProducer(destination);

                // Create message
                long counter = 0;
                //enlarge msg to 16 kb
                int msgSize = 16 * 1024;
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.setLength(msgSize + 15);
                stringBuilder.append("Message: ");
                stringBuilder.append(counter);
                for (int j = 0; j < (msgSize / 10); j++) {
                    stringBuilder.append("XXXXXXXXXX");
                }
                String text = stringBuilder.toString();
                TextMessage message = session.createTextMessage(text);

                // send message
                while (totalMessagesToSend.decrementAndGet() >= 0) {
                    producer.send(message);
                    totalMessagesSent.incrementAndGet();
                    log.debug("Sent message: " + counter);
                    counter++;

                    if ((counter % 10000) == 0)
                        log.info("sent " + counter + " messages");

                    Thread.sleep(PRODUCER_SLEEP);
                }
            } catch (Exception ex) {
                log.error(ex.toString());
                return;
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception ignored) {
                } finally {
                    producersFinished.countDown();
                }
            }
            log.debug("Closing producer for " + destName);
        }
    }

    class Consumer implements Runnable {

        public Object init = new Object();
        protected String queueName = "TEST";
        boolean isTopic = false;

        Logger log = LOG;

        public Consumer(String destName, boolean topic) {
            this.isTopic = topic;
            this.queueName = destName;
        }

        /**
         * connect to broker and receive messages
         */
        public void run() {

            Connection connection = null;
            Session session = null;
            MessageConsumer consumer = null;

            try {
                ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(activemqURL);
                connection = amq.createConnection();
                connection.setExceptionListener(new javax.jms.ExceptionListener() {
                    public void onException(javax.jms.JMSException e) {
                        e.printStackTrace();
                    }
                });
                connection.start();
                // Create a Session
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                // Create the destination (Topic or Queue)
                Destination destination = null;
                if (isTopic)
                    destination = session.createTopic(queueName);
                else
                    destination = session.createQueue(queueName);

                //Create a MessageConsumer from the Session to the Topic or Queue
                consumer = session.createConsumer(destination);

                synchronized (init) {
                    init.notifyAll();
                }

                // Wait for a message
                long counter = 0;
                while (totalReceived.get() < NUM_MSGS) {
                    Message message2 = consumer.receive(5000);

                    if (message2 instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message2;
                        String text = textMessage.getText();
                        log.debug("Received: " + text.substring(0, 50));
                    } else if (totalReceived.get() < NUM_MSGS) {
                        log.error("Received message of unsupported type. Expecting TextMessage. count: " + totalReceived.get());
                    } else {
                        // all done
                        break;
                    }
                    if (message2 != null) {
                        counter++;
                        totalReceived.incrementAndGet();
                        if ((counter % 10000) == 0)
                            log.info("received " + counter + " messages");

                        Thread.sleep(CONSUMER_SLEEP);
                    }
                }
            } catch (Exception e) {
                log.error("Error in Consumer: " + e.getMessage());
                return;
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception ignored) {
                } finally {
                    consumersFinished.countDown();
                }
            }
        }
    }
}
