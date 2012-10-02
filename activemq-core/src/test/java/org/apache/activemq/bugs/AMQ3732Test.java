/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3732Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ3732Test.class);

    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private BrokerService broker;
    private String connectionUri;

    private final Random pause = new Random();
    private final long NUM_MESSAGES = 25000;
    private final AtomicLong totalConsumed = new AtomicLong();

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        connectionFactory.getPrefetchPolicy().setAll(0);
    }

    @After
    public void stopBroker() throws Exception {
        connection.close();

        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 1200000)
    public void testInterruptionAffects() throws Exception {

        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

        Queue queue = session.createQueue("AMQ3732Test");

        final LinkedBlockingQueue<Message> workQueue = new LinkedBlockingQueue<Message>();

        final MessageConsumer consumer1 = session.createConsumer(queue);
        final MessageConsumer consumer2 = session.createConsumer(queue);
        final MessageProducer producer = session.createProducer(queue);

        Thread consumer1Thread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    while (totalConsumed.get() < NUM_MESSAGES) {
                        Message message = consumer1.receiveNoWait();
                        if (message != null) {
                            workQueue.add(message);
                        }
                    }
                } catch(Exception e) {
                    LOG.error("Caught an unexpected error: ", e);
                }
            }
        });
        consumer1Thread.start();

        Thread consumer2Thread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    while (totalConsumed.get() < NUM_MESSAGES) {
                        Message message = consumer2.receive(50);
                        if (message != null) {
                            workQueue.add(message);
                        }
                    }
                } catch(Exception e) {
                    LOG.error("Caught an unexpected error: ", e);
                }
            }
        });
        consumer2Thread.start();

        Thread producerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < NUM_MESSAGES; ++i) {
                        producer.send(session.createTextMessage("TEST"));
                        TimeUnit.MILLISECONDS.sleep(pause.nextInt(10));
                    }
                } catch(Exception e) {
                    LOG.error("Caught an unexpected error: ", e);
                }
            }
        });
        producerThread.start();

        Thread ackingThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    while (totalConsumed.get() < NUM_MESSAGES) {
                        Message message = workQueue.take();
                        message.acknowledge();
                        totalConsumed.incrementAndGet();
                        if ((totalConsumed.get() % 100) == 0) {
                            LOG.info("Consumed " + totalConsumed.get() + " messages so far.");
                        }
                    }
                } catch(Exception e) {
                    LOG.error("Caught an unexpected error: ", e);
                }
            }
        });
        ackingThread.start();

        producerThread.join();
        consumer1Thread.join();
        consumer2Thread.join();
        ackingThread.join();

        assertEquals(NUM_MESSAGES, totalConsumed.get());
    }
}
