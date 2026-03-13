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
package org.apache.activemq.command;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.MessageProducer;
import jakarta.jms.TextMessage;
import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertEquals;

public class ActiveMQTextMessageStressTest {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQTextMessageStressTest.class);
    private BrokerService broker;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        broker.start();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        connection = cf.createConnection();
        connection.setClientID("HIGH_CONC_TEST");
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    public void testConcurrentProducersAndConsumers() throws Exception {
        final int MESSAGE_COUNT = 50;
        final int PRODUCERS = 2;
        final int DURABLE_CONSUMERS = 2;
        final int NON_DURABLE_CONSUMERS = 2;
        final int TOTAL_CONSUMERS = DURABLE_CONSUMERS + NON_DURABLE_CONSUMERS;

        Session tmpSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = tmpSession.createTopic("HIGH_CONC.TOPIC");

        List<MessageConsumer> consumers = new ArrayList<>();
        List<Session> consumerSessions = new ArrayList<>();

        for (int i = 1; i <= DURABLE_CONSUMERS; i++) {
            Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumers.add(s.createDurableSubscriber(topic, "Durable-" + i));
            consumerSessions.add(s);
        }
        for (int i = 1; i <= NON_DURABLE_CONSUMERS; i++) {
            Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumers.add(s.createConsumer(topic));
            consumerSessions.add(s);
        }

        ExecutorService executor = Executors.newFixedThreadPool(PRODUCERS + TOTAL_CONSUMERS);
        CountDownLatch producerLatch = new CountDownLatch(PRODUCERS);

        // Producers
        for (int p = 1; p <= PRODUCERS; p++) {
            final int producerId = p;
            executor.submit(() -> {
                try {
                    Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = s.createProducer(topic);
                    for (int m = 1; m <= MESSAGE_COUNT; m++) {
                        TextMessage msg = s.createTextMessage("P" + producerId + "-M" + m);
                        producer.send(msg);
                    }
                    s.close();
                } catch (JMSException e) {
                    LOG.error("Producer error", e);
                } finally {
                    producerLatch.countDown();
                }
            });
        }

        // Consumers
        List<Future<List<TextMessage>>> consumerFutures = new ArrayList<>();
        for (MessageConsumer consumer : consumers) {
            consumerFutures.add(executor.submit(() -> {
                List<TextMessage> received = new ArrayList<>();
                try {
                    for (int i = 0; i < MESSAGE_COUNT * PRODUCERS; i++) {
                        TextMessage msg = (TextMessage) consumer.receive(10000);
                        assertNotNull("Consumer should receive a message", msg);

                        // Hammer the message to trigger race condition on unmarshalling
                        for (int j = 0; j < 10; j++) {
                            String txt = msg.getText();
                            assertNotNull("Text should never be null during stress", txt);
                            // Clear state to force unmarshalling on the next call
                            ((ActiveMQTextMessage) msg).clearUnMarshalledState();
                        }
                        received.add(msg);
                    }
                } catch (Exception e) {
                    LOG.error("Consumer error", e);
                }
                return received;
            }));
        }

        producerLatch.await(30, TimeUnit.SECONDS);

        List<List<TextMessage>> allConsumed = new ArrayList<>();
        for (Future<List<TextMessage>> f : consumerFutures) {
            allConsumed.add(f.get(30, TimeUnit.SECONDS));
        }

        // Validate independent instances and data integrity
        for (int i = 0; i < allConsumed.size(); i++) {
            List<TextMessage> consumerMsgs = allConsumed.get(i);
            assertEquals("Consumer " + i + " did not receive all messages", MESSAGE_COUNT * PRODUCERS, consumerMsgs.size());

            for (int j = i + 1; j < allConsumed.size(); j++) {
                List<TextMessage> otherMsgs = allConsumed.get(j);

                for (int k = 0; k < consumerMsgs.size(); k++) {
                    TextMessage m1 = consumerMsgs.get(k);
                    TextMessage m2 = otherMsgs.get(k);

                    assertNotSame("Message wrappers MUST be different instances across consumers", m1, m2);
                    assertEquals("Content must match", m1.getText(), m2.getText());
                    assertNotNull("Content should not be null", m1.getText());
                }
            }
        }

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        for (Session s : consumerSessions) {
            s.close();
        }
        tmpSession.close();
    }
}