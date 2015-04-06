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
package org.apache.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class JmsConsumerResetActiveListenerTest {

    private Connection connection;
    private ActiveMQConnectionFactory factory;

    @Rule
    public final TestName name = new TestName();

    @Before
    public void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false&broker.useJmx=false");
        connection = factory.createConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * verify the (undefined by spec) behaviour of setting a listener while receiving a message.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSetListenerFromListener() throws Exception {
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination dest = session.createQueue("Queue-" + name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(dest);

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicBoolean first = new AtomicBoolean(true);
        final Vector<Object> results = new Vector<Object>();
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                if (first.compareAndSet(true, false)) {
                    try {
                        consumer.setMessageListener(this);
                        results.add(message);
                    } catch (JMSException e) {
                        results.add(e);
                    }
                } else {
                    results.add(message);
                }
                latch.countDown();
            }
        });

        connection.start();

        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("First"));
        producer.send(session.createTextMessage("Second"));

        assertTrue("we did not timeout", latch.await(5, TimeUnit.SECONDS));

        assertEquals("we have a result", 2, results.size());
        Object result = results.get(0);
        assertTrue(result instanceof TextMessage);
        assertEquals("result is first", "First", ((TextMessage)result).getText());
        result = results.get(1);
        assertTrue(result instanceof TextMessage);
        assertEquals("result is first", "Second", ((TextMessage)result).getText());
    }

    /**
     * and a listener on a new consumer, just in case.
      *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testNewConsumerSetListenerFromListener() throws Exception {
        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Destination dest = session.createQueue("Queue-" + name.getMethodName());
        final MessageConsumer consumer = session.createConsumer(dest);

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicBoolean first = new AtomicBoolean(true);
        final Vector<Object> results = new Vector<Object>();
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                if (first.compareAndSet(true, false)) {
                    try {
                        MessageConsumer anotherConsumer = session.createConsumer(dest);
                        anotherConsumer.setMessageListener(this);
                        results.add(message);
                    } catch (JMSException e) {
                        results.add(e);
                    }
                } else {
                    results.add(message);
                }
                latch.countDown();
            }
        });

        connection.start();

        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("First"));
        producer.send(session.createTextMessage("Second"));

        assertTrue("we did not timeout", latch.await(5, TimeUnit.SECONDS));

        assertEquals("we have a result", 2, results.size());
        Object result = results.get(0);
        assertTrue(result instanceof TextMessage);
        assertEquals("result is first", "First", ((TextMessage)result).getText());
        result = results.get(1);
        assertTrue(result instanceof TextMessage);
        assertEquals("result is first", "Second", ((TextMessage)result).getText());
    }
 }
