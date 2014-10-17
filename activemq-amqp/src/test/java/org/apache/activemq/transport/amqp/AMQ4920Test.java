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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4920Test extends AmqpTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4920Test.class);
    private static final Integer ITERATIONS = 1 * 1000;
    private static final Integer CONSUMER_COUNT = 4;  // At least 2 consumers are required to reproduce the original issue
    public static final String TEXT_MESSAGE = "TextMessage: ";
    private final CountDownLatch latch = new CountDownLatch(CONSUMER_COUNT * ITERATIONS);
    private final CountDownLatch initLatch = new CountDownLatch(CONSUMER_COUNT);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test(timeout = 1 * 60 * 1000)
    public void testSendWithMultipleConsumers() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl("localhost", port, "admin", "admin");
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String destinationName = "topic://AMQ4920Test" + System.currentTimeMillis();
        Destination destination = session.createTopic(destinationName);
        connection.start();

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i=0; i < CONSUMER_COUNT; i++) {
            AMQ4930ConsumerTask consumerTask =
                new AMQ4930ConsumerTask(initLatch, destinationName, port, "Consumer-" + i, latch, ITERATIONS);
            executor.submit(consumerTask);
        }
        connection.start();

        // Make sure at least Topic consumers are subscribed before the first send.
        initLatch.await();

        LOG.debug("At start latch is " + latch.getCount());
        sendMessages(connection, destination, ITERATIONS, 10);
        LOG.debug("After send latch is " + latch.getCount());

        latch.await(15, TimeUnit.SECONDS);
        LOG.debug("After await latch is " + latch.getCount());
        assertEquals(0, latch.getCount());

        executor.shutdown();
    }

    public void sendMessages(Connection connection, Destination destination, int count, int sleepInterval) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);

        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage();
            message.setText(TEXT_MESSAGE + i);
            LOG.debug("Sending message [" + i + "]");
            producer.send(message);
            if (sleepInterval > 0) {
                Thread.sleep(sleepInterval);
            }
        }

        session.close();
    }
}

class AMQ4930ConsumerTask implements Callable<Boolean> {
    protected static final Logger LOG = LoggerFactory.getLogger(AMQ4930ConsumerTask.class);
    private final String destinationName;
    private final String consumerName;
    private final CountDownLatch messagesReceived;
    private final int port;
    private final int expectedMessageCount;
    private final CountDownLatch started;

    public AMQ4930ConsumerTask (CountDownLatch started, String destinationName, int port, String consumerName, CountDownLatch latch, int expectedMessageCount) {
        this.started = started;
        this.destinationName = destinationName;
        this.port = port;
        this.consumerName = consumerName;
        this.messagesReceived = latch;
        this.expectedMessageCount = expectedMessageCount;
    }

    @Override
    public Boolean call() throws Exception {
        LOG.debug(consumerName + " starting");
        Connection connection=null;
        try {
            ConnectionFactory connectionFactory = new ConnectionFactoryImpl("localhost", port, "admin", "admin");
            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(destinationName);
            MessageConsumer consumer = session.createConsumer(destination);
            connection.start();

            started.countDown();

            int receivedCount = 0;
            while(receivedCount < expectedMessageCount) {
                Message message = consumer.receive(5 * 1000);
                if (message == null) {
                    LOG.error("consumer {} got null message on iteration {}", consumerName, receivedCount);
                    return false;
                }
                if (!(message instanceof TextMessage)) {
                    LOG.error("consumer {} expected text message on iteration {} but got {}", consumerName, receivedCount, message.getClass().getCanonicalName());
                    return false;
                }
                TextMessage tm = (TextMessage) message;
                if (!tm.getText().equals(AMQ4920Test.TEXT_MESSAGE + receivedCount)) {
                    LOG.error("consumer {} expected {} got message [{}]", consumerName, receivedCount, tm.getText());
                    return false;
                }
                LOG.debug("consumer {} expected {} got message [{}]", consumerName, receivedCount, tm.getText());  // TODO make debug

                messagesReceived.countDown();
                receivedCount++;
            }
        } catch (Exception e) {
            LOG.error("UnexpectedException in " + consumerName, e);
        } finally {
            try {
                connection.close();
            } catch (JMSException ignoreMe) {
            }
        }

        return true;
    }
}

