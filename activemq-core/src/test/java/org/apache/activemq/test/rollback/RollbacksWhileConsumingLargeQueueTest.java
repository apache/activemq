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
package org.apache.activemq.test.rollback;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.MessageCreator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version $Revision$
 */
public class RollbacksWhileConsumingLargeQueueTest extends EmbeddedBrokerTestSupport implements MessageListener {

    private static final transient Log log = LogFactory.getLog(RollbacksWhileConsumingLargeQueueTest.class);

    protected int numberOfMessagesOnQueue = 6500;
    private Connection connection;
    private AtomicInteger deliveryCounter = new AtomicInteger(0);
    private AtomicInteger ackCounter = new AtomicInteger(0);
    private CountDownLatch latch;
    private Throwable failure;

    public void testWithReciever() throws Throwable {
        latch = new CountDownLatch(numberOfMessagesOnQueue);
        Session session = connection.createSession(true, 0);
        MessageConsumer consumer = session.createConsumer(destination);

        long start = System.currentTimeMillis();
        while ((System.currentTimeMillis() - start) < 1000 * 1000) {
            if (getFailure() != null) {
                throw getFailure();
            }

            // Are we done receiving all the messages.
            if (ackCounter.get() == numberOfMessagesOnQueue)
                return;

            Message message = consumer.receive(1000);
            if (message == null)
                continue;

            try {
                onMessage(message);
                session.commit();
            } catch (Throwable e) {
                session.rollback();
            }
        }

        fail("Did not receive all the messages.");
    }

    public void testWithMessageListener() throws Throwable {
        latch = new CountDownLatch(numberOfMessagesOnQueue);
        new DelegatingTransactionalMessageListener(this, connection, destination);

        long start = System.currentTimeMillis();
        while ((System.currentTimeMillis() - start) < 1000 * 1000) {

            if (getFailure() != null) {
                throw getFailure();
            }

            if (latch.await(1, TimeUnit.SECONDS)) {
                log.debug("Received: " + deliveryCounter.get() + "  message(s)");
                return;
            }

        }

        fail("Did not receive all the messages.");
    }

    protected void setUp() throws Exception {
        super.setUp();

        connection = createConnection();
        connection.start();

        // lets fill the queue up
        for (int i = 0; i < numberOfMessagesOnQueue; i++) {
            template.send(createMessageCreator(i));
        }

    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected MessageCreator createMessageCreator(final int i) {
        return new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                TextMessage answer = session.createTextMessage("Message: " + i);
                answer.setIntProperty("Counter", i);
                return answer;
            }
        };
    }

    public void onMessage(Message message) {
        String msgId = null;
        String msgText = null;

        try {
            msgId = message.getJMSMessageID();
            msgText = ((TextMessage)message).getText();
        } catch (JMSException e) {
            setFailure(e);
        }

        try {
            assertEquals("Message: " + ackCounter.get(), msgText);
        } catch (Throwable e) {
            setFailure(e);
        }

        int value = deliveryCounter.incrementAndGet();
        if (value % 2 == 0) {
            log.info("Rolling Back message: " + ackCounter.get() + " id: " + msgId + ", content: " + msgText);
            throw new RuntimeException("Dummy exception on message: " + value);
        }

        log.info("Received message: " + ackCounter.get() + " id: " + msgId + ", content: " + msgText);
        ackCounter.incrementAndGet();
        latch.countDown();
    }

    public synchronized Throwable getFailure() {
        return failure;
    }

    public synchronized void setFailure(Throwable failure) {
        this.failure = failure;
    }
}
