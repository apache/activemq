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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.TestSupport;
import org.apache.activemq.command.ActiveMQQueue;

/**
 * In CLIENT_ACKNOWLEDGE and INDIVIDUAL_ACKNOWLEDGE modes following exception
 * occurs when ASYNCH consumers acknowledges messages in not in order they
 * received the messages.
 * <p>
 * Exception thrown on broker side:
 * <p>
 * {@code javax.jms.JMSException: Could not correlate acknowledgment with
 * dispatched message: MessageAck}
 * 
 * @author daroo
 */
public class AMQ2489Test extends TestSupport {
    private final static String SEQ_NUM_PROPERTY = "seqNum";

    private final static int TOTAL_MESSAGES_CNT = 2;
    private final static int CONSUMERS_CNT = 2;

    private final CountDownLatch LATCH = new CountDownLatch(TOTAL_MESSAGES_CNT);

    private Connection connection;

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    public void testUnorderedClientAcknowledge() throws Exception {
        doUnorderedAck(Session.CLIENT_ACKNOWLEDGE);
    }

    public void testUnorderedIndividualAcknowledge() throws Exception {
        doUnorderedAck(ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
    }

    /**
     * Main test method
     * 
     * @param acknowledgmentMode
     *            - ACK mode to be used by consumers
     * @throws Exception
     */
    protected void doUnorderedAck(int acknowledgmentMode) throws Exception {
        List<Consumer> consumers = null;
        Session producerSession = null;

        connection.start();
        // Because exception is thrown on broker side only, let's set up
        // exception listener to get it
        final TestExceptionListener exceptionListener = new TestExceptionListener();
        connection.setExceptionListener(exceptionListener);
        try {
            consumers = new ArrayList<Consumer>();
            // start customers
            for (int i = 0; i < CONSUMERS_CNT; i++) {
                consumers.add(new Consumer(acknowledgmentMode));
            }

            // produce few test messages
            producerSession = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = producerSession
                    .createProducer(new ActiveMQQueue(getQueueName()));
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < TOTAL_MESSAGES_CNT; i++) {
                final Message message = producerSession
                        .createTextMessage("test");
                // assign each message sequence number
                message.setIntProperty(SEQ_NUM_PROPERTY, i);
                producer.send(message);
            }

            // during each onMessage() calls consumers decreases the LATCH
            // counter.
            // 
            // so, let's wait till all messages are consumed.
            //
            LATCH.await();

            // wait a bit more to give exception listener a chance be populated
            // with
            // broker's error
            TimeUnit.SECONDS.sleep(1);

            assertFalse(exceptionListener.getStatusText(), exceptionListener.hasExceptions());

        } finally {
            if (producerSession != null)
                producerSession.close();

            if (consumers != null) {
                for (Consumer c : consumers) {
                    c.close();
                }
            }
        }
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

    public final class Consumer implements MessageListener {
        final Session session;

        private Consumer(int acknowledgmentMode) {
            try {
                session = connection.createSession(false, acknowledgmentMode);
                final Queue queue = session.createQueue(getQueueName()
                        + "?consumer.prefetchSize=1");
                final MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(this);
            } catch (JMSException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public void onMessage(Message message) {
            try {
                // retrieve sequence number assigned by producer...
                final int seqNum = message.getIntProperty(SEQ_NUM_PROPERTY);

                // ...and let's delay every second message a little bit before
                // acknowledgment
                if ((seqNum % 2) == 0) {
                    System.out.println("Delayed message sequence numeber: "
                            + seqNum);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                message.acknowledge();
            } catch (JMSException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                // decrease LATCH counter in the main test method.
                LATCH.countDown();
            }
        }

        private void close() {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public final class TestExceptionListener implements ExceptionListener {
        private final java.util.Queue<Exception> exceptions = new ConcurrentLinkedQueue<Exception>();

        public void onException(JMSException e) {
            exceptions.add(e);
        }

        public boolean hasExceptions() {
            return exceptions.isEmpty() == false;
        }

        public String getStatusText() {
            final StringBuilder str = new StringBuilder();
            str.append("Exceptions count on broker side: " + exceptions.size()
                    + ".\nMessages:\n");
            for (Exception e : exceptions) {
                str.append(e.getMessage() + "\n\n");
            }
            return str.toString();
        }
    }
}
