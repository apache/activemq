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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * Testcases to see if Session.recover() work.
 * 
 * @version $Revision: 1.3 $
 */
public class JmsSessionRecoverTest extends TestCase {

    private Connection connection;
    private ActiveMQConnectionFactory factory;
    private Destination dest;

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = factory.createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testQueueSynchRecover() throws JMSException, InterruptedException {
        dest = new ActiveMQQueue("Queue-" + System.currentTimeMillis());
        doTestSynchRecover();
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testQueueAsynchRecover() throws JMSException, InterruptedException {
        dest = new ActiveMQQueue("Queue-" + System.currentTimeMillis());
        doTestAsynchRecover();
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testTopicSynchRecover() throws JMSException, InterruptedException {
        dest = new ActiveMQTopic("Topic-" + System.currentTimeMillis());
        doTestSynchRecover();
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testTopicAsynchRecover() throws JMSException, InterruptedException {
        dest = new ActiveMQTopic("Topic-" + System.currentTimeMillis());
        doTestAsynchRecover();
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testQueueAsynchRecoverWithAutoAck() throws JMSException, InterruptedException {
        dest = new ActiveMQQueue("Queue-" + System.currentTimeMillis());
        doTestAsynchRecoverWithAutoAck();
    }

    /**
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void testTopicAsynchRecoverWithAutoAck() throws JMSException, InterruptedException {
        dest = new ActiveMQTopic("Topic-" + System.currentTimeMillis());
        doTestAsynchRecoverWithAutoAck();
    }

    /**
     * Test to make sure that a Sync recover works.
     * 
     * @throws JMSException
     */
    public void doTestSynchRecover() throws JMSException {
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(dest);
        connection.start();

        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("First"));
        producer.send(session.createTextMessage("Second"));

        TextMessage message = (TextMessage)consumer.receive(1000);
        assertEquals("First", message.getText());
        assertFalse(message.getJMSRedelivered());
        message.acknowledge();

        message = (TextMessage)consumer.receive(1000);
        assertEquals("Second", message.getText());
        assertFalse(message.getJMSRedelivered());

        session.recover();

        message = (TextMessage)consumer.receive(2000);
        assertEquals("Second", message.getText());
        assertTrue(message.getJMSRedelivered());

        message.acknowledge();
    }

    /**
     * Test to make sure that a Async recover works.
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void doTestAsynchRecover() throws JMSException, InterruptedException {

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final String errorMessage[] = new String[] {null};
        final CountDownLatch doneCountDownLatch = new CountDownLatch(1);

        MessageConsumer consumer = session.createConsumer(dest);

        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("First"));
        producer.send(session.createTextMessage("Second"));

        consumer.setMessageListener(new MessageListener() {
            int counter;

            public void onMessage(Message msg) {
                counter++;
                try {
                    TextMessage message = (TextMessage)msg;
                    switch (counter) {
                    case 1:
                        assertEquals("First", message.getText());
                        assertFalse(message.getJMSRedelivered());
                        message.acknowledge();

                        break;
                    case 2:
                        assertEquals("Second", message.getText());
                        assertFalse(message.getJMSRedelivered());
                        session.recover();
                        break;

                    case 3:
                        assertEquals("Second", message.getText());
                        assertTrue(message.getJMSRedelivered());
                        message.acknowledge();
                        doneCountDownLatch.countDown();
                        break;

                    default:
                        errorMessage[0] = "Got too many messages: " + counter;
                        doneCountDownLatch.countDown();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errorMessage[0] = "Got exception: " + e;
                    doneCountDownLatch.countDown();
                }
            }
        });
        connection.start();

        if (doneCountDownLatch.await(5, TimeUnit.SECONDS)) {
            if (errorMessage[0] != null) {
                fail(errorMessage[0]);
            }
        } else {
            fail("Timeout waiting for async message delivery to complete.");
        }

    }

    /**
     * Test to make sure that a Async recover works when using AUTO_ACKNOWLEDGE.
     * 
     * @throws JMSException
     * @throws InterruptedException
     */
    public void doTestAsynchRecoverWithAutoAck() throws JMSException, InterruptedException {

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final String errorMessage[] = new String[] {null};
        final CountDownLatch doneCountDownLatch = new CountDownLatch(1);

        MessageConsumer consumer = session.createConsumer(dest);

        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("First"));
        producer.send(session.createTextMessage("Second"));

        consumer.setMessageListener(new MessageListener() {
            int counter;

            public void onMessage(Message msg) {
                counter++;
                try {
                    TextMessage message = (TextMessage)msg;
                    switch (counter) {
                    case 1:
                        assertEquals("First", message.getText());
                        assertFalse(message.getJMSRedelivered());
                        break;
                    case 2:
                        // This should rollback the delivery of this message..
                        // and re-deliver.
                        assertEquals("Second", message.getText());
                        assertFalse(message.getJMSRedelivered());
                        session.recover();
                        break;

                    case 3:
                        assertEquals("Second", message.getText());
                        assertTrue(message.getJMSRedelivered());
                        doneCountDownLatch.countDown();
                        break;

                    default:
                        errorMessage[0] = "Got too many messages: " + counter;
                        doneCountDownLatch.countDown();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errorMessage[0] = "Got exception: " + e;
                    doneCountDownLatch.countDown();
                }
            }
        });
        connection.start();

        if (doneCountDownLatch.await(5000, TimeUnit.SECONDS)) {
            if (errorMessage[0] != null) {
                fail(errorMessage[0]);
            }
        } else {
            fail("Timeout waiting for async message delivery to complete.");
        }
    }
}
