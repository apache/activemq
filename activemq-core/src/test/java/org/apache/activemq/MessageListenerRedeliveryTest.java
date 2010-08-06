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

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MessageListenerRedeliveryTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(MessageListenerRedeliveryTest.class);

    private Connection connection;

    protected void setUp() throws Exception {
        connection = createConnection();
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

    protected RedeliveryPolicy getRedeliveryPolicy() {
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setInitialRedeliveryDelay(0);
        redeliveryPolicy.setRedeliveryDelay(1000);
        redeliveryPolicy.setMaximumRedeliveries(3);
        redeliveryPolicy.setBackOffMultiplier((short)2);
        redeliveryPolicy.setUseExponentialBackOff(true);
        return redeliveryPolicy;
    }

    protected Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        factory.setRedeliveryPolicy(getRedeliveryPolicy());
        return factory.createConnection();
    }

    private class TestMessageListener implements MessageListener {

        public int counter;
        private Session session;

        public TestMessageListener(Session session) {
            this.session = session;
        }

        public void onMessage(Message message) {
            try {
                LOG.info("Message Received: " + message);
                counter++;
                if (counter <= 4) {
                    LOG.info("Message Rollback.");
                    session.rollback();
                } else {
                    LOG.info("Message Commit.");
                    message.acknowledge();
                    session.commit();
                }
            } catch (JMSException e) {
                LOG.error("Error when rolling back transaction");
            }
        }
    }

    public void testQueueRollbackConsumerListener() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session);
        producer.send(message);
        session.commit();

        MessageConsumer consumer = session.createConsumer(queue);

        ActiveMQMessageConsumer mc = (ActiveMQMessageConsumer)consumer;
        mc.setRedeliveryPolicy(getRedeliveryPolicy());

        TestMessageListener listener = new TestMessageListener(session);
        consumer.setMessageListener(listener);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }

        // first try.. should get 2 since there is no delay on the
        // first redeliver..
        assertEquals(2, listener.counter);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        // 2nd redeliver (redelivery after 1 sec)
        assertEquals(3, listener.counter);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {

        }
        // 3rd redeliver (redelivery after 2 seconds) - it should give up after
        // that
        assertEquals(4, listener.counter);

        // create new message
        producer.send(createTextMessage(session));
        session.commit();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // ignore
        }
        // it should be committed, so no redelivery
        assertEquals(5, listener.counter);

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            // ignore
        }
        // no redelivery, counter should still be 4
        assertEquals(5, listener.counter);

        session.close();
    }

    public void testQueueRollbackSessionListener() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session);
        producer.send(message);
        session.commit();

        MessageConsumer consumer = session.createConsumer(queue);

        ActiveMQMessageConsumer mc = (ActiveMQMessageConsumer)consumer;
        mc.setRedeliveryPolicy(getRedeliveryPolicy());

        TestMessageListener listener = new TestMessageListener(session);
        consumer.setMessageListener(listener);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }
        // first try
        assertEquals(2, listener.counter);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
        // second try (redelivery after 1 sec)
        assertEquals(3, listener.counter);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {

        }
        // third try (redelivery after 2 seconds) - it should give up after that
        assertEquals(4, listener.counter);

        // create new message
        producer.send(createTextMessage(session));
        session.commit();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // ignore
        }
        // it should be committed, so no redelivery
        assertEquals(5, listener.counter);

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            // ignore
        }
        // no redelivery, counter should still be 4
        assertEquals(5, listener.counter);

        session.close();
    }

    public void testQueueSessionListenerExceptionRetry() throws  Exception {
        connection.start();
            
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session, "1");
        producer.send(message);
        message = createTextMessage(session, "2");
        producer.send(message);
            
        
        MessageConsumer consumer = session.createConsumer(queue);
            
        final CountDownLatch gotMessage = new CountDownLatch(2);
        final AtomicInteger count  = new AtomicInteger(0);
        final int maxDeliveries = getRedeliveryPolicy().getMaximumRedeliveries();
        final ArrayList<String> received = new ArrayList<String>();
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                LOG.info("Message Received: " + message);
                try {
                    received.add(((TextMessage) message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                    fail(e.toString());
                }
                if (count.incrementAndGet() < maxDeliveries) {
                    throw new RuntimeException(getName() + " force a redelivery");
                }
                // new blood
                count.set(0);
                gotMessage.countDown();
            }
        });
            
        assertTrue("got message before retry expiry", gotMessage.await(20, TimeUnit.SECONDS));
        
        for (int i=0; i<maxDeliveries; i++) {
            assertEquals("got first redelivered: " + i, "1", received.get(i));
        }
        for (int i=maxDeliveries; i<maxDeliveries*2; i++) {
            assertEquals("got first redelivered: " + i, "2", received.get(i));
        }
        session.close();
    }
        

    public void testQueueSessionListenerExceptionDlq() throws  Exception {
        connection.start();
            
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session);
        producer.send(message);

        ActiveMQDestination dlqDestination = new ActiveMQQueue("ActiveMQ.DLQ");
        MessageConsumer dlqConsumer = session.createConsumer(dlqDestination);
        final CountDownLatch gotDlqMessage = new CountDownLatch(1);
        dlqConsumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                gotDlqMessage.countDown();
            }
        });

        MessageConsumer consumer = session.createConsumer(queue);
            
        final int maxDeliveries = getRedeliveryPolicy().getMaximumRedeliveries();
        final CountDownLatch gotMessage = new CountDownLatch(maxDeliveries);
        
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                LOG.info("Message Received: " + message);
                gotMessage.countDown();
                throw new RuntimeException(getName() + " force a redelivery");
            }
        });
            
        assertTrue("got message before retry expiry", gotMessage.await(20, TimeUnit.SECONDS));
        
        // check DLQ
        assertTrue("got dlq message", gotDlqMessage.await(20, TimeUnit.SECONDS));
        
        session.close();
    }
    

    private TextMessage createTextMessage(Session session, String text) throws JMSException {
        return session.createTextMessage(text);
    }
    private TextMessage createTextMessage(Session session) throws JMSException {
        return session.createTextMessage("Hello");
    }

    private MessageProducer createProducer(Session session, Destination queue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(getDeliveryMode());
        return producer;
    }

    protected int getDeliveryMode() {
        return DeliveryMode.PERSISTENT;
    }
}
