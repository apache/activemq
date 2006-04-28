/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MessageListenerRedeliveryTest extends TestCase {

    private static final Log log = LogFactory.getLog(MessageListenerRedeliveryTest.class);
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
        redeliveryPolicy.setInitialRedeliveryDelay(1000);
        redeliveryPolicy.setBackOffMultiplier((short) 5);
        redeliveryPolicy.setMaximumRedeliveries(10);
        redeliveryPolicy.setUseExponentialBackOff(true);
        return redeliveryPolicy;
    }

    protected Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        factory.setRedeliveryPolicy(getRedeliveryPolicy());
        return factory.createConnection();
    }

    private class ConsumerMessageListenerTest implements MessageListener {
        private ActiveMQMessageConsumer consumer;
        public int counter = 0;

        public ConsumerMessageListenerTest(ActiveMQMessageConsumer consumer) {
            this.consumer = consumer;
        }

        public void onMessage(Message message) {
            try {
                log.info("Message: " + message);
                counter++;
                if (counter <= 2) {
                    log.info("ROLLBACK");
                    consumer.rollback();
                } else {
                    log.info("COMMIT");
                    message.acknowledge();
                    consumer.commit();
                }
            } catch (JMSException e) {
                System.err.println("Error when rolling back transaction");
            }
        }
    }

    private class SessionMessageListenerTest implements MessageListener {
        private Session session;
        public int counter = 0;

        public SessionMessageListenerTest(Session session) {
            this.session = session;
        }

        public void onMessage(Message message) {
            try {
                log.info("Message: " + message);
                counter++;
                if (counter < 2) {
                    log.info("ROLLBACK");
                    session.rollback();
                } else {
                    log.info("COMMIT");
                    message.acknowledge();
                    session.commit();
                }
            } catch (JMSException e) {
                System.err.println("Error when rolling back transaction");
            }
        }
    }

    public void testQueueRollbackMessageListener() throws JMSException {
        connection.start();

        Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue-" + getName());
        MessageProducer producer = createProducer(session, queue);
        Message message = createTextMessage(session);
        producer.send(message);
        session.commit();

        MessageConsumer consumer = session.createConsumer(queue);

        ActiveMQMessageConsumer mc = (ActiveMQMessageConsumer) consumer;
        mc.setRedeliveryPolicy(getRedeliveryPolicy());

        SessionMessageListenerTest listener = new SessionMessageListenerTest(session);
        consumer.setMessageListener(listener);

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {

        }
        assertEquals(2, listener.counter);

        producer.send(createTextMessage(session));
        session.commit();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignore
        }
        assertEquals(3, listener.counter);

        session.close();
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