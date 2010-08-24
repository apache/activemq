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

import java.util.Date;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
public class JmsSendReceiveWithMessageExpirationTest extends TestSupport {

    private static final Log LOG = LogFactory.getLog(JmsSendReceiveWithMessageExpirationTest.class);

    protected int messageCount = 100;
    protected String[] data;
    protected Session session;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected boolean durable;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected long timeToLive = 5000;
    protected boolean verbose;

    protected Connection connection;

    protected void setUp() throws Exception {

        super.setUp();

        data = new String[messageCount];

        for (int i = 0; i < messageCount; i++) {
            data[i] = "Text for message: " + i + " at " + new Date();
        }

        connectionFactory = createConnectionFactory();
        connection = createConnection();

        if (durable) {
            connection.setClientID(getClass().getName());
        }

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Test consuming an expired queue.
     * 
     * @throws Exception
     */
    public void testConsumeExpiredQueue() throws Exception {

        MessageProducer producer = createProducer(timeToLive);

        consumerDestination = session.createQueue(getConsumerSubject());
        producerDestination = session.createQueue(getProducerSubject());

        MessageConsumer consumer = createConsumer();
        connection.start();

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a queue message: " + message + " with text: " + data[i]);
                }
            }

            producer.send(producerDestination, message);
        }

        // sleeps a second longer than the expiration time.
        // Basically waits till queue expires.
        Thread.sleep(timeToLive + 1000);

        // message should have expired.
        assertNull(consumer.receive(1000));
    }

     public void testConsumeExpiredQueueAndDlq() throws Exception {

         MessageProducer producerNormal = createProducer(0);
         MessageProducer producerExpire = createProducer(500);

         consumerDestination = session.createQueue("ActiveMQ.DLQ");
         MessageConsumer dlqConsumer = createConsumer();

         consumerDestination = session.createQueue(getConsumerSubject());
         producerDestination = session.createQueue(getProducerSubject());


         Connection consumerConnection = createConnection();
         ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
         prefetchPolicy.setAll(10);
         ((ActiveMQConnection)consumerConnection).setPrefetchPolicy(prefetchPolicy);
         Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSession.createConsumer(consumerDestination);
         consumerConnection.start();
         connection.start();

         String msgBody = new String(new byte[20*1024]);
         for (int i = 0; i < data.length; i++) {
             Message message = session.createTextMessage(msgBody);
             producerExpire.send(producerDestination, message);
         }

         for (int i = 0; i < data.length; i++) {
             Message message = session.createTextMessage(msgBody);
             producerNormal.send(producerDestination, message);
         }

         Vector<Message> messages = new Vector<Message>();
         Message received;
         while ((received = consumer.receive(1000)) != null) {
             messages.add(received);
             if (messages.size() == 1) {
                TimeUnit.SECONDS.sleep(1);
             }
             received.acknowledge();
         };

         assertEquals("got messages", messageCount + 1, messages.size());

         Vector<Message> dlqMessages = new Vector<Message>();
         while ((received = dlqConsumer.receive(1000)) != null) {
             dlqMessages.add(received);
         };

         assertEquals("got dlq messages", data.length - 1, dlqMessages.size());
    }
    
    /**
     * Sends and consumes the messages to a queue destination.
     * 
     * @throws Exception
     */
    public void testConsumeQueue() throws Exception {

        MessageProducer producer = createProducer(0);

        consumerDestination = session.createQueue(getConsumerSubject());
        producerDestination = session.createQueue(getProducerSubject());

        MessageConsumer consumer = createConsumer();
        connection.start();

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a queue message: " + message + " with text: " + data[i]);
                }
            }

            producer.send(producerDestination, message);
        }

        // should receive a queue since there is no expiration.
        assertNotNull(consumer.receive(1000));
    }

    /**
     * Test consuming an expired topic.
     * 
     * @throws Exception
     */
    public void testConsumeExpiredTopic() throws Exception {

        MessageProducer producer = createProducer(timeToLive);

        consumerDestination = session.createTopic(getConsumerSubject());
        producerDestination = session.createTopic(getProducerSubject());

        MessageConsumer consumer = createConsumer();
        connection.start();

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a topic message: " + message + " with text: " + data[i]);
                }
            }

            producer.send(producerDestination, message);
        }

        // sleeps a second longer than the expiration time.
        // Basically waits till topic expires.
        Thread.sleep(timeToLive + 1000);

        // message should have expired.
        assertNull(consumer.receive(1000));
    }

    /**
     * Sends and consumes the messages to a topic destination.
     * 
     * @throws Exception
     */
    public void testConsumeTopic() throws Exception {

        MessageProducer producer = createProducer(0);

        consumerDestination = session.createTopic(getConsumerSubject());
        producerDestination = session.createTopic(getProducerSubject());

        MessageConsumer consumer = createConsumer();
        connection.start();

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a topic message: " + message + " with text: " + data[i]);
                }
            }

            producer.send(producerDestination, message);
        }

        // should receive a topic since there is no expiration.
        assertNotNull(consumer.receive(1000));
    }

    protected MessageProducer createProducer(long timeToLive) throws JMSException {
        MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);
        producer.setTimeToLive(timeToLive);

        return producer;
    }

    protected MessageConsumer createConsumer() throws JMSException {
        if (durable) {
            LOG.info("Creating durable consumer");
            return session.createDurableSubscriber((Topic)consumerDestination, getName());
        }
        return session.createConsumer(consumerDestination);
    }

    protected void tearDown() throws Exception {
        LOG.info("Dumping stats...");
        LOG.info("Closing down connection");

        session.close();
        connection.close();
    }

}
