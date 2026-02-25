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

import java.util.Date;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

/**
 *
 */
@Category(ParallelTest.class)
public class JmsSendReceiveWithMessageExpirationTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsSendReceiveWithMessageExpirationTest.class);

    protected int messageCount = 100;
    protected String[] data;
    protected Session session;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected boolean durable;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected long timeToLive = 3000;
    protected boolean verbose;

    protected Connection connection;
    protected BrokerService brokerService;

    protected void setUp() throws Exception {

        super.setUp();

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();

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

         assertEquals("got all (normal plus one with ttl) messages", messageCount + 1, messages.size());

         Vector<Message> dlqMessages = new Vector<Message>();
         while ((received = dlqConsumer.receive(1000)) != null) {
             dlqMessages.add(received);
         };

         assertEquals("got dlq messages", data.length - 1, dlqMessages.size());

         final DestinationStatistics view = getDestinationStatistics(BrokerRegistry.getInstance().findFirst(), ActiveMQDestination.transform(consumerDestination));

         // wait for all to inflight to expire
         assertTrue("all inflight messages expired ", Wait.waitFor(new Wait.Condition() {
             @Override
             public boolean isSatisified() throws Exception {
                 return view.getInflight().getCount() == 0;
             }
         }));
         assertEquals("Wrong inFlightCount: ", 0, view.getInflight().getCount());

         LOG.info("Stats: received: "  + messages.size() + ", messages: " + view.getMessages().getCount() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                 + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expired: " + view.getExpired().getCount());

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

        MessageConsumer consumer1 = createConsumer();
        MessageConsumer consumer2 =  session.createConsumer(consumerDestination);
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
        assertNull(consumer1.receive(1000));
        assertNull(consumer2.receive(100));

        for (Subscription consumer : brokerService.getDestination(
                (ActiveMQDestination) consumerDestination)
            .getConsumers()) {
            assertEquals(0, consumer.getPendingQueueSize());
        }

        // Memory usage should be 0 after expiration
        assertEquals(0, brokerService.getDestination((ActiveMQDestination) consumerDestination)
            .getMemoryUsage().getUsage());
    }

    public void testConsumeExpiredTopicDurable() throws Exception {
        brokerService.stop();

        // Use persistent broker and durables so restart
        brokerService = new BrokerService();
        brokerService.setPersistent(true);
        brokerService.start();
        connection.close();
        connection = createConnection();
        connection.setClientID(getClass().getName());
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = createProducer(timeToLive);
        Topic topic = session.createTopic("test.expiration.topic");
        MessageConsumer consumer1 = session.createDurableSubscriber(topic, "sub1");
        MessageConsumer consumer2 = session.createDurableSubscriber(topic, "sub2");

        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);
            producer.send(topic, message);
        }

        // sleeps a second longer than the expiration time.
        // Basically waits till topic expires.
        Thread.sleep(timeToLive + 1000);

        // message should have expired for both clients
        assertNull(consumer1.receive(1000));
        assertNull(consumer2.receive(100));

        TopicMessageStore store = (TopicMessageStore) brokerService.getDestination((ActiveMQDestination) topic).getMessageStore();
        assertEquals(0, store.getMessageCount(getClass().getName(), "sub1"));
        assertEquals(0, store.getMessageCount(getClass().getName(), "sub2"));

        for (Subscription consumer : brokerService.getDestination((ActiveMQDestination) topic)
            .getConsumers()) {
            assertEquals(0, consumer.getPendingQueueSize());
        }
        // Memory usage should be 0 after expiration
        assertEquals(0, brokerService.getDestination((ActiveMQDestination) topic)
            .getMemoryUsage().getUsage());
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

        try {
            session.close();
            connection.close();
        } catch (Exception e) {
            // ignore
        }
        if (brokerService != null) {
            brokerService.stop();
        }
    }

}
