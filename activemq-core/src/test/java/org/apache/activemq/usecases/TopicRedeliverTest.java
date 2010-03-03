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
package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.memory.list.MessageList;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TopicRedeliverTest extends TestSupport {

    private static final Log LOG = LogFactory.getLog(TopicRedeliverTest.class);
    private static final int RECEIVE_TIMEOUT = 10000;

    protected int deliveryMode = DeliveryMode.PERSISTENT;
    private IdGenerator idGen = new IdGenerator();

    public TopicRedeliverTest() {
    }

    public TopicRedeliverTest(String n) {
        super(n);
    }

    protected void setUp() throws Exception {
        super.setUp();
        topic = true;
    }

    /**
     * test messages are acknowledged and recovered properly
     * 
     * @throws Exception
     */
    public void testClientAcknowledge() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        // send some messages

        TextMessage sent1 = producerSession.createTextMessage();
        sent1.setText("msg1");
        producer.send(sent1);

        TextMessage sent2 = producerSession.createTextMessage();
        sent1.setText("msg2");
        producer.send(sent2);

        TextMessage sent3 = producerSession.createTextMessage();
        sent1.setText("msg3");
        producer.send(sent3);

        consumer.receive(RECEIVE_TIMEOUT);
        Message rec2 = consumer.receive(RECEIVE_TIMEOUT);
        consumer.receive(RECEIVE_TIMEOUT);

        // ack rec2
        rec2.acknowledge();

        TextMessage sent4 = producerSession.createTextMessage();
        sent4.setText("msg4");
        producer.send(sent4);

        Message rec4 = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(rec4.equals(sent4));
        consumerSession.recover();
        rec4 = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(rec4.equals(sent4));
        assertTrue(rec4.getJMSRedelivered());
        rec4.acknowledge();
        connection.close();

    }

    /**
     * Test redelivered flag is set on rollbacked transactions
     * 
     * @throws Exception
     */
    public void testRedilveredFlagSetOnRollback() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (topic) {
            consumer = consumerSession.createDurableSubscriber((Topic)destination, "TESTRED");
        } else {
            consumer = consumerSession.createConsumer(destination);
        }
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertFalse(recMsg.getJMSRedelivered());
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.rollback();
        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(recMsg.getJMSRedelivered());
        consumerSession.commit();
        assertTrue(recMsg.equals(sentMsg));
        assertTrue(recMsg.getJMSRedelivered());
        connection.close();
    }

    /**
     * Check a session is rollbacked on a Session close();
     * 
     * @throws Exception
     */

    public void xtestTransactionRollbackOnSessionClose() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        if (topic) {
            consumer = consumerSession.createDurableSubscriber((Topic)destination, "TESTRED");
        } else {
            consumer = consumerSession.createConsumer(destination);
        }
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);

        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertFalse(recMsg.getJMSRedelivered());
        consumerSession.close();
        consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        consumer = consumerSession.createConsumer(destination);

        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.commit();
        assertTrue(recMsg.equals(sentMsg));
        connection.close();
    }

    /**
     * check messages are actuallly sent on a tx rollback
     * 
     * @throws Exception
     */

    public void testTransactionRollbackOnSend() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        Message recMsg = consumer.receive(RECEIVE_TIMEOUT);
        consumerSession.commit();
        assertTrue(recMsg.equals(sentMsg));

        sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg2");
        producer.send(sentMsg);
        producerSession.rollback();

        sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg3");
        producer.send(sentMsg);
        producerSession.commit();

        recMsg = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue(recMsg.equals(sentMsg));
        consumerSession.commit();

        connection.close();
    }

    
    public void testRedeliveryOnListenerException() throws Exception {
        Destination destination = createDestination(getClass().getName());
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        
        final ArrayList<Message> receivedMessages = new ArrayList<Message>();
        final CountDownLatch received = new CountDownLatch(6);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                LOG.info("got: " + message);
                receivedMessages.add(message);
                received.countDown();
                if (received.getCount() == 5) {
                    throw new RuntimeException("force redelivery on first message");
                }
            }
        });
        Session producerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        TextMessage sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg1");
        producer.send(sentMsg);
        producerSession.commit();

        sentMsg = producerSession.createTextMessage();
        sentMsg.setText("msg2");
        producer.send(sentMsg);
        producerSession.commit();

        TimeUnit.SECONDS.sleep(2);
        //assertTrue("got our redeliveries", received.await(20, TimeUnit.SECONDS));
        assertEquals("got message one", "msg1", ((TextMessage)receivedMessages.get(0)).getText());
        // retries
        for (int i=1; i< 6; i++) {
            assertEquals("got message one", "msg2", ((TextMessage)receivedMessages.get(i)).getText());
        }
        
        connection.close();
    }

}
