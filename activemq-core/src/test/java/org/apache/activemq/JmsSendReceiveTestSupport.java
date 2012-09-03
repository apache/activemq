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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class JmsSendReceiveTestSupport extends TestSupport implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(JmsSendReceiveTestSupport.class);

    protected int messageCount = 100;
    protected String[] data;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Destination consumerDestination;
    protected Destination producerDestination;
    protected List<Message> messages = createConcurrentList();
    protected boolean topic = true;
    protected boolean durable;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected final Object lock = new Object();
    protected boolean verbose;

    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        String temp = System.getProperty("messageCount");

        if (temp != null) {
            int i = Integer.parseInt(temp);
            if (i > 0) {
                messageCount = i;
            }
        }

        LOG.info("Message count for test case is: " + messageCount);
        data = new String[messageCount];

        for (int i = 0; i < messageCount; i++) {
            data[i] = "Text for message: " + i + " at " + new Date();
        }
    }

    /**
     * Sends and consumes the messages.
     * 
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        messages.clear();
        for (int i = 0; i < data.length; i++) {
            Message message = session.createTextMessage(data[i]);
            message.setStringProperty("stringProperty", data[i]);
            message.setIntProperty("intProperty", i);

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("About to send a message: " + message + " with text: " + data[i]);
                }
            }

            sendToProducer(producer, producerDestination, message);
            messageSent();
        }

        assertMessagesAreReceived();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }

    /**
     * Sends a message to a destination using the supplied producer
     * @param producer
     * @param producerDestination
     * @param message
     * @throws JMSException
     */
    protected void sendToProducer(MessageProducer producer,
            Destination producerDestination, Message message) throws JMSException {
        producer.send(producerDestination, message);   
    }

    /**
     * Asserts messages are received.
     * 
     * @throws JMSException
     */
    protected void assertMessagesAreReceived() throws JMSException {
        waitForMessagesToBeDelivered();
        assertMessagesReceivedAreValid(messages);
    }

    /**
     * Tests if the messages received are valid.
     * 
     * @param receivedMessages - list of received messages.
     * @throws JMSException
     */
    protected void assertMessagesReceivedAreValid(List<Message> receivedMessages) throws JMSException {
        List<Object> copyOfMessages = Arrays.asList(receivedMessages.toArray());
        int counter = 0;

        if (data.length != copyOfMessages.size()) {
            for (Iterator<Object> iter = copyOfMessages.iterator(); iter.hasNext();) {
                TextMessage message = (TextMessage)iter.next();
                if (LOG.isInfoEnabled()) {
                    LOG.info("<== " + counter++ + " = " + message.getText());
                }
            }
        }

        assertEquals("Not enough messages received", data.length, receivedMessages.size());

        for (int i = 0; i < data.length; i++) {
            TextMessage received = (TextMessage)receivedMessages.get(i);
            String text = received.getText();
            String stringProperty = received.getStringProperty("stringProperty");
            int intProperty = received.getIntProperty("intProperty");

            if (verbose) {
                if (LOG.isDebugEnabled()) {
                    LOG.info("Received Text: " + text);
                }
            }

            assertEquals("Message: " + i, data[i], text);
            assertEquals(data[i], stringProperty);
            assertEquals(i, intProperty);
        }
    }

    /**
     * Waits for messages to be delivered.
     */
    protected void waitForMessagesToBeDelivered() {
        long maxWaitTime = 60000;
        long waitTime = maxWaitTime;
        long start = (maxWaitTime <= 0) ? 0 : System.currentTimeMillis();

        synchronized (lock) {
            while (messages.size() < data.length && waitTime >= 0) {
                try {
                    lock.wait(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                waitTime = maxWaitTime - (System.currentTimeMillis() - start);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    public synchronized void onMessage(Message message) {
        consumeMessage(message, messages);
    }

    /**
     * Consumes messages.
     * 
     * @param message - message to be consumed.
     * @param messageList -list of consumed messages.
     */
    protected void consumeMessage(Message message, List<Message> messageList) {
        if (verbose) {
            if (LOG.isDebugEnabled()) {
                LOG.info("Received message: " + message);
            }
        }

        messageList.add(message);

        if (messageList.size() >= data.length) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    /**
     * Returns the ArrayList as a synchronized list.
     * 
     * @return List
     */
    protected List<Message> createConcurrentList() {
        return Collections.synchronizedList(new ArrayList<Message>());
    }

    /**
     * Just a hook so can insert failure tests
     * 
     * @throws Exception
     */
    protected void messageSent() throws Exception {

    }
}
