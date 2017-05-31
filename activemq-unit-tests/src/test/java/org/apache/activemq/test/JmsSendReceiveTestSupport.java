/*
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
package org.apache.activemq.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

import junit.framework.AssertionFailedError;

public abstract class JmsSendReceiveTestSupport extends org.apache.activemq.TestSupport implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(JmsSendReceiveTestSupport.class);

    protected int messageCount = 100;
    protected String[] data;
    protected Session session;
    protected Session consumeSession;
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
    protected boolean useSeparateSession;
    protected boolean largeMessages;
    protected int largeMessageLoopSize = 4 * 1024;

    /*
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        messages.clear();
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
            data[i] = createMessageText(i);
        }
    }

    protected String createMessageText(int i) {
        if (largeMessages) {
            return createMessageBodyText();
        } else {
            return "Text for message: " + i + " at " + new Date();
        }
    }

    protected String createMessageBodyText() {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < largeMessageLoopSize; i++) {
            buffer.append("0123456789");
        }
        return buffer.toString();
    }

    /**
     * Test if all the messages sent are being received.
     *
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        Thread.sleep(100);

        sendMessages();

        assertMessagesAreReceived();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }

    protected void sendMessages() throws Exception {
        for (int i = 0; i < data.length; i++) {
            Message message = createMessage(i);
            configureMessage(message);
            if (verbose) {
                LOG.info("About to send a message: " + message + " with text: " + data[i]);
            }
            sendMessage(i, message);
        }
    }

    protected void sendMessage(int index, Message message) throws Exception {
        producer.send(producerDestination, message);
    }

    protected Message createMessage(int index) throws JMSException {
        Message message = session.createTextMessage(data[index]);
        return message;
    }

    /**
     * A hook to allow the message to be configured such as adding extra headers
     *
     * @throws JMSException
     */
    protected void configureMessage(Message message) throws JMSException {
    }

    /**
     * Waits to receive the messages and performs the test if all messages have
     * been received and are in sequential order.
     *
     * @throws JMSException
     */
    protected void assertMessagesAreReceived() throws JMSException {
        waitForMessagesToBeDelivered();
        assertMessagesReceivedAreValid(messages);
    }

    /**
     * Tests if the messages have all been received and are in sequential order.
     *
     * @param receivedMessages
     * @throws JMSException
     */
    protected void assertMessagesReceivedAreValid(List<Message> receivedMessages) throws JMSException {
        List<Object> copyOfMessages = Arrays.asList(receivedMessages.toArray());
        int counter = 0;

        if (data.length != copyOfMessages.size()) {
            for (Iterator<Object> iter = copyOfMessages.iterator(); iter.hasNext();) {
                Object message = iter.next();
                LOG.info("<== " + counter++ + " = " + message);
            }
        }

        assertEquals("Invalid number of messages received", data.length, receivedMessages.size());

        for (int i = 0; i < data.length; i++) {
            Message received = receivedMessages.get(i);
            try {
                assertMessageValid(i, received);
            } catch (AssertionFailedError e) {
                for (int j = 0; j < data.length; j++) {
                    Message m = receivedMessages.get(j);
                    System.out.println(j+" => "+m.getJMSMessageID());
                }
                throw e;
            }
        }
    }

    protected void assertMessageValid(int index, Message message) throws JMSException {
        TextMessage textMessage = (TextMessage)message;
        String text = textMessage.getText();

        if (verbose) {
            LOG.info("Received Text: " + text);
        }

        assertEquals("Message: " + index, data[index], text);
    }

    /**
     * Waits for the messages to be delivered or when the wait time has been
     * reached.
     */
    protected void waitForMessagesToBeDelivered() {
        waitForMessagesToBeDelivered(TimeUnit.MINUTES.toMillis(1));
    }

    protected void waitForMessagesToBeDelivered(long maxWaitTime) {
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

    /**
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public synchronized void onMessage(Message message) {
        consumeMessage(message, messages);
    }

    /**
     * Consumes a received message.
     *
     * @param message - a newly received message.
     * @param messageList - list containing the received messages.
     */
    protected void consumeMessage(Message message, List<Message> messageList) {
        if (verbose) {
            LOG.info("Received message: " + message);
        }

        messageList.add(message);

        if (messageList.size() >= data.length) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    /**
     * Creates a synchronized list.
     *
     * @return a synchronized view of the specified list.
     */
    protected List<Message> createConcurrentList() {
        return Collections.synchronizedList(new ArrayList<Message>());
    }
}
