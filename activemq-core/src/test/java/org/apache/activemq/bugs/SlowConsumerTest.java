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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SlowConsumerTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(SlowConsumerTest.class);
    private static final int MESSAGES_COUNT = 10000;

    protected int messageLogFrequency = 2500;
    protected long messageReceiveTimeout = 10000L;

    private Socket stompSocket;
    private ByteArrayOutputStream inputBuffer;
    private int messagesCount;

    /**
     * @param args
     * @throws Exception
     */
    public void testRemoveSubscriber() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);

        broker.addConnector("tcp://localhost:61616").setName("Default");
        broker.start();
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        final Connection connection = factory.createConnection();
        connection.start();

        Thread producingThread = new Thread("Producing thread") {
            public void run() {
                try {
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(new ActiveMQQueue(getDestinationName()));
                    for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
                        Message message = session.createTextMessage("" + idx);
                        producer.send(message);
                        LOG.debug("Sending: " + idx);
                    }
                    producer.close();
                    session.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        producingThread.setPriority(Thread.MAX_PRIORITY);
        producingThread.start();
        Thread.sleep(1000);

        Thread consumingThread = new Thread("Consuming thread") {

            public void run() {
                try {
                    Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(getDestinationName()));
                    int diff = 0;
                    while (messagesCount != MESSAGES_COUNT) {
                        Message msg = consumer.receive(messageReceiveTimeout);
                        if (msg == null) {
                            LOG.warn("Got null message at count: " + messagesCount + ". Continuing...");
                            break;
                        }
                        String text = ((TextMessage)msg).getText();
                        int currentMsgIdx = Integer.parseInt(text);
                        LOG.debug("Received: " + text + " messageCount: " + messagesCount);
                        msg.acknowledge();
                        if ((messagesCount + diff) != currentMsgIdx) {
                            LOG.debug("Message(s) skipped!! Should be message no.: " + messagesCount + " but got: " + currentMsgIdx);
                            diff = currentMsgIdx - messagesCount;
                        }
                        ++messagesCount;
                        if (messagesCount % messageLogFrequency == 0) {
                            LOG.info("Received: " + messagesCount + " messages so far");
                        }
                        // Thread.sleep(70);
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        consumingThread.start();
        consumingThread.join();

        assertEquals(MESSAGES_COUNT, messagesCount);

    }

    public void sendFrame(String data) throws Exception {
        byte[] bytes = data.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        for (int i = 0; i < bytes.length; i++) {
            outputStream.write(bytes[i]);
        }
        outputStream.flush();
    }

    public String receiveFrame(long timeOut) throws Exception {
        stompSocket.setSoTimeout((int)timeOut);
        InputStream is = stompSocket.getInputStream();
        int c = 0;
        for (;;) {
            c = is.read();
            if (c < 0) {
                throw new IOException("socket closed.");
            } else if (c == 0) {
                c = is.read();
                byte[] ba = inputBuffer.toByteArray();
                inputBuffer.reset();
                return new String(ba, "UTF-8");
            } else {
                inputBuffer.write(c);
            }
        }
    }

    protected String getDestinationName() {
        return getClass().getName() + "." + getName();
    }
}
