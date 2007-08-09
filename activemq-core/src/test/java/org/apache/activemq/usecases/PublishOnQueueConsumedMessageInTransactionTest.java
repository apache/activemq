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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class PublishOnQueueConsumedMessageInTransactionTest extends TestCase implements MessageListener {

    private static final Log LOG = LogFactory.getLog(PublishOnQueueConsumedMessageInTransactionTest.class);

    private Session producerSession;
    private Session consumerSession;
    private Destination queue;
    private ActiveMQConnectionFactory factory;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private Connection connection;
    private ObjectMessage objectMessage;
    private List messages = createConcurrentList();
    private final Object lock = new Object();
    private String[] data;
    private String dataFileRoot = IOHelper.getDefaultDataDirectory();
    private int messageCount = 3;
    private String url = "vm://localhost";

    // Invalid acknowledgment warning can be viewed on the console of a remote
    // broker
    // The warning message is not thrown back to the client
    // private String url = "tcp://localhost:61616";

    protected void setUp() throws Exception {
        File dataFile = new File(dataFileRoot);
        recursiveDelete(dataFile);
        try {
            factory = new ActiveMQConnectionFactory(url);
            connection = factory.createConnection();
            producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            queue = new ActiveMQQueue("FOO.BAR");
            data = new String[messageCount];

            for (int i = 0; i < messageCount; i++) {
                data[i] = "Message : " + i;
            }
        } catch (JMSException je) {
            fail("Error setting up connection : " + je.toString());
        }
    }

    public void testSendReceive() throws Exception {
        sendMessage();

        connection.start();
        consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(this);
        waitForMessagesToBeDelivered();
        assertEquals("Messages received doesn't equal messages sent", messages.size(), data.length);

    }

    protected void sendMessage() throws JMSException {
        messages.clear();
        try {
            for (int i = 0; i < data.length; ++i) {
                producer = producerSession.createProducer(queue);
                objectMessage = producerSession.createObjectMessage(data[i]);
                producer.send(objectMessage);
                producerSession.commit();
                LOG.info("sending message :" + objectMessage);
            }
        } catch (Exception e) {
            if (producerSession != null) {
                producerSession.rollback();
                LOG.info("rollback");
                producerSession.close();
            }

            e.printStackTrace();
        }
    }

    public synchronized void onMessage(Message m) {
        try {
            objectMessage = (ObjectMessage)m;
            consumeMessage(objectMessage, messages);

            LOG.info("consumer received message :" + objectMessage);
            consumerSession.commit();

        } catch (Exception e) {
            try {
                consumerSession.rollback();
                LOG.info("rolled back transaction");
            } catch (JMSException e1) {
                LOG.info(e1);
                e1.printStackTrace();
            }
            LOG.info(e);
            e.printStackTrace();
        }
    }

    protected void consumeMessage(Message message, List messageList) {
        messageList.add(message);
        if (messageList.size() >= data.length) {
            synchronized (lock) {
                lock.notifyAll();
            }
        }

    }

    protected List createConcurrentList() {
        return Collections.synchronizedList(new ArrayList());
    }

    protected void waitForMessagesToBeDelivered() {
        long maxWaitTime = 5000;
        long waitTime = maxWaitTime;
        long start = (maxWaitTime <= 0) ? 0 : System.currentTimeMillis();

        synchronized (lock) {
            while (messages.size() <= data.length && waitTime >= 0) {
                try {
                    lock.wait(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                waitTime = maxWaitTime - (System.currentTimeMillis() - start);
            }
        }
    }

    protected static void recursiveDelete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        file.delete();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }

        super.tearDown();
    }
}
