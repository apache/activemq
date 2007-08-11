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

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * @version $Revision: 1.2 $
 */
public class JmsCreateConsumerInOnMessageTest extends TestSupport implements MessageListener {

    private Connection connection;
    private Session publisherSession;
    private Session consumerSession;
    private MessageConsumer consumer;
    private MessageConsumer testConsumer;
    private MessageProducer producer;
    private Topic topic;
    private Object lock = new Object();

    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        super.topic = true;
        connection = createConnection();
        connection.setClientID("connection:" + getSubject());
        publisherSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = (Topic)super.createDestination("Test.Topic");
        consumer = consumerSession.createConsumer(topic);
        consumer.setMessageListener(this);
        producer = publisherSession.createProducer(topic);
        connection.start();
    }

    /*
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        connection.close();
    }

    /**
     * Tests if a consumer can be created asynchronusly
     * 
     * @throws Exception
     */
    public void testCreateConsumer() throws Exception {
        Message msg = super.createMessage();
        producer.send(msg);
        if (testConsumer == null) {
            synchronized (lock) {
                lock.wait(3000);
            }
        }
        assertTrue(testConsumer != null);
    }

    /**
     * Use the asynchronous subscription mechanism
     * 
     * @param message
     */
    public void onMessage(Message message) {
        try {
            testConsumer = consumerSession.createConsumer(topic);
            consumerSession.createProducer(topic);
            synchronized (lock) {
                lock.notify();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            assertTrue(false);
        }
    }
}
