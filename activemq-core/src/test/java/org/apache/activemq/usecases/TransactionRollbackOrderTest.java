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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case for AMQ-268
 * 
 * @author Paul Smith
 * 
 */
public final class TransactionRollbackOrderTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionRollbackOrderTest.class);

    private volatile String receivedText;

    private Session producerSession;
    private Session consumerSession;
    private Destination queue;

    private MessageProducer producer;
    private MessageConsumer consumer;
    private Connection connection;
    private CountDownLatch latch = new CountDownLatch(1);
    private int numMessages = 5;
    private List<String> msgSent = new ArrayList<String>();
    private List<String> msgCommitted = new ArrayList<String>();
    private List<String> msgRolledBack = new ArrayList<String>();
    private List<String> msgRedelivered = new ArrayList<String>();

    public void testTransaction() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        connection = factory.createConnection();
        queue = new ActiveMQQueue(getClass().getName() + "." + getName());

        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerSession = connection.createSession(true, 0);

        producer = producerSession.createProducer(queue);

        consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            int msgCount;
            int msgCommittedCount;

            public void onMessage(Message m) {
                try {
                    msgCount++;
                    TextMessage tm = (TextMessage)m;
                    receivedText = tm.getText();

                    if (tm.getJMSRedelivered()) {
                        msgRedelivered.add(receivedText);
                    }

                    LOG.info("consumer received message: " + receivedText + (tm.getJMSRedelivered() ? " ** Redelivered **" : ""));
                    if (msgCount == 3) {
                        msgRolledBack.add(receivedText);
                        consumerSession.rollback();
                        LOG.info("[msg: " + receivedText + "] ** rolled back **");
                    } else {
                        msgCommittedCount++;
                        msgCommitted.add(receivedText);
                        consumerSession.commit();
                        LOG.info("[msg: " + receivedText + "] committed transaction ");
                    }
                    if (msgCommittedCount == numMessages) {
                        latch.countDown();
                    }
                } catch (JMSException e) {
                    try {
                        consumerSession.rollback();
                        LOG.info("rolled back transaction");
                    } catch (JMSException e1) {
                        LOG.info(e1.toString());
                        e1.printStackTrace();
                    }
                    LOG.info(e.toString());
                    e.printStackTrace();
                }
            }
        });
        connection.start();

        TextMessage tm = null;
        try {
            for (int i = 1; i <= numMessages; i++) {
                tm = producerSession.createTextMessage();
                tm.setText("Hello " + i);
                msgSent.add(tm.getText());
                producer.send(tm);
                LOG.info("producer sent message: " + tm.getText());
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }

        LOG.info("Waiting for latch");
        latch.await();

        assertEquals(1, msgRolledBack.size());
        assertEquals(1, msgRedelivered.size());

        LOG.info("msg RolledBack = " + msgRolledBack.get(0));
        LOG.info("msg Redelivered = " + msgRedelivered.get(0));

        assertEquals(msgRolledBack.get(0), msgRedelivered.get(0));

        assertEquals(numMessages, msgSent.size());
        assertEquals(numMessages, msgCommitted.size());

        assertEquals(msgSent, msgCommitted);

    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            LOG.info("Closing the connection");
            connection.close();
        }
        super.tearDown();
    }
}
