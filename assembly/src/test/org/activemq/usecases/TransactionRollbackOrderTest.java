/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.usecases;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import junit.framework.TestCase;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTopic;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;


/**
 * Test case for AMQ-268
 *
 * @author Paul Smith
 * @version $Revision: 1.1 $
 */
public final class TransactionRollbackOrderTest extends TestCase {

    private volatile String receivedText;

    private Session producerSession;
    private Session consumerSession;
    private Destination queue;

    private MessageProducer producer;
    private MessageConsumer consumer;
    private Connection connection;
    private CountDownLatch latch = new CountDownLatch(1);
    private int NUM_MESSAGES = 5;
    private List msgSent = new ArrayList();
    private List msgCommitted = new ArrayList();
    private List msgRolledBack = new ArrayList();
    private List msgRedelivered = new ArrayList();

    public void testTransaction() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        connection = factory.createConnection();
        queue = new ActiveMQQueue(getClass().getName() + "." + getName());

        producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerSession = connection.createSession(true, 0);

        producer = producerSession.createProducer(queue);

        consumer = consumerSession.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            int msgCount = 0;
            int msgCommittedCount = 0;

            public void onMessage(Message m) {
                try {
                    msgCount++;
                    TextMessage tm = (TextMessage) m;
                    receivedText = tm.getText();

                    if (tm.getJMSRedelivered()) {
                        msgRedelivered.add(receivedText);
                    }

                    System.out.println("consumer received message: " + receivedText + (tm.getJMSRedelivered() ? " ** Redelivered **" : ""));
                    if (msgCount == 3) {
                        msgRolledBack.add(receivedText);
                        consumerSession.rollback();
                        System.out.println("[msg: " + receivedText + "] ** rolled back **");
                    }
                    else {
                        msgCommittedCount++;
                        msgCommitted.add(receivedText);
                        consumerSession.commit();
                        System.out.println("[msg: " + receivedText + "] committed transaction ");
                    }
                    if (msgCommittedCount == NUM_MESSAGES) {
                        latch.countDown();
                    }
                }
                catch (JMSException e) {
                    try {
                        consumerSession.rollback();
                        System.out.println("rolled back transaction");
                    }
                    catch (JMSException e1) {
                        System.out.println(e1);
                        e1.printStackTrace();
                    }
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
        });
        connection.start();

        TextMessage tm = null;
        try {
            for (int i = 1; i <= NUM_MESSAGES; i++) {
                tm = producerSession.createTextMessage();
                tm.setText("Hello " + i);
                msgSent.add(tm.getText());
                producer.send(tm);
                System.out.println("producer sent message: " + tm.getText());
            }
        }
        catch (JMSException e) {
            e.printStackTrace();
        }

        System.out.println("Waiting for latch");
        latch.await();

        assertEquals(1, msgRolledBack.size());
        assertEquals(1, msgRedelivered.size());

        System.out.println("msg RolledBack = " + msgRolledBack.get(0));
        System.out.println("msg Redelivered = " + msgRedelivered.get(0));

        assertEquals(msgRolledBack.get(0), msgRedelivered.get(0));

        assertEquals(NUM_MESSAGES, msgSent.size());
        assertEquals(NUM_MESSAGES, msgCommitted.size());

        assertEquals(msgSent, msgCommitted);

    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            System.out.println("Closing the connection");
            connection.close();
        }
        super.tearDown();
    }
}