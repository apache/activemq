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

import java.util.HashMap;
import java.net.URI;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.util.IdGenerator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ReliableReconnectTest extends TestSupport {
    private static final int RECEIVE_TIMEOUT = 10000;
    protected static final int MESSAGE_COUNT = 100;
    protected static final String DEFAULT_BROKER_URL = "vm://localhost";
    private IdGenerator idGen = new IdGenerator();
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected String consumerClientId;
    protected Destination destination;
    protected AtomicBoolean closeBroker = new AtomicBoolean(false);
    protected AtomicInteger messagesReceived = new AtomicInteger(0);
    protected BrokerService broker;
    protected int firstBatch = MESSAGE_COUNT / 10;

    public ReliableReconnectTest() {
    }

    public ReliableReconnectTest(String n) {
        super(n);
    }

    protected void setUp() throws Exception {
        consumerClientId = idGen.generateId();
        super.setUp();
        topic = true;
        destination = createDestination(getClass().getName());
    }

    public ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        String url = "failover://" + DEFAULT_BROKER_URL;
        return new ActiveMQConnectionFactory(url);
    }

    protected void startBroker() throws JMSException {
        try {
            broker = BrokerFactory.createBroker(new URI("broker://()/localhost"));
            broker.addConnector(DEFAULT_BROKER_URL);
            broker.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Connection createConsumerConnection() throws Exception {
        Connection consumerConnection = getConnectionFactory().createConnection();
        consumerConnection.setClientID(consumerClientId);
        consumerConnection.start();
        return consumerConnection;
    }

    protected MessageConsumer createConsumer(Connection con) throws Exception {
        Session s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return s.createDurableSubscriber((Topic)destination, "TestFred");
    }

    protected void spawnConsumer() {
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    Connection consumerConnection = createConsumerConnection();
                    MessageConsumer consumer = createConsumer(consumerConnection);
                    // consume some messages

                    for (int i = 0; i < firstBatch; i++) {
                        Message msg = consumer.receive(RECEIVE_TIMEOUT);
                        if (msg != null) {
                            // log.info("GOT: " + msg);
                            messagesReceived.incrementAndGet();
                        }
                    }
                    synchronized (closeBroker) {
                        closeBroker.set(true);
                        closeBroker.notify();
                    }
                    Thread.sleep(2000);
                    for (int i = firstBatch; i < MESSAGE_COUNT; i++) {
                        Message msg = consumer.receive(RECEIVE_TIMEOUT);
                        // log.info("GOT: " + msg);
                        if (msg != null) {
                            messagesReceived.incrementAndGet();
                        }
                    }
                    consumerConnection.close();
                    synchronized (messagesReceived) {
                        messagesReceived.notify();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }

    public void testReconnect() throws Exception {
        startBroker();
        // register an interest as a durable subscriber
        Connection consumerConnection = createConsumerConnection();
        createConsumer(consumerConnection);
        consumerConnection.close();
        // send some messages ...
        Connection connection = createConnection();
        connection.setClientID(idGen.generateId());
        connection.start();
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        TextMessage msg = producerSession.createTextMessage();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            msg.setText("msg: " + i);
            producer.send(msg);
        }
        connection.close();
        spawnConsumer();
        synchronized (closeBroker) {
            if (!closeBroker.get()) {
                closeBroker.wait();
            }
        }
        // System.err.println("Stopping broker");
        broker.stop();
        startBroker();
        // System.err.println("Started Broker again");
        synchronized (messagesReceived) {
            if (messagesReceived.get() < MESSAGE_COUNT) {
                messagesReceived.wait(60000);
            }
        }
        // assertTrue(messagesReceived.get() == MESSAGE_COUNT);
        int count = messagesReceived.get();
        assertTrue("Not enough messages received: " + count, count > firstBatch);
    }
}
