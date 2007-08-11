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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * simulate message flow which cause the following exception in the broker
 * (exception logged by client) <p/> 2007-07-24 13:51:23,624
 * com.easynet.halo.Halo ERROR (LoggingErrorHandler.java: 23) JMS failure
 * javax.jms.JMSException: Transaction 'TX:ID:dmt-53625-1185281414694-1:0:344'
 * has not been started. at
 * org.apache.activemq.broker.TransactionBroker.getTransaction(TransactionBroker.java:230)
 * This appears to be consistent in a MacBook. Haven't been able to replicate it
 * on Windows though
 */
public class TransactionNotStartedErrorTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(TransactionNotStartedErrorTest.class);
    
    private static int counter = 500;

    private static int hectorToHaloCtr;
    private static int xenaToHaloCtr;
    private static int troyToHaloCtr;

    private static int haloToHectorCtr;
    private static int haloToXenaCtr;
    private static int haloToTroyCtr;

    private String hectorToHalo = "hectorToHalo";
    private String xenaToHalo = "xenaToHalo";
    private String troyToHalo = "troyToHalo";

    private String haloToHector = "haloToHector";
    private String haloToXena = "haloToXena";
    private String haloToTroy = "haloToTroy";


    private BrokerService broker;

    private Connection hectorConnection;
    private Connection xenaConnection;
    private Connection troyConnection;
    private Connection haloConnection;

    private final Object lock = new Object();

    public Connection createConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        return factory.createConnection();
    }

    public Session createSession(Connection connection, boolean transacted) throws JMSException {
        return connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
    }

    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616").setName("Default");
        broker.start();
        LOG.info("Starting broker..");
    }

    public void tearDown() throws Exception {
        hectorConnection.close();
        xenaConnection.close();
        troyConnection.close();
        haloConnection.close();
        broker.stop();
    }

    public void testTransactionNotStartedError() throws Exception {
        startBroker();
        hectorConnection = createConnection();
        Thread hectorThread = buildProducer(hectorConnection, hectorToHalo);
        Receiver hHectorReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                haloToHectorCtr++;
                if (haloToHectorCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        buildReceiver(hectorConnection, haloToHector, false, hHectorReceiver);

        troyConnection = createConnection();
        Thread troyThread = buildProducer(troyConnection, troyToHalo);
        Receiver hTroyReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                haloToTroyCtr++;
                if (haloToTroyCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        buildReceiver(hectorConnection, haloToTroy, false, hTroyReceiver);

        xenaConnection = createConnection();
        Thread xenaThread = buildProducer(xenaConnection, xenaToHalo);
        Receiver hXenaReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                haloToXenaCtr++;
                if (haloToXenaCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        buildReceiver(xenaConnection, haloToXena, false, hXenaReceiver);

        haloConnection = createConnection();
        final MessageSender hectorSender = buildTransactionalProducer(haloToHector, haloConnection);
        final MessageSender troySender = buildTransactionalProducer(haloToTroy, haloConnection);
        final MessageSender xenaSender = buildTransactionalProducer(haloToXena, haloConnection);
        Receiver hectorReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                hectorToHaloCtr++;
                troySender.send("halo to troy because of hector");
                if (hectorToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        Receiver xenaReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                xenaToHaloCtr++;
                hectorSender.send("halo to hector because of xena");
                if (xenaToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        Receiver troyReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                troyToHaloCtr++;
                xenaSender.send("halo to xena because of troy");
                if (troyToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        buildReceiver(haloConnection, hectorToHalo, true, hectorReceiver);
        buildReceiver(haloConnection, xenaToHalo, true, xenaReceiver);
        buildReceiver(haloConnection, troyToHalo, true, troyReceiver);

        haloConnection.start();

        troyConnection.start();
        troyThread.start();

        xenaConnection.start();
        xenaThread.start();

        hectorConnection.start();
        hectorThread.start();
        waitForMessagesToBeDelivered();
        // number of messages received should match messages sent
        assertEquals(hectorToHaloCtr, counter);
        LOG.info("hectorToHalo received " + hectorToHaloCtr + " messages");
        assertEquals(xenaToHaloCtr, counter);
        LOG.info("xenaToHalo received " + xenaToHaloCtr + " messages");
        assertEquals(troyToHaloCtr, counter);
        LOG.info("troyToHalo received " + troyToHaloCtr + " messages");
        assertEquals(haloToHectorCtr, counter);
        LOG.info("haloToHector received " + haloToHectorCtr + " messages");
        assertEquals(haloToXenaCtr, counter);
        LOG.info("haloToXena received " + haloToXenaCtr + " messages");
        assertEquals(haloToTroyCtr, counter);
        LOG.info("haloToTroy received " + haloToTroyCtr + " messages");

    }

    protected void waitForMessagesToBeDelivered() {
        // let's give the listeners enough time to read all messages
        long maxWaitTime = counter * 3000;
        long waitTime = maxWaitTime;
        long start = (maxWaitTime <= 0) ? 0 : System.currentTimeMillis();

        synchronized (lock) {
            boolean hasMessages = true;
            while (hasMessages && waitTime >= 0) {
                try {
                    lock.wait(200);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
                // check if all messages have been received
                hasMessages = hectorToHaloCtr < counter || xenaToHaloCtr < counter || troyToHaloCtr < counter || haloToHectorCtr < counter || haloToXenaCtr < counter
                              || haloToTroyCtr < counter;
                waitTime = maxWaitTime - (System.currentTimeMillis() - start);
            }
        }
    }

    public MessageSender buildTransactionalProducer(String queueName, Connection connection) throws Exception {

        return new MessageSender(queueName, connection, true);
    }

    public Thread buildProducer(Connection connection, final String queueName) throws Exception {

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageSender producer = new MessageSender(queueName, connection, false);
        Thread thread = new Thread() {

            public synchronized void run() {
                for (int i = 0; i < counter; i++) {
                    try {
                        producer.send(queueName);
                        if (session.getTransacted()) {
                            session.commit();
                        }

                    } catch (Exception e) {
                        throw new RuntimeException("on " + queueName + " send", e);
                    }
                }
            }
        };
        return thread;
    }

    public void buildReceiver(Connection connection, final String queueName, boolean transacted, final Receiver receiver) throws Exception {
        final Session session = transacted ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer inputMessageConsumer = session.createConsumer(session.createQueue(queueName));
        MessageListener messageListener = new MessageListener() {

            public void onMessage(Message message) {
                try {
                    ObjectMessage objectMessage = (ObjectMessage)message;
                    String s = (String)objectMessage.getObject();
                    receiver.receive(s);
                    if (session.getTransacted()) {
                        session.commit();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        inputMessageConsumer.setMessageListener(messageListener);
    }

}
