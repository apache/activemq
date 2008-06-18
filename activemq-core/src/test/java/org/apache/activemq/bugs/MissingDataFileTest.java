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
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Try and replicate:
 * Caused by: java.io.IOException: Could not locate data file data--188
 *  at org.apache.activemq.kaha.impl.async.AsyncDataManager.getDataFile(AsyncDataManager.java:302)
 *  at org.apache.activemq.kaha.impl.async.AsyncDataManager.read(AsyncDataManager.java:614)
 *  at org.apache.activemq.store.amq.AMQPersistenceAdapter.readCommand(AMQPersistenceAdapter.java:523)
 */

public class MissingDataFileTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(MissingDataFileTest.class);
    
    private static int counter = 300;

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
    final boolean useTopic = false;
    final boolean useSleep = true;
    
    protected static final String payload = new String(new byte[500]);

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
   
        SystemUsage systemUsage;
        systemUsage = new SystemUsage();
        systemUsage.getMemoryUsage().setLimit(1024 * 1024); // Just a few messags 
        broker.setSystemUsage(systemUsage);
        
        AMQPersistenceAdapterFactory factory = (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
        factory.setMaxFileLength(2*1024); // ~4 messages
        factory.setCleanupInterval(1000); // every few second
        
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

    public void testForNoDataFoundError() throws Exception {
        
        startBroker();
        hectorConnection = createConnection();
        Thread hectorThread = buildProducer(hectorConnection, hectorToHalo, false, useTopic);
        Receiver hHectorReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                haloToHectorCtr++;
                if (haloToHectorCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
                possiblySleep(haloToHectorCtr);
            }
        };
        buildReceiver(hectorConnection, haloToHector, false, hHectorReceiver, useTopic);

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
                possiblySleep(haloToTroyCtr);
            }
        };
        buildReceiver(hectorConnection, haloToTroy, false, hTroyReceiver, false);

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
                possiblySleep(haloToXenaCtr);
            }
        };
        buildReceiver(xenaConnection, haloToXena, false, hXenaReceiver, false);

        haloConnection = createConnection();
        final MessageSender hectorSender = buildTransactionalProducer(haloToHector, haloConnection, false);
        final MessageSender troySender = buildTransactionalProducer(haloToTroy, haloConnection, false);
        final MessageSender xenaSender = buildTransactionalProducer(haloToXena, haloConnection, false);
        Receiver hectorReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                hectorToHaloCtr++;
                troySender.send(payload);
                if (hectorToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                    possiblySleep(hectorToHaloCtr);
                }
            }
        };
        Receiver xenaReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                xenaToHaloCtr++;
                hectorSender.send(payload);
                if (xenaToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
                possiblySleep(xenaToHaloCtr);
            }
        };
        Receiver troyReceiver = new Receiver() {
            public void receive(String s) throws Exception {
                troyToHaloCtr++;
                xenaSender.send(payload);
                if (troyToHaloCtr >= counter) {
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };
        buildReceiver(haloConnection, hectorToHalo, true, hectorReceiver, false);
        buildReceiver(haloConnection, xenaToHalo, true, xenaReceiver, false);
        buildReceiver(haloConnection, troyToHalo, true, troyReceiver, false);

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

    protected void possiblySleep(int count) throws InterruptedException {
        if (useSleep) {
            if (count % 100 == 0) {
                Thread.sleep(5000);
            }
        }
        
    }

    protected void waitForMessagesToBeDelivered() {
        // let's give the listeners enough time to read all messages
        long maxWaitTime = counter * 1000;
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

    public MessageSender buildTransactionalProducer(String queueName, Connection connection, boolean isTopic) throws Exception {

        return new MessageSender(queueName, connection, true, isTopic);
    }

    public Thread buildProducer(Connection connection, final String queueName) throws Exception {
        return buildProducer(connection, queueName, false, false);
    }
    
    public Thread buildProducer(Connection connection, final String queueName, boolean transacted, boolean isTopic) throws Exception {
        final MessageSender producer = new MessageSender(queueName, connection, transacted, isTopic);
        Thread thread = new Thread() {
            public synchronized void run() {
                for (int i = 0; i < counter; i++) {
                    try {
                        producer.send(payload );
                    } catch (Exception e) {
                        throw new RuntimeException("on " + queueName + " send", e);
                    }
                }
            }
        };
        return thread;
    }

    public void buildReceiver(Connection connection, final String queueName, boolean transacted, final Receiver receiver, boolean isTopic) throws Exception {
        final Session session = transacted ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer inputMessageConsumer = session.createConsumer(isTopic ? session.createTopic(queueName) : session.createQueue(queueName));
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
