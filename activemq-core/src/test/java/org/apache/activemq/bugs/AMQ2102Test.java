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


import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AMQ2102Test extends CombinationTestSupport implements UncaughtExceptionHandler {
       
    final static int MESSAGE_COUNT = 12120;
    final static int NUM_CONSUMERS = 10;
    final static int CONSUME_ALL = -1;
    
    
    private static final Log LOG = LogFactory.getLog(AMQ2102Test.class);
    
    private final static Map<Thread, Throwable> exceptions = new ConcurrentHashMap<Thread, Throwable>();
    
    private class Consumer implements Runnable, ExceptionListener {
        private ActiveMQConnectionFactory connectionFactory;
        private String name;
        private String queueName;
        private boolean running;
        private org.omg.CORBA.IntHolder startup;
        private Thread thread;
        private final int numToProcessPerIteration;

        Consumer(ActiveMQConnectionFactory connectionFactory, String queueName, org.omg.CORBA.IntHolder startup, int id, int numToProcess) {
            this.connectionFactory = connectionFactory;
            this.queueName = queueName;
            this.startup = startup;
            name = "Consumer-" + queueName + "-" + id;
            numToProcessPerIteration = numToProcess;
            thread = new Thread(this, name);
        }

        private String getClientId() {
            try {
                return InetAddress.getLocalHost().getHostName() + ":" + name;
            } catch (UnknownHostException e) {
                return "localhost:" + name;
            }
        }

        synchronized boolean isRunning() {
            return running;
        }

        void join() {
            try {
                thread.join(30000);
            } catch (InterruptedException e) {
                error("Interrupted waiting for " + name + " to stop", e);
            }
        }

        public void onException(JMSException e) {
            exceptions.put(Thread.currentThread(), e);
            error("JMS exception: ", e);
        }

        private void processMessage(Session session, MessageProducer producer, Message message) throws Exception {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;

                Destination replyQueue = textMessage.getJMSReplyTo();
                if (replyQueue != null) {
                    TextMessage reply = session.createTextMessage("reply-" + textMessage.getText());
                    
                    reply.setJMSCorrelationID(textMessage.getJMSCorrelationID());
                    
                    producer.send(replyQueue, reply);
                    debug("replied via " + replyQueue + " for message => " + textMessage.getText());
                } else {
                    debug("no reply to message => " + textMessage.getText());
                }
            } else {
                error("Consumer cannot process " + message.getClass().getSimpleName());
            }
        }

        private void processMessages() throws JMSException {
            ActiveMQConnection connection = null;

            try {
                connection = (ActiveMQConnection) connectionFactory.createConnection();

                RedeliveryPolicy policy = connection.getRedeliveryPolicy();

                policy.setMaximumRedeliveries(6);
                policy.setInitialRedeliveryDelay(1000);
                policy.setUseCollisionAvoidance(false);
                policy.setCollisionAvoidancePercent((short) 15);
                policy.setUseExponentialBackOff(false);
                policy.setBackOffMultiplier((short) 5);

                connection.setClientID(getClientId());
                connection.setExceptionListener(this);
                connection.start();

                processMessages(connection);
            } finally {
                connection.close();
                connection = null;
            }
        }

        private void processMessages(Connection connection) throws JMSException {
            Session session = null;
            try {
                session = connection.createSession(true, Session.SESSION_TRANSACTED);
                if (numToProcessPerIteration > 0) {
                    while(isRunning()) {
                        processMessages(session);
                    }
                } else {
                    processMessages(session);
                }
            } finally {
                if (session != null) {
                    session.close();
                }
            }
        }
        private void processMessages(Session session) throws JMSException {
            MessageConsumer consumer = null;

            try {
                consumer = session.createConsumer(session.createQueue(queueName), null);
                processMessages(session, consumer);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        private void processMessages(Session session, MessageConsumer consumer) throws JMSException {
            MessageProducer producer = null;

            try {
                producer = session.createProducer(null);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                processMessages(session, consumer, producer);
            } finally {
                if (producer != null) {
                    producer.close();
                }
            }
        }

        private void processMessages(Session session, MessageConsumer consumer, MessageProducer producer) throws JMSException {
            debug("waiting for messages...");
            if (startup != null) {
                synchronized (startup) {
                    startup.value--;
                    startup.notify();
                }
                startup = null;
            }
            int numToProcess = numToProcessPerIteration;
            do {
                Message message = consumer.receive(5000);

                if (message != null) {
                    try {
                        processMessage(session, producer, message);
                        session.commit();
                        numToProcess--;
                    } catch (Throwable t) {
                        error("message=" + message + " failure", t);
                        session.rollback();
                    }
                } else {
                    info("got null message on: " + numToProcess);
                }
            } while ((numToProcessPerIteration == CONSUME_ALL || numToProcess > 0) && isRunning());
        }

        public void run() {
            setRunning(true);
            
            while (isRunning()) {
                try {
                    processMessages();
                } catch (Throwable t) {
                    error("Unexpected consumer problem: ", t);
                }
            }
        }
        synchronized void setRunning(boolean running) {
            this.running = running;
        }

        void start() {
            thread.start();
        }
    }
    
    private class Producer implements ExceptionListener {
        private ActiveMQConnectionFactory connectionFactory;
        private String queueName;
        
        Producer(ActiveMQConnectionFactory connectionFactory, String queueName) {
            this.connectionFactory = connectionFactory;
            this.queueName = queueName;
        }
        
        void execute(String[] args) {
            try {
                sendMessages();
            } catch (Exception e) {
                error("Producer failed", e);
            }
        }

        private void sendMessages() throws JMSException {
            ActiveMQConnection connection = null;

            try {
                connection = (ActiveMQConnection) connectionFactory.createConnection();
                connection.setExceptionListener(this);
                connection.start();

                sendMessages(connection);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (JMSException e) {
                        error("Problem closing connection", e);
                    }
                }
            }
        }

        private void sendMessages(ActiveMQConnection connection) throws JMSException {
            Session session = null;

            try {
                session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                
                sendMessages(session);
            } catch (JMSException e) {
                e.printStackTrace();
                exceptions.put(Thread.currentThread(), e);
                if (session != null) {
                    session.rollback();
                }
            } finally {
                if (session != null) {
                    session.close();
                }
            }
        }

        private void sendMessages(Session session) throws JMSException {
            TemporaryQueue replyQueue = null;
            
            try {
                replyQueue = session.createTemporaryQueue();
                
                sendMessages(session, replyQueue);
            } finally {
                if (replyQueue != null) {
                    replyQueue.delete();
                }
            }
        }

        private void sendMessages(Session session, Destination replyQueue) throws JMSException {
            MessageConsumer consumer = null;
            
            try {
                consumer = session.createConsumer(replyQueue);
                sendMessages(session, replyQueue, consumer);
            } finally {
                consumer.close();
                session.commit();
            }
        }

        private void sendMessages(Session session, Destination replyQueue, int messageCount) throws JMSException {
            MessageProducer producer = null;

            try {
                producer = session.createProducer(session.createQueue(queueName));
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.setTimeToLive(0);
                producer.setPriority(Message.DEFAULT_PRIORITY);

                for (int i = 0; i < messageCount; i++) {
                    TextMessage message = session.createTextMessage("message#" + i);
                    message.setJMSReplyTo(replyQueue);
                    producer.send(message);
                }
            } finally {
                if (producer != null) {
                    producer.close();
                }
            }
        }

        private void sendMessages(final Session session, Destination replyQueue, MessageConsumer consumer) throws JMSException {
            final org.omg.CORBA.IntHolder messageCount = new org.omg.CORBA.IntHolder(MESSAGE_COUNT);
            consumer.setMessageListener(new MessageListener() {
                public void onMessage(Message reply) {
                    if (reply instanceof TextMessage) {
                        TextMessage textReply = (TextMessage) reply;
                        synchronized (messageCount) {
                            try {
                                debug("receive reply#" + messageCount.value + " " + textReply.getText());
                            } catch (JMSException e) {
                                error("Problem processing reply", e);
                            }
                            messageCount.value--;
                            if (messageCount.value % 200 == 0) {
                                // ack a bunch of replys
                                info("acking via session commit: messageCount=" + messageCount.value);
                                try {
                                    session.commit();
                                } catch (JMSException e) {
                                    error("Failed to commit with count: " + messageCount.value, e);
                                }
                            }
                            messageCount.notifyAll();
                        }
                    } else {
                        error("Producer cannot process " + reply.getClass().getSimpleName());
                    }
                }});

            sendMessages(session, replyQueue, messageCount.value);

            session.commit();
            
            synchronized (messageCount) {
                while (messageCount.value > 0) {
                    
                    
                    try {
                        messageCount.wait();
                    } catch (InterruptedException e) {
                        error("Interrupted waiting for replies", e);
                    }
                }
            }
            // outstanding replys
            session.commit();
            debug("All replies received...");
        }

        public void onException(JMSException exception) {
           LOG.error(exception);
           exceptions.put(Thread.currentThread(), exception);
        }
    }

    private static void debug(String message) {
        LOG.debug(message);
    }

    private static void info(String message) {
        LOG.info(message);
    }
    
    private static void error(String message) {
        LOG.error(message);
    }

    private static void error(String message, Throwable t) {
        t.printStackTrace();
        String msg = message + ": " + (t.getMessage() != null ? t.getMessage() : t.toString());
        LOG.error(msg, t);
        exceptions.put(Thread.currentThread(), t);
        fail(msg);
    }

    private ArrayList<Consumer> createConsumers(ActiveMQConnectionFactory connectionFactory, String queueName, 
            int max, int numToProcessPerConsumer) {
        ArrayList<Consumer> consumers = new ArrayList<Consumer>(max);
        org.omg.CORBA.IntHolder startup = new org.omg.CORBA.IntHolder(max);

        for (int id = 0; id < max; id++) {
            consumers.add(new Consumer(connectionFactory, queueName, startup, id, numToProcessPerConsumer));
        }
        for (Consumer consumer : consumers) {
            consumer.start();
        }
        synchronized (startup) {
            while (startup.value > 0) {
                try {
                    startup.wait();
                } catch (InterruptedException e) {
                    error("Interrupted waiting for consumers to start", e);
                }
            }
        }
        return consumers;
    }

    final BrokerService master = new BrokerService();
    BrokerService slave = new BrokerService();
    String masterUrl;

    public void setUp() throws Exception {
        setMaxTestTime(12 * 60 * 1000);
        setAutoFail(true);
        super.setUp();
        master.setUseShutdownHook(false);
        master.setBrokerName("Master");
        master.addConnector("tcp://localhost:0");
        master.deleteAllMessages();
        master.setWaitForSlave(true);
        
        Thread t = new Thread() {
            public void run() {
                try {
                    master.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions.put(Thread.currentThread(), e);
                }
            }
        };
        t.start();
        Thread.sleep(2000);
        masterUrl = master.getTransportConnectors().get(0).getConnectUri().toString(); 
        
        debug("masterUrl: " + masterUrl);
        slave.setUseShutdownHook(false);
        slave.setBrokerName("Slave");
        slave.deleteAllMessages();
        slave.addConnector("tcp://localhost:0");
        slave.setMasterConnectorURI(masterUrl);
        slave.start();
    }
    
    public void tearDown() throws Exception {
        master.stop();
        slave.stop();
        exceptions.clear();
    }
    
    public void testMasterSlaveBug() throws Exception {
        
        Thread.setDefaultUncaughtExceptionHandler(this);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + 
                masterUrl + ")?randomize=false");
        String queueName = "MasterSlaveBug";
        ArrayList<Consumer> consumers = createConsumers(connectionFactory, queueName, NUM_CONSUMERS, CONSUME_ALL);
        
        Producer producer = new Producer(connectionFactory, queueName);
        producer.execute(new String[]{});

        for (Consumer consumer : consumers) {
            consumer.setRunning(false);
        }
        
        for (Consumer consumer : consumers) {
            consumer.join();
        }
        assertTrue(exceptions.isEmpty());
    }

    
    public void testMasterSlaveBugWithStopStartConsumers() throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(this);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "failover:(" + masterUrl + ")?randomize=false");
        String queueName = "MasterSlaveBug";
        ArrayList<Consumer> consumers = createConsumers(connectionFactory,
                queueName, NUM_CONSUMERS, 10);

        Producer producer = new Producer(connectionFactory, queueName);
        producer.execute(new String[] {});

        for (Consumer consumer : consumers) {
            consumer.setRunning(false);
        }

        for (Consumer consumer : consumers) {
            consumer.join();
        }
        assertTrue(exceptions.isEmpty());
    }

    public void uncaughtException(Thread t, Throwable e) {
        error("" + t + e);
        exceptions.put(t,e);
    }
}
