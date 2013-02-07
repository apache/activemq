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
package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;


public class NetworkedSyncTest extends TestCase {

    // constants
    public static final int MESSAGE_COUNT = 10000; //100000;
    public final static String config = "org/apache/activemq/perf/networkSync.xml";
    public final static String broker1URL = "tcp://localhost:61616";
    public final static String broker2URL = "tcp://localhost:62616";
    private final String networkConnectorURL = "static://(" + broker2URL + ")";
    private static final Logger LOG = LoggerFactory.getLogger(NetworkedSyncTest.class);
    BrokerService broker1 = null;
    BrokerService broker2 = null;
    NetworkConnector connector = null;

    /**
     * @param name
     */
    public NetworkedSyncTest(String name) {
        super(name);
        LOG.info("Testcase started.");
    }

   public static void main(String args[]) {
       TestRunner.run(NetworkedSyncTest.class);
   }

    /**
     * @throws java.lang.Exception
     */
    @Override
    protected void setUp() throws Exception {
        LOG.info("setUp() called.");
        ClassPathXmlApplicationContext context1 = null;
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(config));
        assertNotNull(brokerFactory);

        /* start up first broker instance */
        try {
            // resolve broker1
            Thread.currentThread().setContextClassLoader(
                    NetworkedSyncTest.class.getClassLoader());
            context1 = new ClassPathXmlApplicationContext(config);
            broker1 = (BrokerService) context1.getBean("broker1");

            // start the broker
            if (!broker1.isStarted()) {
                LOG.info("Broker broker1 not yet started. Kicking it off now.");
                broker1.start();
            } else {
                LOG.info("Broker broker1 already started. Not kicking it off a second time.");
                broker1.waitUntilStopped();
            }
        } catch (Exception e) {
            LOG.error("Error: " + e.getMessage());
            throw e;
            // brokerService.stop();
        }

        /* start up second broker instance */
        try {
            Thread.currentThread().setContextClassLoader(
                    NetworkedSyncTest.class.getClassLoader());
            context1 = new ClassPathXmlApplicationContext(config);
            broker2 = (BrokerService) context1.getBean("broker2");

            // start the broker
            if (!broker2.isStarted()) {
                LOG.info("Broker broker2 not yet started. Kicking it off now.");
                broker2.start();
            } else {
                LOG.info("Broker broker2 already started. Not kicking it off a second time.");
                broker2.waitUntilStopped();
            }
        } catch (Exception e) {
            LOG.error("Error: " + e.getMessage());
            throw e;
        }

        // setup network connector from broker1 to broker2
        connector = broker1.addNetworkConnector(networkConnectorURL);
        connector.setBrokerName(broker1.getBrokerName());
        connector.setDuplex(true);
        connector.start();
        LOG.info("Network connector created.");
    }

    /**
     * @throws java.lang.Exception
     */
    @Override
    protected void tearDown() throws Exception {

        LOG.info("tearDown() called.");

        if (broker1 != null && broker1.isStarted()) {
            LOG.info("Broker1 still running, stopping it now.");
            broker1.stop();
        } else {
            LOG.info("Broker1 not running, nothing to shutdown.");
        }
        if (broker2 != null && broker2.isStarted()) {
            LOG.info("Broker2 still running, stopping it now.");
            broker2.stop();
        } else {
            LOG.info("Broker2 not running, nothing to shutdown.");
        }

    }

    public void testMessageExchange() throws Exception {
        LOG.info("testMessageExchange() called.");

        long start = System.currentTimeMillis();

        // create producer and consumer threads
        Thread producer = new Thread(new Producer());
        Thread consumer = new Thread(new Consumer());
        // start threads
        consumer.start();
        Thread.sleep(2000);
        producer.start();


        // wait for threads to finish
        producer.join();
        consumer.join();
        long end = System.currentTimeMillis();

        System.out.println("Duration: "+(end-start));
    }
}

/**
 * Message producer running as a separate thread, connecting to broker1
 *
 * @author tmielke
 *
 */
class Producer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    /**
     * connect to broker and constantly send messages
     */
    @Override
    public void run() {

        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
                    NetworkedSyncTest.broker1URL);
            connection = amq.createConnection();

            connection.setExceptionListener(new javax.jms.ExceptionListener() {
                @Override
                public void onException(javax.jms.JMSException e) {
                    e.printStackTrace();
                }
            });

            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic("TEST.FOO");

            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            long counter = 0;

            // Create and send message
            for (int i = 0; i < NetworkedSyncTest.MESSAGE_COUNT; i++) {

                String text = "Hello world! From: "
                        + Thread.currentThread().getName() + " : "
                        + this.hashCode() + ":" + counter;
                TextMessage message = session.createTextMessage(text);
                producer.send(message);
                counter++;

                if ((counter % 1000) == 0)
                    LOG.info("sent " + counter + " messages");

            }
        } catch (Exception ex) {
            LOG.error(ex.toString());
            return;
        } finally {
            try {
                if (producer != null)
                    producer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                LOG.error("Problem closing down JMS objects: " + e);
            }
        }
    }
}

/*
 * * Message consumer running as a separate thread, connecting to broker2
 * @author tmielke
 *
 */
class Consumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);;


    /**
     * connect to broker and receive messages
     */
    @Override
    public void run() {
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(
                    NetworkedSyncTest.broker2URL);
            connection = amq.createConnection();
            // need to set clientID when using durable subscription.
            connection.setClientID("tmielke");

            connection.setExceptionListener(new javax.jms.ExceptionListener() {
                @Override
                public void onException(javax.jms.JMSException e) {
                    e.printStackTrace();
                }
            });

            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic("TEST.FOO");
            consumer = session.createDurableSubscriber((Topic) destination,"tmielke");

            long counter = 0;
            // Wait for a message
            for (int i = 0; i < NetworkedSyncTest.MESSAGE_COUNT; i++) {
                Message message2 = consumer.receive();
                if (message2 instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message2;
                    textMessage.getText();
                    // logger.info("Received: " + text);
                } else {
                    LOG.error("Received message of unsupported type. Expecting TextMessage. "+ message2);
                }
                counter++;
                if ((counter % 1000) == 0)
                    LOG.info("received " + counter + " messages");


            }
        } catch (Exception e) {
            LOG.error("Error in Consumer: " + e);
            return;
        } finally {
            try {
                if (consumer != null)
                    consumer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception ex) {
                LOG.error("Error closing down JMS objects: " + ex);
            }
        }
    }
}
