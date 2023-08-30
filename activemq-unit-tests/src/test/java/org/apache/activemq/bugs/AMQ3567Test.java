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

import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.cursors.AbstractStoreCursor;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 *
 */
public class AMQ3567Test {

    private static Logger logger = LoggerFactory.getLogger(AMQ3567Test.class);

    private ActiveMQConnectionFactory factory;
    private Connection connection;
    private Session sessionWithListener, session;
    private Queue destination;
    private MessageConsumer consumer;
    private Thread thread;
    private BrokerService broker;
    private String connectionUri;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        startBroker();
        initializeConsumer();
        startConsumer();
    }

    @Test
    public void runTest() throws Exception {
        produceSingleMessage();
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger("org.apache.activemq.util.ServiceSupport"));

        final AtomicBoolean failed = new AtomicBoolean(false);
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getThrown() != null) {
                    if (event.getThrown() instanceof InterruptedException) {
                        InterruptedException ie = (InterruptedException)event.getThrown();
                        if (ie.getMessage().startsWith("Could not stop service:")) {
                            logger.info("Received an interrupted exception : ", ie);
                            failed.set(true);
                        }
                    }
                }
            }
        };
        appender.start();

        Level level = logger.getLevel();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
      
        try {
            stopConsumer();
            stopBroker();
            if (failed.get()) {
                fail("An Interrupt exception was generated");
            }

        } finally {
            logger.setLevel(level);
            logger.removeAppender(appender);
        }
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target/data");
        connectionUri = broker.addConnector("tcp://localhost:0?wireFormat.maxInactivityDuration=30000&transport.closeAsync=false&transport.threadName&soTimeout=60000&transport.keepAlive=false&transport.useInactivityMonitor=false").getPublishableConnectString();
        broker.start(true);
        broker.waitUntilStarted();
    }

    private void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    private void initializeConsumer() throws JMSException {
        logger.info("Initializing the consumer messagor that will just not do anything....");
        factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL("failover:("+connectionUri+"?wireFormat.maxInactivityDuration=30000&keepAlive=true&soTimeout=60000)?jms.watchTopicAdvisories=false&jms.useAsyncSend=false&jms.dispatchAsync=true&jms.producerWindowSize=10485760&jms.copyMessageOnSend=false&jms.disableTimeStampsByDefault=true&InitialReconnectDelay=1000&maxReconnectDelay=10000&maxReconnectAttempts=400&useExponentialBackOff=true");
        connection = factory.createConnection();
        connection.start();
        sessionWithListener = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = sessionWithListener.createQueue("EMPTY.QUEUE");
    }

    private void startConsumer() throws Exception {
        logger.info("Starting the consumer");
        consumer = sessionWithListener.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                logger.info("Received a message: " + message);
            }

        });

        thread = new Thread(new Runnable() {

            private Session session;

            @Override
            public void run() {
                try {
                    destination = session.createQueue("EMPTY.QUEUE");
                    MessageConsumer consumer = session.createConsumer(destination);
                    for (int cnt = 0; cnt < 2; cnt++) {
                        Message message = consumer.receive(50000);
                        logger.info("Received message: " + message);
                    }
                } catch (JMSException e) {
                    logger.debug("Received an exception while processing messages", e);
                } finally {
                    try {
                        session.close();
                    } catch (JMSException e) {
                        logger.debug("Received an exception while closing session", e);
                    }
                }
            }

            public Runnable setSession(Session session) {
                this.session = session;
                return this;
            }

        }.setSession(session)) {
            {
                start();
            }
        };
    }

    private void stopConsumer() throws JMSException {
        logger.info("Stopping the consumer");
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.debug("Received an exception while waiting for thread to complete", e);
        }
        if (sessionWithListener != null) {
            sessionWithListener.close();
        }
        if (connection != null) {
            connection.stop();
        }
    }

    private void produceSingleMessage() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(connectionUri);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("EMPTY.QUEUE");
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage("Single Message"));
        producer.close();
        session.close();
        connection.close();
    }
}
