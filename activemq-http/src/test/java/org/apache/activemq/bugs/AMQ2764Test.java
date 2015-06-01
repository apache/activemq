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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.ConsumerEvent;
import org.apache.activemq.advisory.ConsumerEventSource;
import org.apache.activemq.advisory.ConsumerListener;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.http.WaitForJettyListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2764Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2764Test.class);

    @Rule public TestName name = new TestName();

    private BrokerService brokerOne;
    private BrokerService brokerTwo;
    private Destination destination;
    private final ArrayList<Connection> connections = new ArrayList<Connection>();

    @Test(timeout = 60000)
    public void testInactivityMonitor() throws Exception {

        startBrokerTwo();
        brokerTwo.waitUntilStarted();

        startBrokerOne();
        brokerOne.waitUntilStarted();

        ActiveMQConnectionFactory secondProducerConnectionFactory = createBrokerTwoHttpConnectionFactory();
        ActiveMQConnectionFactory consumerConnectionFactory = createBrokerOneHttpConnectionFactory();

        MessageConsumer consumer = createConsumer(consumerConnectionFactory);
        AtomicInteger counter = createConsumerCounter(consumerConnectionFactory);
        waitForConsumerToArrive(counter);

        Connection connection = secondProducerConnectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        final int expectedMessagesReceived = 1000;

        for (int i = 1; i <= expectedMessagesReceived; i++) {
            Message message = session.createMessage();
            producer.send(message);
            if (i % 200 == 0) {
                LOG.info("sent message " + i);
            }
        }

        for (int i = 1; i <= expectedMessagesReceived; i++) {
            Message message = consumer.receive(2000);
            if (message == null) {
                fail("Didn't receive a message");
            }
            if (i % 200 == 0) {
                LOG.info("received message " + i);
            }
        }
    }

    @Test(timeout = 60000)
    public void testBrokerRestart() throws Exception {
        startBrokerTwo();
        brokerTwo.waitUntilStarted();

        startBrokerOne();
        brokerOne.waitUntilStarted();

        ActiveMQConnectionFactory producerConnectionFactory = createBrokerOneConnectionFactory();
        ActiveMQConnectionFactory secondProducerConnectionFactory = createBrokerTwoConnectionFactory();
        ActiveMQConnectionFactory consumerConnectionFactory = createBrokerOneConnectionFactory();

        MessageConsumer consumer = createConsumer(consumerConnectionFactory);
        AtomicInteger counter = createConsumerCounter(consumerConnectionFactory);
        waitForConsumerToArrive(counter);

        final int expectedMessagesReceived = 25;
        int actualMessagesReceived = doSendMessage(expectedMessagesReceived, consumer, producerConnectionFactory);
        assertEquals("Didn't receive the right amount of messages directly connected", expectedMessagesReceived, actualMessagesReceived);
        assertNull("Had extra messages", consumer.receiveNoWait());

        actualMessagesReceived = doSendMessage(expectedMessagesReceived, consumer, secondProducerConnectionFactory);
        assertEquals("Didn't receive the right amount of messages via network", expectedMessagesReceived, actualMessagesReceived);
        assertNull("Had extra messages", consumer.receiveNoWait());

        LOG.info("Stopping broker one");
        stopBrokerOne();

        TimeUnit.SECONDS.sleep(1);
        LOG.info("Restarting broker");
        startBrokerOne();

        consumer = createConsumer(consumerConnectionFactory);
        counter = createConsumerCounter(consumerConnectionFactory);
        waitForConsumerToArrive(counter);

        actualMessagesReceived = doSendMessage(expectedMessagesReceived, consumer, secondProducerConnectionFactory);
        assertEquals("Didn't receive the right amount of messages via network after restart", expectedMessagesReceived, actualMessagesReceived);
        assertNull("Had extra messages", consumer.receiveNoWait());

        stopBrokerOne();
        stopBrokerTwo();
    }

    protected int doSendMessage(int expectedMessagesReceived, MessageConsumer consumer, ActiveMQConnectionFactory connectionFactory) throws Exception {
        int messagesReceived = 0;
        for (int i = 0; i < expectedMessagesReceived; i++) {
            sendMessage(connectionFactory);
            Message message = consumer.receive(5000);
            if (message != null) {
                messagesReceived++;
            }
        }
        return messagesReceived;
    }

    protected String sendMessage(ActiveMQConnectionFactory connectionFactory) throws JMSException {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            Message message = session.createMessage();
            producer.send(message);
            return message.getJMSMessageID();
        } finally {
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected BrokerService createFirstBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/bugs/amq2764/reconnect-broker1.xml"));
    }

    protected BrokerService createSecondBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/bugs/amq2764/reconnect-broker2.xml"));
    }

    protected ActiveMQConnectionFactory createBrokerOneConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker1?create=false");
    }

    protected ActiveMQConnectionFactory createBrokerTwoConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker2?create=false");
    }

    protected ActiveMQConnectionFactory createBrokerOneHttpConnectionFactory() {
        return new ActiveMQConnectionFactory("http://localhost:61616");
    }

    protected ActiveMQConnectionFactory createBrokerTwoHttpConnectionFactory() {
        return new ActiveMQConnectionFactory("http://localhost:61617");
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("===== Starting test {} ================", getTestName());
        destination = new ActiveMQQueue("RECONNECT.TEST.QUEUE");
    }

    @After
    public void tearDown() throws Exception {
        disposeConsumerConnections();
        Thread.sleep(10);
        try {
            stopBrokerOne();
        } catch (Throwable e) {
        }
        try {
            stopBrokerTwo();
        } catch (Throwable e) {
        }

        LOG.info("===== Finished test {} ================", getTestName());
    }

    protected String getTestName() {
        return name.getMethodName();
    }

    protected void disposeConsumerConnections() {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection connection = iter.next();
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected void startBrokerOne() throws Exception {
        if (brokerOne == null) {
            brokerOne = createFirstBroker();
            brokerOne.start();
            brokerOne.waitUntilStarted();
            WaitForJettyListener.waitForJettySocketToAccept("http://localhost:61616");
        }
    }

    protected void stopBrokerOne() throws Exception {
        if (brokerOne != null) {
            brokerOne.stop();
            brokerOne = null;
        }
    }

    protected void startBrokerTwo() throws Exception {
        if (brokerTwo == null) {
            brokerTwo = createSecondBroker();
            brokerTwo.start();
            brokerTwo.waitUntilStarted();
            WaitForJettyListener.waitForJettySocketToAccept("http://localhost:61617");
        }
    }

    protected void stopBrokerTwo() throws Exception {
        if (brokerTwo != null) {
            brokerTwo.stop();
            brokerTwo = null;
        }
    }

    protected MessageConsumer createConsumer(ActiveMQConnectionFactory consumerConnectionFactory) throws JMSException {
        Connection connection = consumerConnectionFactory.createConnection();
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return session.createConsumer(destination);
    }

    protected AtomicInteger createConsumerCounter(ActiveMQConnectionFactory cf) throws Exception {
        final AtomicInteger rc = new AtomicInteger(0);
        Connection connection = cf.createConnection();
        connections.add(connection);
        connection.start();

        ConsumerEventSource source = new ConsumerEventSource(connection, destination);
        source.setConsumerListener(new ConsumerListener() {
            @Override
            public void onConsumerEvent(ConsumerEvent event) {
                rc.set(event.getConsumerCount());
            }
        });
        source.start();

        return rc;
    }

    protected void waitForConsumerToArrive(AtomicInteger consumerCounter) throws InterruptedException {
        for (int i = 0; i < 200; i++) {
            if (consumerCounter.get() > 0) {
                return;
            }
            Thread.sleep(50);
        }

        fail("The consumer did not arrive.");
    }

    protected void waitForConsumerToLeave(AtomicInteger consumerCounter) throws InterruptedException {
        for (int i = 0; i < 200; i++) {
            if (consumerCounter.get() == 0) {
                return;
            }
            Thread.sleep(50);
        }

        fail("The consumer did not leave.");
    }
}
