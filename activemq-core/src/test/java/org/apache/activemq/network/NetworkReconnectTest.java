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
package org.apache.activemq.network;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.ConsumerEvent;
import org.apache.activemq.advisory.ConsumerEventSource;
import org.apache.activemq.advisory.ConsumerListener;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * These test cases are used to verifiy that network connections get re
 * established in all broker restart scenarios.
 * 
 * @author chirino
 */
public class NetworkReconnectTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(NetworkReconnectTest.class);

    private BrokerService producerBroker;
    private BrokerService consumerBroker;
    private ActiveMQConnectionFactory producerConnectionFactory;
    private ActiveMQConnectionFactory consumerConnectionFactory;
    private Destination destination;
    private ArrayList connections = new ArrayList();

    public void testMultipleProducerBrokerRestarts() throws Exception {
        for (int i = 0; i < 10; i++) {
            testWithProducerBrokerRestart();
            disposeConsumerConnections();
        }
    }

    public void testWithoutRestarts() throws Exception {
        startProducerBroker();
        startConsumerBroker();

        MessageConsumer consumer = createConsumer();
        AtomicInteger counter = createConsumerCounter(producerConnectionFactory);
        waitForConsumerToArrive(counter);

        String messageId = sendMessage();
        Message message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());

        assertNull(consumer.receiveNoWait());

    }

    public void testWithProducerBrokerRestart() throws Exception {
        startProducerBroker();
        startConsumerBroker();

        MessageConsumer consumer = createConsumer();
        AtomicInteger counter = createConsumerCounter(producerConnectionFactory);
        waitForConsumerToArrive(counter);

        String messageId = sendMessage();
        Message message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());
        assertNull(consumer.receiveNoWait());

        // Restart the first broker...
        stopProducerBroker();
        startProducerBroker();

        counter = createConsumerCounter(producerConnectionFactory);
        waitForConsumerToArrive(counter);

        messageId = sendMessage();
        message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());
        assertNull(consumer.receiveNoWait());

    }

    public void testWithConsumerBrokerRestart() throws Exception {

        startProducerBroker();
        startConsumerBroker();

        MessageConsumer consumer = createConsumer();
        AtomicInteger counter = createConsumerCounter(producerConnectionFactory);
        waitForConsumerToArrive(counter);

        String messageId = sendMessage();
        Message message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());
        assertNull(consumer.receiveNoWait());

        // Restart the first broker...
        stopConsumerBroker();
        waitForConsumerToLeave(counter);
        startConsumerBroker();

        consumer = createConsumer();
        waitForConsumerToArrive(counter);

        messageId = sendMessage();
        message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());
        assertNull(consumer.receiveNoWait());

    }

    public void testWithConsumerBrokerStartDelay() throws Exception {

        startConsumerBroker();
        MessageConsumer consumer = createConsumer();

        Thread.sleep(1000 * 5);

        startProducerBroker();
        AtomicInteger counter = createConsumerCounter(producerConnectionFactory);
        waitForConsumerToArrive(counter);

        String messageId = sendMessage();
        Message message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());

        assertNull(consumer.receiveNoWait());

    }

    public void testWithProducerBrokerStartDelay() throws Exception {

        startProducerBroker();
        AtomicInteger counter = createConsumerCounter(producerConnectionFactory);

        Thread.sleep(1000 * 5);

        startConsumerBroker();
        MessageConsumer consumer = createConsumer();

        waitForConsumerToArrive(counter);

        String messageId = sendMessage();
        Message message = consumer.receive(1000);

        assertEquals(messageId, message.getJMSMessageID());

        assertNull(consumer.receiveNoWait());

    }

    protected void setUp() throws Exception {

        LOG.info("===============================================================================");
        LOG.info("Running Test Case: " + getName());
        LOG.info("===============================================================================");

        producerConnectionFactory = createProducerConnectionFactory();
        consumerConnectionFactory = createConsumerConnectionFactory();
        destination = new ActiveMQQueue("RECONNECT.TEST.QUEUE");

    }

    protected void tearDown() throws Exception {
        disposeConsumerConnections();
        try {
            stopProducerBroker();
        } catch (Throwable e) {
        }
        try {
            stopConsumerBroker();
        } catch (Throwable e) {
        }
    }

    protected void disposeConsumerConnections() {
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            Connection connection = (Connection)iter.next();
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    protected void startProducerBroker() throws Exception {
        if (producerBroker == null) {
            producerBroker = createFirstBroker();
            producerBroker.start();
        }
    }

    protected void stopProducerBroker() throws Exception {
        if (producerBroker != null) {
            producerBroker.stop();
            producerBroker = null;
        }
    }

    protected void startConsumerBroker() throws Exception {
        if (consumerBroker == null) {
            consumerBroker = createSecondBroker();
            consumerBroker.start();
        }
    }

    protected void stopConsumerBroker() throws Exception {
        if (consumerBroker != null) {
            consumerBroker.stop();
            consumerBroker = null;
        }
    }

    protected BrokerService createFirstBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/network/reconnect-broker1.xml"));
    }

    protected BrokerService createSecondBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/network/reconnect-broker2.xml"));
    }

    protected ActiveMQConnectionFactory createProducerConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker1");
    }

    protected ActiveMQConnectionFactory createConsumerConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker2");
    }

    protected String sendMessage() throws JMSException {
        Connection connection = null;
        try {
            connection = producerConnectionFactory.createConnection();
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

    protected MessageConsumer createConsumer() throws JMSException {
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
            public void onConsumerEvent(ConsumerEvent event) {
                rc.set(event.getConsumerCount());
            }
        });
        source.start();

        return rc;
    }

    protected void waitForConsumerToArrive(AtomicInteger consumerCounter) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (consumerCounter.get() > 0) {
                return;
            }
            Thread.sleep(100);
        }
        fail("The consumer did not arrive.");
    }

    protected void waitForConsumerToLeave(AtomicInteger consumerCounter) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (consumerCounter.get() == 0) {
                return;
            }
            Thread.sleep(100);
        }
        fail("The consumer did not leave.");
    }

}
