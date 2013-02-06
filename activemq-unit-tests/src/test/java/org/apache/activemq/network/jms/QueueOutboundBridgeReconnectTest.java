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
package org.apache.activemq.network.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * These test cases are used to verify that queue outbound bridge connections get
 * re-established in all broker restart scenarios. This is possible when the
 * outbound bridge is configured using the failover URI with a timeout.
 */
public class QueueOutboundBridgeReconnectTest {

    private BrokerService producerBroker;
    private BrokerService consumerBroker;
    private ActiveMQConnectionFactory producerConnectionFactory;
    private ActiveMQConnectionFactory consumerConnectionFactory;
    private Destination destination;
    private final ArrayList<Connection> connections = new ArrayList<Connection>();

    @Test
    public void testMultipleProducerBrokerRestarts() throws Exception {
        for (int i = 0; i < 10; i++) {
            testWithProducerBrokerRestart();
            disposeConsumerConnections();
        }
    }

    @Test
    public void testRestartProducerWithNoConsumer() throws Exception {
        stopConsumerBroker();

        startProducerBroker();
        sendMessage("test123");
        sendMessage("test456");
    }

    @Test
    public void testWithoutRestartsConsumerFirst() throws Exception {
        startConsumerBroker();
        startProducerBroker();
        sendMessage("test123");
        sendMessage("test456");

        MessageConsumer consumer = createConsumer();
        Message message = consumer.receive(3000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage)message).getText());

        message = consumer.receive(3000);
        assertNotNull(message);
        assertEquals("test456", ((TextMessage)message).getText());

        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testWithoutRestartsProducerFirst() throws Exception {
        startProducerBroker();
        sendMessage("test123");

        startConsumerBroker();

        // unless using a failover URI, the first attempt of this send will likely fail,
        // so increase the timeout below to give the bridge time to recover
        sendMessage("test456");

        MessageConsumer consumer = createConsumer();
        Message message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage) message).getText());

        message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals("test456", ((TextMessage) message).getText());

        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testWithProducerBrokerRestart() throws Exception {
        startProducerBroker();
        startConsumerBroker();

        sendMessage("test123");

        MessageConsumer consumer = createConsumer();
        Message message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage)message).getText());
        assertNull(consumer.receiveNoWait());

        // Restart the first broker...
        stopProducerBroker();
        startProducerBroker();

        sendMessage("test123");
        message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage)message).getText());
        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testWithConsumerBrokerRestart() throws Exception {

        startProducerBroker();
        startConsumerBroker();

        sendMessage("test123");

        final MessageConsumer consumer1 = createConsumer();
        Message message = consumer1.receive(5000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage)message).getText());
        assertNull(consumer1.receiveNoWait());
        consumer1.close();

        // Restart the first broker...
        stopConsumerBroker();
        startConsumerBroker();

        // unless using a failover URI, the first attempt of this send will likely fail,
        // so increase the timeout below to give the bridge time to recover
        sendMessage("test123");

        final MessageConsumer consumer2 = createConsumer();
        assertTrue("Expected recover and delivery failed", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                Message message = consumer2.receiveNoWait();
                if (message == null || !((TextMessage)message).getText().equals("test123")) {
                    return false;
                }
                return true;
            }
        }));
        assertNull(consumer2.receiveNoWait());
    }

    @Test
    public void testWithConsumerBrokerStartDelay() throws Exception {

        startConsumerBroker();
        final MessageConsumer consumer = createConsumer();

        TimeUnit.SECONDS.sleep(5);

        startProducerBroker();

        sendMessage("test123");
        assertTrue("Expected recover and delivery failed", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                Message message = consumer.receiveNoWait();
                if (message == null || !((TextMessage)message).getText().equals("test123")) {
                    return false;
                }
                return true;
            }
        }));
        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testWithProducerBrokerStartDelay() throws Exception {

        startProducerBroker();

        TimeUnit.SECONDS.sleep(5);

        startConsumerBroker();
        MessageConsumer consumer = createConsumer();

        sendMessage("test123");
        Message message = consumer.receive(5000);
        assertNotNull(message);
        assertEquals("test123", ((TextMessage)message).getText());
        assertNull(consumer.receiveNoWait());
    }

    @Before
    public void setUp() throws Exception {

        producerConnectionFactory = createProducerConnectionFactory();
        consumerConnectionFactory = createConsumerConnectionFactory();
        destination = new ActiveMQQueue("RECONNECT.TEST.QUEUE");
    }

    @After
    public void tearDown() throws Exception {
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
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection connection = iter.next();
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
        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker1");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61616");
        broker.addConnector("vm://broker1");

        SimpleJmsQueueConnector jmsQueueConnector = new SimpleJmsQueueConnector();
        jmsQueueConnector.setOutboundQueueBridges(
            new OutboundQueueBridge[] {new OutboundQueueBridge("RECONNECT.TEST.QUEUE")});
        jmsQueueConnector.setOutboundQueueConnectionFactory(
            new ActiveMQConnectionFactory("tcp://localhost:61617"));

        broker.setJmsBridgeConnectors(new JmsConnector[]{jmsQueueConnector});

        return broker;
    }

    protected BrokerService createSecondBroker() throws Exception {

        BrokerService broker = new BrokerService();
        broker.setBrokerName("broker2");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61617");
        broker.addConnector("vm://broker2");

        return broker;
    }

    protected ActiveMQConnectionFactory createProducerConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker1");
    }

    protected ActiveMQConnectionFactory createConsumerConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://broker2");
    }

    protected void sendMessage(String text) throws JMSException {
        Connection connection = null;
        try {
            connection = producerConnectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText(text);
            producer.send(message);
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
}
