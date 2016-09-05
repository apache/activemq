/*
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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class AmqpAndStompInteropTest {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAndStompInteropTest.class);

    @Rule
    public TestName name = new TestName();

    protected BrokerService broker;
    private TransportConnector amqpConnector;
    private TransportConnector stompConnector;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);

        amqpConnector = broker.addConnector("amqp://0.0.0.0:0");
        stompConnector = broker.addConnector("stomp://0.0.0.0:0");

        return broker;
    }

    @Test(timeout = 30000)
    public void testSendFromAMQPToSTOMP() throws Exception {
        sendMessageToQueueUsingAmqp();
        readMessageFromQueueUsingStomp();
    }

    @Test(timeout = 30000)
    public void testSendFromSTOMPToAMQP() throws Exception {
        sendMessageToQueueUsingStomp();
        readMessageFromQueueUsingAmqp();
    }

    @Test(timeout = 30000)
    public void testSendFromSTOMPToSTOMP() throws Exception {
        sendMessageToQueueUsingStomp();
        readMessageFromQueueUsingStomp();
    }

    @Test(timeout = 30000)
    public void testSendFromAMQPToAMQP() throws Exception {
        sendMessageToQueueUsingAmqp();
        readMessageFromQueueUsingAmqp();
    }

    private String getQueueName() {
        return name.getMethodName() + "-Queue";
    }

    private void sendMessageToQueueUsingAmqp() throws Exception {
        Connection connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageProducer producer = session.createProducer(queue);

        try {
            TextMessage message = session.createTextMessage("test-message-amqp-source");
            producer.send(message);

            LOG.info("Send AMQP message with Message ID -> {}", message.getJMSMessageID());
        } finally {
            connection.close();
        }
    }

    private void sendMessageToQueueUsingStomp() throws Exception {
        Connection connection = createStompConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageProducer producer = session.createProducer(queue);

        try {
            TextMessage message = session.createTextMessage("test-message-stomp-source");
            producer.send(message);

            LOG.info("Send STOMP message with Message ID -> {}", message.getJMSMessageID());
        } finally {
            connection.close();
        }
    }

    private void readMessageFromQueueUsingAmqp() throws Exception {
        Connection connection = createAmqpConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageConsumer consumer = session.createConsumer(queue);

        connection.start();

        Message received = consumer.receive(2000);
        assertNotNull(received);

        LOG.info("Read from AMQP -> message ID = {}", received.getJMSMessageID());

        assertTrue(received instanceof TextMessage);

        TextMessage textMessage = (TextMessage) received;
        assertNotNull(textMessage.getText());
    }

    private void readMessageFromQueueUsingStomp() throws Exception {
        Connection connection = createStompConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getQueueName());
        MessageConsumer consumer = session.createConsumer(queue);

        connection.start();

        Message received = consumer.receive(2000);
        assertNotNull(received);

        LOG.info("Read from STOMP -> message ID = {}", received.getJMSMessageID());

        assertTrue(received instanceof TextMessage);

        TextMessage textMessage = (TextMessage) received;
        assertNotNull(textMessage.getText());
    }

    private Connection createStompConnection() throws Exception {

        String stompURI = "tcp://localhost:" + stompConnector.getConnectUri().getPort();

        final StompJmsConnectionFactory factory = new StompJmsConnectionFactory();

        factory.setBrokerURI(stompURI);
        factory.setUsername("admin");
        factory.setPassword("password");

        final Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });

        connection.start();
        return connection;
    }

    private Connection createAmqpConnection() throws Exception {

        String amqpURI = "amqp://localhost:" + amqpConnector.getConnectUri().getPort();

        final JmsConnectionFactory factory = new JmsConnectionFactory(amqpURI);

        factory.setUsername("admin");
        factory.setPassword("password");

        final Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });

        connection.start();
        return connection;
    }
}
