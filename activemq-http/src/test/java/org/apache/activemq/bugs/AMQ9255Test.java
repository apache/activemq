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

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.http.WaitForJettyListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AMQ9255Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ9255Test.class);

    @Rule
    public TestName name = new TestName();
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection sendConnection, receiveConnection;
    private Session sendSession, receiveSession;
    private MessageConsumer consumer;
    private MessageProducer producer;

    @Before
    public void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
            broker.start();
        }
        WaitForJettyListener.waitForJettySocketToAccept(getBrokerURL());
        connectionFactory = createConnectionFactory();
        LOG.info("Creating send connection");
        sendConnection = createSendConnection();
        LOG.info("Starting send connection");
        sendConnection.start();

        LOG.info("Creating receive connection");
        receiveConnection = createReceiveConnection();
        LOG.info("Starting receive connection");
        receiveConnection.start();

        sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        receiveSession = receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("Created sendSession: " + sendSession);
        LOG.info("Created receiveSession: " + receiveSession);

        producer = sendSession.createProducer(sendSession.createQueue(getProducerSubject()));
        consumer = receiveSession.createConsumer(receiveSession.createQueue(getConsumerSubject()));

        LOG.info("Created consumer of type: " + consumer.getClass());
        LOG.info("Created producer of type: " + producer.getClass());
    }

    @After
    public void tearDown() throws Exception {
        if (receiveSession != null) {
            receiveSession.close();
        }
        if (sendSession != null) {
            sendSession.close();
        }
        if (receiveConnection != null) {
            receiveConnection.close();
        }
        if (sendConnection != null) {
            sendConnection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    private String getConsumerSubject() {
        return "ActiveMQ.DLQ";
    }

    private String getProducerSubject() {
        return name.getMethodName();
    }

    private Connection createReceiveConnection() throws Exception {
        return connectionFactory.createConnection();
    }

    private Connection createSendConnection() throws Exception {
        return connectionFactory.createConnection();
    }

    private ActiveMQConnectionFactory createConnectionFactory() {
        return new ActiveMQConnectionFactory(getBrokerURL());
    }

    protected String getBrokerURL() {
        return "http://localhost:8161";
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.addConnector(getBrokerURL());
        answer.setUseJmx(false);
        return answer;
    }

    @Test
    public void testExpiredMessages() throws Exception {
        // Given
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.setTimeToLive(100L);
        String text = name.toString();

        // When
        producer.send(sendSession.createTextMessage(text));

        // Then
        TextMessage message = (TextMessage) consumer.receive(30_000);
        assertNotNull(message);
        assertEquals(text, message.getText());
    }
}
