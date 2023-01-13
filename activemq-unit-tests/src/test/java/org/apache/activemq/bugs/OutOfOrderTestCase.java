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
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfOrderTestCase extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderTestCase.class);

    private static final String BROKER_URL = "tcp://localhost:0";
    private static final int PREFETCH = 10;
    private static final String CONNECTION_URL_OPTIONS = "?jms.prefetchPolicy.all=" + PREFETCH;

    private static final String DESTINATION = "QUEUE?consumer.exclusive=true";

    private BrokerService brokerService;
    private Session session;
    private Connection connection;
    private String connectionUri;

    private int seq = 0;

    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(true);
        brokerService.addConnector(BROKER_URL);
        brokerService.deleteAllMessages();
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionUri = brokerService.getTransportConnectors().get(0).getPublishableConnectString();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri + CONNECTION_URL_OPTIONS);
        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    protected void tearDown() throws Exception {
        try {
            session.close();
            connection.close();
        } catch (Exception e) {
            //swallow any error so broker can still be stopped
        }
        brokerService.stop();
    }

    public void testOrder() throws Exception {

        log.info("Producing messages 0-29 . . .");
        Destination destination = session.createQueue(DESTINATION);
        final MessageProducer messageProducer = session
                .createProducer(destination);
        try {
            for (int i = 0; i < 30; ++i) {
                final Message message = session
                        .createTextMessage(createMessageText(i));
                message.setStringProperty("JMSXGroupID", "FOO");

                messageProducer.send(message);
                log.info("sent " + toString(message));
            }
        } finally {
            messageProducer.close();
        }

        log.info("Consuming messages 0-9 . . .");
        consumeBatch();

        log.info("Consuming messages 10-19 . . .");
        consumeBatch();

        log.info("Consuming messages 20-29 . . .");
        consumeBatch();
    }

    protected void consumeBatch() throws Exception {
        Destination destination = session.createQueue(DESTINATION);
        final MessageConsumer messageConsumer = session.createConsumer(destination);
        try {
            for (int i = 0; i < 10; ++i) {
                final Message message = messageConsumer.receive(1000L);
                log.info("received " + toString(message));
                assertEquals("Message out of order", createMessageText(seq++), ((TextMessage) message).getText());
                message.acknowledge();
            }
        } finally {
            messageConsumer.close();
        }
    }

    private String toString(final Message message) throws JMSException {
        String ret = "received message '" + ((TextMessage) message).getText() + "' - " + message.getJMSMessageID();
        if (message.getJMSRedelivered())
             ret += " (redelivered)";
        return ret;

    }

    private static String createMessageText(final int index) {
        return "message #" + index;
    }
}
