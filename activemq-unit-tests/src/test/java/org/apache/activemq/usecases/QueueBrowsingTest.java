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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueBrowsingTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueBrowsingTest.class);

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;
    private final int maxPageSize = 100;

    @Before
    public void startBroker() throws Exception {
        broker = createBroker();
        TransportConnector connector = broker.addConnector("tcp://0.0.0.0:0");
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();

        PolicyEntry policy = new PolicyEntry();
        policy.setMaxPageSize(maxPageSize);
        broker.setDestinationPolicy(new PolicyMap());
        broker.getDestinationPolicy().setDefaultEntry(policy);

        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
    }

    public BrokerService createBroker() throws IOException {
        return new BrokerService();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testBrowsing() throws JMSException {

        int messageToSend = 370;

        ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
            LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
        }

        browser.close();

        assertEquals(messageToSend, received);
    }

    @Test
    public void testBrowseConcurrent() throws Exception {
        final int messageToSend = 370;

        final ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        Thread browserThread = new Thread() {
            @Override
            public void run() {
                try {
                    QueueBrowser browser = session.createBrowser(queue);
                    Enumeration<?> enumeration = browser.getEnumeration();
                    int received = 0;
                    while (enumeration.hasMoreElements()) {
                        Message m = (Message) enumeration.nextElement();
                        received++;
                        LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
                    }
                    assertEquals("Browsed all messages", messageToSend, received);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        browserThread.start();

        Thread consumerThread = new Thread() {
            @Override
            public void run() {
                try {
                    MessageConsumer consumer = session.createConsumer(queue);
                    int received = 0;
                    while (true) {
                        Message m = consumer.receive(1000);
                        if (m == null)
                            break;
                        received++;
                    }
                    assertEquals("Consumed all messages", messageToSend, received);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        consumerThread.start();

        browserThread.join();
        consumerThread.join();
    }

    @Test
    public void testMemoryLimit() throws Exception {
        broker.getSystemUsage().getMemoryUsage().setLimit(16 * 1024);

        int messageToSend = 370;

        ActiveMQQueue queue = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        String data = "";
        for( int i=0; i < 1024*2; i++ ) {
            data += "x";
        }

        for( int i=0; i < messageToSend; i++ ) {
            producer.send(session.createTextMessage(data));
        }

        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
            LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
        }

        browser.close();
        assertTrue("got at least maxPageSize", received >= maxPageSize);
    }
}
