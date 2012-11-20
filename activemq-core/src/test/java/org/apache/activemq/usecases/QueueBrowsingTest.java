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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import java.net.URI;
import java.util.Enumeration;

import static org.junit.Assert.*;

public class QueueBrowsingTest {

    private static final Logger LOG = LoggerFactory.getLogger(QueueBrowsingTest.class);

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;


    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        TransportConnector connector = broker.addConnector("tcp://0.0.0.0:0");
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();
        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
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
        Enumeration enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
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
                    Enumeration enumeration = browser.getEnumeration();
                    int received = 0;
                    while (enumeration.hasMoreElements()) {
                        Message m = (Message) enumeration.nextElement();
                        received++;
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
                        Message m = (Message) consumer.receive(1000);
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
        broker.getSystemUsage().getMemoryUsage().setLimit(10 * 1024);

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
        Enumeration enumeration = browser.getEnumeration();
        int received = 0;
        while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            received++;
            LOG.info("Browsed message " + received + ": " + m.getJMSMessageID());
        }

        browser.close();

        assertEquals(3, received);
    }



}
