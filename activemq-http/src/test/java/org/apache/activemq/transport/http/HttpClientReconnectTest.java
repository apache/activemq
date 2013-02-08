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
package org.apache.activemq.transport.http;

import static org.junit.Assert.assertEquals;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HttpClientReconnectTest {

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;

    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        broker = new BrokerService();
        TransportConnector connector = broker.addConnector("https://localhost:0?trace=true");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.deleteAllMessages();
        broker.start();

        String connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri + "?trace=true&soTimeout=1000");
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test(timeout=120000)
    public void testReconnectClient() throws Exception {
        for (int i = 0; i < 100; i++) {
            sendAndReceiveMessage(i);
        }
    }

    private void sendAndReceiveMessage(int i) throws Exception {
        Connection conn = factory.createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Destination dest = new ActiveMQQueue("test");
        MessageProducer producer = sess.createProducer(dest);
        MessageConsumer consumer = sess.createConsumer(dest);
        String messageText = "test " + i;
        try {
            producer.send(sess.createTextMessage(messageText));
            TextMessage msg = (TextMessage)consumer.receive(1000);
            assertEquals(messageText, msg.getText());
        } finally {
            producer.close();
            consumer.close();
            conn.close();
            sess.close();
        }
    }
}
