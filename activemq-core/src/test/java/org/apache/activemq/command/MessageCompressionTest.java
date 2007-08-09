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
package org.apache.activemq.command;

import java.net.URI;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

public class MessageCompressionTest extends TestCase {

    protected BrokerService broker;
    private ActiveMQQueue queue;
    private static final String BROKER_URL = "tcp://localhost:61216";

    // The following text should compress well
    private static final String TEXT = "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                       + "The quick red fox jumped over the lazy brown dog. ";

    protected void setUp() throws Exception {
        broker = new BrokerService();

        TransportConnector tc = new TransportConnector();
        tc.setUri(new URI(BROKER_URL));
        tc.setName("tcp");

        queue = new ActiveMQQueue("TEST." + System.currentTimeMillis());

        broker.addConnector(tc);
        broker.start();

    }

    protected void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void testTextMessageCompression() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setUseCompression(true);
        sendTestMessage(factory, TEXT);
        ActiveMQTextMessage message = receiveTestMessage(factory);
        int compressedSize = message.getContent().getLength();

        factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setUseCompression(false);
        sendTestMessage(factory, TEXT);
        message = receiveTestMessage(factory);
        int unCompressedSize = message.getContent().getLength();

        assertTrue("expected: compressed Size '" + compressedSize + "' < unCompressedSize '" + unCompressedSize + "'", compressedSize < unCompressedSize);
    }

    private void sendTestMessage(ActiveMQConnectionFactory factory, String message) throws JMSException {
        ActiveMQConnection connection = (ActiveMQConnection)factory.createConnection();
        Session session = connection.createSession(false, 0);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage(message));
        connection.close();
    }

    private ActiveMQTextMessage receiveTestMessage(ActiveMQConnectionFactory factory) throws JMSException {
        ActiveMQConnection connection = (ActiveMQConnection)factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, 0);
        MessageConsumer consumer = session.createConsumer(queue);
        ActiveMQTextMessage rc = (ActiveMQTextMessage)consumer.receive();
        connection.close();
        return rc;
    }

    // public void testJavaUtilZip() throws Exception {
    // String str = "When the going gets weird, the weird turn pro.";
    // byte[] bytes = str.getBytes();
    //
    // ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
    // DeflaterOutputStream dos = new DeflaterOutputStream(baos);
    // dos.
    // }
}
