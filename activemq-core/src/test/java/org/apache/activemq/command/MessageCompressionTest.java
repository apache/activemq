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

import java.io.UnsupportedEncodingException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class MessageCompressionTest extends TestCase {

    private static final String BROKER_URL = "tcp://localhost:0";
    // The following text should compress well
    private static final String TEXT = "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
            + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. ";

    private BrokerService broker;
    private ActiveMQQueue queue;
    private String connectionUri;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        connectionUri = broker.addConnector(BROKER_URL).getPublishableConnectString();
        broker.start();
        queue = new ActiveMQQueue("TEST." + System.currentTimeMillis());
    }

    protected void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void testTextMessageCompression() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(true);
        sendTestMessage(factory, TEXT);
        ActiveMQTextMessage message = receiveTestMessage(factory);
        int compressedSize = message.getContent().getLength();

        factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(false);
        sendTestMessage(factory, TEXT);
        message = receiveTestMessage(factory);
        int unCompressedSize = message.getContent().getLength();

        assertTrue("expected: compressed Size '" + compressedSize + "' < unCompressedSize '" + unCompressedSize + "'",
                compressedSize < unCompressedSize);
    }

    public void testBytesMessageCompression() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(true);
        sendTestBytesMessage(factory, TEXT);
        ActiveMQBytesMessage message = receiveTestBytesMessage(factory);
        int compressedSize = message.getContent().getLength();
        byte[] bytes = new byte[TEXT.getBytes("UTF8").length];
        message.readBytes(bytes);
        assertTrue(message.readBytes(new byte[255]) == -1);
        String rcvString = new String(bytes, "UTF8");
        assertEquals(TEXT, rcvString);
        assertTrue(message.isCompressed());

        factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(false);
        sendTestBytesMessage(factory, TEXT);
        message = receiveTestBytesMessage(factory);
        int unCompressedSize = message.getContent().getLength();

        assertTrue("expected: compressed Size '" + compressedSize + "' < unCompressedSize '" + unCompressedSize + "'",
                   compressedSize < unCompressedSize);
    }

    private void sendTestMessage(ActiveMQConnectionFactory factory, String message) throws JMSException {
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage(message));
        connection.close();
    }

    private ActiveMQTextMessage receiveTestMessage(ActiveMQConnectionFactory factory) throws JMSException {
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        ActiveMQTextMessage rc = (ActiveMQTextMessage) consumer.receive();
        connection.close();
        return rc;
    }

    private void sendTestBytesMessage(ActiveMQConnectionFactory factory, String message) throws JMSException, UnsupportedEncodingException {
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(message.getBytes("UTF8"));
        producer.send(bytesMessage);
        connection.close();
    }

    private ActiveMQBytesMessage receiveTestBytesMessage(ActiveMQConnectionFactory factory) throws JMSException, UnsupportedEncodingException {
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        ActiveMQBytesMessage rc = (ActiveMQBytesMessage) consumer.receive();
        connection.close();
        return rc;
    }
}
