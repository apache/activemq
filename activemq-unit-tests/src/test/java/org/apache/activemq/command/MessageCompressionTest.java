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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.util.MarshallingSupport.ActiveMQUnmarshalEOFException;
import org.apache.activemq.util.MarshallingSupport.MaxInflatedDataSizeExceededException;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MessageCompressionTest {

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
    private final AtomicBoolean throwMaxInflatedException = new AtomicBoolean(false);
    private final AtomicBoolean sentToDlq = new AtomicBoolean(false);

    @Before
    public void setUp() throws Exception {
        throwMaxInflatedException.set(false);
        sentToDlq.set(false);

        broker = new BrokerService();
        broker.setPlugins(new BrokerPlugin[]{new BrokerPluginSupport() {
            @Override
            public Broker installPlugin(Broker broker) {
                return new BrokerFilter(broker) {
                    @Override
                    public void preProcessDispatch(MessageDispatch messageDispatch) {
                        super.preProcessDispatch(messageDispatch);
                        // simulate a max inflated data size exception during protocol
                        // conversion and decompression
                        if (throwMaxInflatedException.get()) {
                            try {
                                throw new MaxInflatedDataSizeExceededException("Test");
                            } catch (ActiveMQUnmarshalEOFException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public boolean sendToDeadLetterQueue(ConnectionContext context,
                            MessageReference messageReference, Subscription subscription,
                            Throwable poisonCause) {
                        sentToDlq.set(true);
                        return super.sendToDeadLetterQueue(context, messageReference,
                                subscription, poisonCause);
                    }
                };
            }
        }});

        connectionUri = broker.addConnector(BROKER_URL).getPublishableConnectString();
        broker.start();
        queue = new ActiveMQQueue("TEST." + System.currentTimeMillis());
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
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

    @Test
    public void testBytesMessageCompression() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(true);
        sendTestBytesMessage(factory, TEXT);
        ActiveMQBytesMessage message = receiveTestBytesMessage(factory);
        int compressedSize = message.getContent().getLength();
        byte[] bytes = new byte[TEXT.getBytes(StandardCharsets.UTF_8).length];
        message.readBytes(bytes);
        assertEquals(-1, message.readBytes(new byte[255]));
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

    // Test that an error during dispatch goes to the DLQ
    @Test
    public void testMaxInflatedSizeDlq() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setUseCompression(true);
        ActiveMQConnection con1 = (ActiveMQConnection) factory.createConnection();
        con1.start();

        Session session1 = con1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session1.createProducer(queue);
        ActiveMQBytesMessage bytesMessage = (ActiveMQBytesMessage) session1.createBytesMessage();
        bytesMessage.writeBytes(TEXT.getBytes(StandardCharsets.UTF_8));
        producer.send(bytesMessage);

        assertTrue(Wait.waitFor(() -> broker.getDestination(queue)
                .getDestinationStatistics().getMessages().getCount() == 1, 1000, 10));
        assertFalse(sentToDlq.get());

        // simulate a decompression error
        // this should poison ack and DLQ and we shouldn't get the message
        // but the connection should still be open
        this.throwMaxInflatedException.set(true);

        ActiveMQConnection con2 = (ActiveMQConnection) factory.createConnection();
        con2.start();
        Session session2 = con1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session2.createConsumer(queue);
        assertNull(consumer.receive(1000));

        // verify message is gone off the dest and went to the DLQ
        assertTrue(Wait.waitFor(() -> broker.getDestination(queue)
                .getDestinationStatistics().getMessages().getCount() == 0, 500, 10));
        assertTrue(sentToDlq.get());

        // no longer throw an exception
        this.throwMaxInflatedException.set(false);
        sentToDlq.set(false);

        // exception has been disabled so we should receive again on the same connection
        producer.send(bytesMessage);
        assertNotNull(consumer.receive(1000));
        assertFalse(sentToDlq.get());

        con1.close();
        con2.close();
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
