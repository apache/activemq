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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test covers the Message Compression feature of the ActiveMQConnectionFactory.setUseCompression
 * and has no relation to Http transport level compression.  The Messages are compressed using the
 * deflate algorithm by the ActiveMQ layer before marshalled to XML so only the Message body will
 * be compressed.
 */
public class HttpSendCompressedMessagesTest {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSendCompressedMessagesTest.class);

    private BrokerService broker;
    private static final String tcpBindAddress = "tcp://0.0.0.0:0";
    private static final String httpBindAddress = "http://0.0.0.0:8171";
    private ActiveMQConnectionFactory tcpConnectionFactory;
    private ActiveMQConnectionFactory httpConnectionFactory;
    private ActiveMQConnection tcpConnection;
    private ActiveMQConnection httpConnection;
    private Session tcpSession;
    private Session httpSession;
    private Topic destination;
    private MessageConsumer tcpConsumer;
    private MessageConsumer httpConsumer;

    private static final String destinationName = "HttpCompressionTopic";

    @Test
    public void testTextMessageCompressionFromTcp() throws Exception {
        sendTextMessage(true);
        doTestTextMessageCompression();
    }

    @Test
    public void testTextMessageCompressionFromHttp() throws Exception {
        sendTextMessage(httpConnectionFactory, true);
        doTestTextMessageCompression();
    }

    private void doTestTextMessageCompression() throws Exception {
        ActiveMQTextMessage tcpMessage = (ActiveMQTextMessage) tcpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        ActiveMQTextMessage httpMessage = (ActiveMQTextMessage) httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));

        assertNotNull(tcpMessage);
        assertNotNull(httpMessage);

        ByteSequence tcpContent = tcpMessage.getContent();
        ByteSequence httpContent = httpMessage.getContent();

        assertNotNull(tcpContent);
        assertNotNull(httpContent);

        assertTrue(tcpMessage.isCompressed());
        assertTrue(httpMessage.isCompressed());

        int tcpCompressedSize = tcpContent.getLength();
        int httpCompressedSize = httpContent.getLength();

        assertEquals(tcpContent.getLength(), httpContent.getLength());
        assertEquals(tcpMessage.getText(), httpMessage.getText());

        LOG.info("Received Message on TCP: " + tcpMessage.toString());
        LOG.info("Received Message on HTTP: " + httpMessage.toString());

        sendTextMessage(false);

        ActiveMQTextMessage uncompressedHttpMessage = (ActiveMQTextMessage)
            httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        int httpUncompressedSize = uncompressedHttpMessage.getContent().getLength();

        assertTrue(httpUncompressedSize > httpCompressedSize);
        assertTrue(httpUncompressedSize > tcpCompressedSize);
    }

    @Test
    public void testBytesMessageCompressionFromTcp() throws Exception {
        sendBytesMessage(true);
        doTestBytesMessageCompression();
    }

    @Test
    public void testBytesMessageCompressionFromHttp() throws Exception {
        sendBytesMessage(httpConnectionFactory, true);
        doTestBytesMessageCompression();
    }

    private void doTestBytesMessageCompression() throws Exception {
        ActiveMQBytesMessage tcpMessage = (ActiveMQBytesMessage) tcpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        ActiveMQBytesMessage httpMessage = (ActiveMQBytesMessage) httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));

        assertNotNull(tcpMessage);
        assertNotNull(httpMessage);

        ByteSequence tcpContent = tcpMessage.getContent();
        ByteSequence httpContent = httpMessage.getContent();

        assertNotNull(tcpContent);
        assertNotNull(httpContent);

        assertTrue(tcpMessage.isCompressed());
        assertTrue(httpMessage.isCompressed());

        int tcpCompressedSize = tcpContent.getLength();
        int httpCompressedSize = httpContent.getLength();

        assertEquals(tcpContent.getLength(), httpContent.getLength());
        assertEquals(tcpMessage.readUTF(), httpMessage.readUTF());

        LOG.info("Received Message on TCP: " + tcpMessage.toString());
        LOG.info("Received Message on HTTP: " + httpMessage.toString());

        sendBytesMessage(false);

        ActiveMQBytesMessage uncompressedHttpMessage = (ActiveMQBytesMessage)
            httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        int httpUncompressedSize = uncompressedHttpMessage.getContent().getLength();

        assertTrue(httpUncompressedSize > httpCompressedSize);
        assertTrue(httpUncompressedSize > tcpCompressedSize);
    }

    @Test
    public void testStreamMessageCompressionFromTcp() throws Exception {
        sendStreamMessage(true);
        doTestStreamMessageCompression();
    }

    @Test
    public void testStreamMessageCompressionFromHttp() throws Exception {
        sendStreamMessage(httpConnectionFactory, true);
        doTestStreamMessageCompression();
    }

    private void doTestStreamMessageCompression() throws Exception {
        ActiveMQStreamMessage tcpMessage = (ActiveMQStreamMessage) tcpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        ActiveMQStreamMessage httpMessage = (ActiveMQStreamMessage) httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));

        assertNotNull(tcpMessage);
        assertNotNull(httpMessage);

        ByteSequence tcpContent = tcpMessage.getContent();
        ByteSequence httpContent = httpMessage.getContent();

        assertNotNull(tcpContent);
        assertNotNull(httpContent);

        assertTrue(tcpMessage.isCompressed());
        assertTrue(httpMessage.isCompressed());

        int tcpCompressedSize = tcpContent.getLength();
        int httpCompressedSize = httpContent.getLength();

        assertEquals(tcpContent.getLength(), httpContent.getLength());
        assertEquals(tcpMessage.readString(), httpMessage.readString());

        LOG.info("Received Message on TCP: " + tcpMessage.toString());
        LOG.info("Received Message on HTTP: " + httpMessage.toString());

        sendStreamMessage(false);

        ActiveMQStreamMessage uncompressedHttpMessage = (ActiveMQStreamMessage)
            httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        int httpUncompressedSize = uncompressedHttpMessage.getContent().getLength();

        assertTrue(httpUncompressedSize > httpCompressedSize);
        assertTrue(httpUncompressedSize > tcpCompressedSize);
    }

    @Test
    public void testMapMessageCompressionFromTcp() throws Exception {
        sendMapMessage(true);
        doTestMapMessageCompression();
    }

    @Test
    public void testMapMessageCompressionFromHttp() throws Exception {
        sendMapMessage(httpConnectionFactory, true);
        doTestMapMessageCompression();
    }

    private void doTestMapMessageCompression() throws Exception {
        ActiveMQMapMessage tcpMessage = (ActiveMQMapMessage) tcpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        ActiveMQMapMessage httpMessage = (ActiveMQMapMessage) httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));

        assertNotNull(tcpMessage);
        assertNotNull(httpMessage);

        ByteSequence tcpContent = tcpMessage.getContent();
        ByteSequence httpContent = httpMessage.getContent();

        assertNotNull(tcpContent);
        assertNotNull(httpContent);

        assertTrue(tcpMessage.isCompressed());
        assertTrue(httpMessage.isCompressed());

        int tcpCompressedSize = tcpContent.getLength();
        int httpCompressedSize = httpContent.getLength();

        assertEquals(tcpContent.getLength(), httpContent.getLength());
        assertEquals(tcpMessage.getString("content"), httpMessage.getString("content"));

        LOG.info("Received Message on TCP: " + tcpMessage.toString());
        LOG.info("Received Message on HTTP: " + httpMessage.toString());

        sendMapMessage(false);

        ActiveMQMapMessage uncompressedHttpMessage = (ActiveMQMapMessage)
            httpConsumer.receive(TimeUnit.SECONDS.toMillis(3));
        int httpUncompressedSize = uncompressedHttpMessage.getContent().getLength();

        assertTrue(httpUncompressedSize > httpCompressedSize);
        assertTrue(httpUncompressedSize > tcpCompressedSize);
    }

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);
        TransportConnector tcpConnector = broker.addConnector(tcpBindAddress);
        TransportConnector httpConnector = broker.addConnector(httpBindAddress);
        broker.start();
        broker.waitUntilStarted();

        WaitForJettyListener.waitForJettySocketToAccept(httpConnector.getPublishableConnectString());

        tcpConnectionFactory = new ActiveMQConnectionFactory(tcpConnector.getPublishableConnectString());
        tcpConnectionFactory.setUseCompression(true);
        httpConnectionFactory = new ActiveMQConnectionFactory(httpConnector.getPublishableConnectString());
        httpConnectionFactory.setUseCompression(true);
        tcpConnection = (ActiveMQConnection) tcpConnectionFactory.createConnection();
        httpConnection = (ActiveMQConnection) httpConnectionFactory.createConnection();
        tcpSession = tcpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        httpSession = httpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = tcpSession.createTopic(destinationName);
        tcpConsumer = tcpSession.createConsumer(destination);
        httpConsumer = httpSession.createConsumer(destination);
        tcpConnection.start();
        httpConnection.start();
    }

    @After
    public void shutDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void sendTextMessage(boolean compressed) throws Exception {
        sendTextMessage(tcpConnectionFactory, compressed);
    }

    private void sendTextMessage(ActiveMQConnectionFactory factory, boolean compressed) throws Exception {

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 10; ++i) {
            builder.append(UUID.randomUUID().toString());
        }

        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.setUseCompression(compressed);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage(builder.toString()));
    }

    private void sendBytesMessage(boolean compressed) throws Exception {
        sendBytesMessage(tcpConnectionFactory, compressed);
    }

    private void sendBytesMessage(ActiveMQConnectionFactory factory, boolean compressed) throws Exception {

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 10; ++i) {
            builder.append(UUID.randomUUID().toString());
        }

        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.setUseCompression(compressed);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        BytesMessage message = session.createBytesMessage();
        message.writeUTF(builder.toString());
        producer.send(message);
    }

    private void sendStreamMessage(boolean compressed) throws Exception {
        sendStreamMessage(tcpConnectionFactory, compressed);
    }

    private void sendStreamMessage(ActiveMQConnectionFactory factory, boolean compressed) throws Exception {

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 10; ++i) {
            builder.append(UUID.randomUUID().toString());
        }

        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.setUseCompression(compressed);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        StreamMessage message = session.createStreamMessage();
        message.writeString(builder.toString());
        producer.send(message);
    }

    private void sendMapMessage(boolean compressed) throws Exception {
        sendMapMessage(tcpConnectionFactory, compressed);
    }

    private void sendMapMessage(ActiveMQConnectionFactory factory, boolean compressed) throws Exception {

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 10; ++i) {
            builder.append(UUID.randomUUID().toString());
        }

        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.setUseCompression(compressed);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(destinationName);
        MessageProducer producer = session.createProducer(destination);
        MapMessage message = session.createMapMessage();
        message.setString("content", builder.toString());
        producer.send(message);
    }
}
