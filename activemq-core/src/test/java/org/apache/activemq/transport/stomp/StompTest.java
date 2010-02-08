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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

public class StompTest extends CombinationTestSupport {
    private static final Log LOG = LogFactory.getLog(StompTest.class);

    protected String bindAddress = "stomp://localhost:61613";
    protected String confUri = "xbean:org/apache/activemq/transport/stomp/stomp-auth-broker.xml";
    protected String jmsUri = "vm://localhost";


    private BrokerService broker;
    private StompConnection stompConnection = new StompConnection();
    private Connection connection;
    private Session session;
    private ActiveMQQueue queue;
    private String xmlObject = "<pojo>\n"
            + "  <name>Dejan</name>\n"
            + "  <city>Belgrade</city>\n"
            + "</pojo>";

    private String xmlMap = "<map>\n"
        + "  <entry>\n"
        + "    <string>name</string>\n"
        + "    <string>Dejan</string>\n"
        + "  </entry>\n"
        + "  <entry>\n"
        + "    <string>city</string>\n"
        + "    <string>Belgrade</string>\n"
        + "  </entry>\n"
        + "</map>\n";

    private String jsonObject = "{\"pojo\":{"
        + "\"name\":\"Dejan\","
        + "\"city\":\"Belgrade\""
        + "}}";

    private String jsonMap = "{\"map\":{"
        + "\"entry\":["
        + "{\"string\":[\"name\",\"Dejan\"]},"
        + "{\"string\":[\"city\",\"Belgrade\"]}"
        + "]"
        + "}}";

    protected void setUp() throws Exception {
        // The order of the entries is different when using ibm jdk 5.
        if (System.getProperty("java.vendor").equals("IBM Corporation")
            && System.getProperty("java.version").startsWith("1.5")) {
            xmlMap = "<map>\n"
                + "  <entry>\n"
                + "    <string>city</string>\n"
                + "    <string>Belgrade</string>\n"
                + "  </entry>\n"
                + "  <entry>\n"
                + "    <string>name</string>\n"
                + "    <string>Dejan</string>\n"
                + "  </entry>\n"
                + "</map>\n";
            jsonMap = "{\"map\":{"
                + "\"entry\":["
                + "{\"string\":[\"city\",\"Belgrade\"]},"
                + "{\"string\":[\"name\",\"Dejan\"]}"
                + "]"
                + "}}";
        }
        broker = BrokerFactory.createBroker(new URI(confUri));
        broker.start();

        stompConnect();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(jmsUri);
        connection = cf.createConnection("system", "manager");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = new ActiveMQQueue(getQueueName());
        connection.start();
    }

    private void stompConnect() throws IOException, URISyntaxException, UnknownHostException {
        URI connectUri = new URI(bindAddress);
        stompConnection.open(createSocket(connectUri));
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        return new Socket("127.0.0.1", connectUri.getPort());
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

    protected void tearDown() throws Exception {
        try {
            connection.close();
            stompDisconnect();
        } catch(Exception e) {
            // Some tests explicitly disconnect from stomp so can ignore
        } finally {
            broker.stop();
        }
    }

    private void stompDisconnect() throws IOException {
        if (stompConnection != null) {
            stompConnection.close();
            stompConnection = null;
        }
    }

    public void sendMessage(String msg) throws Exception {
        sendMessage(msg, "foo", "xyz");
    }

    public void sendMessage(String msg, String propertyName, String propertyValue) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage(msg);
        message.setStringProperty(propertyName, propertyValue);
        producer.send(message);
    }

    public void sendBytesMessage(byte[] msg) throws Exception {
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(msg);
        producer.send(message);
    }

    public void testConnect() throws Exception {

        String connectFrame = "CONNECT\n" + "login: system\n" + "passcode: manager\n" + "request-id: 1\n" + "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("response-id:1") >= 0);

    }

    public void testSendMessage() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());

        // Make sure that the timestamp is valid - should
        // be very close to the current time.
        long tnow = System.currentTimeMillis();
        long tmsg = message.getJMSTimestamp();
        assertTrue(Math.abs(tnow - tmsg) < 1000);
    }

    public void testJMSXGroupIdCanBeSet() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "JMSXGroupID: TEST\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("TEST", ((ActiveMQTextMessage)message).getGroupID());
    }

    public void testSendMessageWithCustomHeadersAndSelector() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "foo:abc\n" + "bar:123\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("foo", "abc", message.getStringProperty("foo"));
        assertEquals("bar", "123", message.getStringProperty("bar"));
    }

    public void testSendMessageWithStandardHeaders() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "correlation-id:c123\n" + "priority:3\n" + "type:t345\n" + "JMSXGroupID:abc\n" + "foo:abc\n" + "bar:123\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World"
                + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
        assertEquals("getJMSType", "t345", message.getJMSType());
        assertEquals("getJMSPriority", 3, message.getJMSPriority());
        assertEquals("foo", "abc", message.getStringProperty("foo"));
        assertEquals("bar", "123", message.getStringProperty("bar"));

        assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
        ActiveMQTextMessage amqMessage = (ActiveMQTextMessage)message;
        assertEquals("GroupID", "abc", amqMessage.getGroupID());
    }

    public void testSubscribeWithAutoAck() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage(getName());

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithAutoAckAndBytesMessage() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendBytesMessage(new byte[] {
            1, 2, 3, 4, 5
        });

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        Pattern cl = Pattern.compile("Content-length:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher clMmatcher = cl.matcher(frame);
        assertTrue(clMmatcher.find());
        assertEquals("5", clMmatcher.group(1));

        assertFalse(Pattern.compile("type:\\s*null", Pattern.CASE_INSENSITIVE).matcher(frame).find());

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithMessageSentWithProperties() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("Hello World");
        message.setStringProperty("s", "value");
        message.setBooleanProperty("n", false);
        message.setByteProperty("byte", (byte)9);
        message.setDoubleProperty("d", 2.0);
        message.setFloatProperty("f", (float)6.0);
        message.setIntProperty("i", 10);
        message.setLongProperty("l", 121);
        message.setShortProperty("s", (short)12);
        producer.send(message);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));


        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testMessagesAreInOrder() throws Exception {
        int ctr = 10;
        String[] data = new String[ctr];

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        for (int i = 0; i < ctr; ++i) {
            data[i] = getName() + i;
            sendMessage(data[i]);
        }

        for (int i = 0; i < ctr; ++i) {
            frame = stompConnection.receiveFrame();
            assertTrue("Message not in order", frame.indexOf(data[i]) >= 0);
        }

        // sleep a while before publishing another set of messages
        waitForFrameToTakeEffect();

        for (int i = 0; i < ctr; ++i) {
            data[i] = getName() + ":second:" + i;
            sendMessage(data[i]);
        }

        for (int i = 0; i < ctr; ++i) {
            frame = stompConnection.receiveFrame();
            assertTrue("Message not in order", frame.indexOf(data[i]) >= 0);
        }

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithAutoAckAndSelector() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "selector: foo = 'zzz'\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage("Ignored message", "foo", "1234");
        sendMessage("Real message", "foo", "zzz");

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real message") > 0);

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithClientAck() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        sendMessage(getName());
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        stompDisconnect();

        // message should be received since message was not acknowledged
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertTrue(message.getJMSRedelivered());
    }

    public void testUnsubscribe() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // send a message to our queue
        sendMessage("first message");

        // receive message from socket
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        // remove suscription
        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        waitForFrameToTakeEffect();

        // send a message to our queue
        sendMessage("second message");

        try {
            frame = stompConnection.receiveFrame();
            LOG.info("Received frame: " + frame);
            fail("No message should have been received since subscription was removed");
        } catch (SocketTimeoutException e) {
        }
    }

    public void testTransactionCommit() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));

        frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transaction: tx1\n" + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "COMMIT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        waitForFrameToTakeEffect();

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull("Should have received a message", message);
    }

    public void testTransactionRollback() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));

        frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transaction: tx1\n" + "\n" + "first message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // rollback first message
        frame = "ABORT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transaction: tx1\n" + "\n" + "second message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "COMMIT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // This test case is currently failing
        waitForFrameToTakeEffect();

        // only second msg should be received since first msg was rolled back
        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("second message", message.getText().trim());
    }

    public void testDisconnectedClientsAreRemovedFromTheBroker() throws Exception {
        assertClients(1);
        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        // This test case is currently failing
        waitForFrameToTakeEffect();

        assertClients(2);

        // now lets kill the stomp connection
        stompConnection.close();

        Thread.sleep(2000);

        assertClients(1);
    }

    public void testConnectNotAuthenticatedWrongUser() throws Exception {
        String frame = "CONNECT\n" + "login: dejanb\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();

        assertTrue(f.startsWith("ERROR"));
        assertClients(1);
    }

    public void testConnectNotAuthenticatedWrongPassword() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: dejanb\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();

        assertTrue(f.startsWith("ERROR"));
        assertClients(1);
    }

    public void testSendNotAuthorized() throws Exception {

        String frame = "CONNECT\n" + "login: guest\n" + "passcode: password\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/USERS." + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("ERROR"));
    }

    public void testSubscribeNotAuthorized() throws Exception {

        String frame = "CONNECT\n" + "login: guest\n" + "passcode: password\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("ERROR"));

    }

    public void testTransformationUnknownTranslator() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:test" + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
    }

    public void testTransformationFailed() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertNotNull(message.getStringProperty(Stomp.Headers.TRANSFORMATION_ERROR));
        assertEquals("Hello World", message.getText());
    }

    public void testTransformationSendXMLObject() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + xmlObject + Stomp.NULL;

        stompConnection.sendFrame(frame);

        ObjectMessage message = (ObjectMessage)consumer.receive(2500);
        assertNotNull(message);
        SamplePojo object = (SamplePojo)message.getObject();
        assertEquals("Dejan", object.getName());
    }

    public void testTransformationSendJSONObject() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_OBJECT_JSON + "\n\n" + jsonObject + Stomp.NULL;

        stompConnection.sendFrame(frame);

        ObjectMessage message = (ObjectMessage)consumer.receive(2500);
        assertNotNull(message);
        SamplePojo object = (SamplePojo)message.getObject();
        assertEquals("Dejan", object.getName());
    }

    public void testTransformationSubscribeXML() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationReceiveJSONObject() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(jsonObject));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationReceiveXMLObject() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationNotOverrideSubscription() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        message.setStringProperty("transformation",	Stomp.Transformations.JMS_OBJECT_XML.toString());
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(jsonObject));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationIgnoreTransformation() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        message.setStringProperty("transformation", Stomp.Transformations.JMS_OBJECT_XML.toString());
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.endsWith("\n\n"));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationSendXMLMap() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_XML + "\n\n" + xmlMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        MapMessage message = (MapMessage) consumer.receive(2500);
        assertNotNull(message);
        assertEquals(message.getString("name"), "Dejan");
    }

    public void testTransformationSendJSONMap() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_JSON + "\n\n" + jsonMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        MapMessage message = (MapMessage) consumer.receive(2500);
        assertNotNull(message);
        assertEquals(message.getString("name"), "Dejan");
    }

    public void testTransformationReceiveXMLMap() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        MapMessage message = session.createMapMessage();
        message.setString("name", "Dejan");
        message.setString("city", "Belgrade");
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n" + "transformation:" + Stomp.Transformations.JMS_MAP_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlMap.trim()));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testTransformationReceiveJSONMap() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        MapMessage message = session.createMapMessage();
        message.setString("name", "Dejan");
        message.setString("city", "Belgrade");
        producer.send(message);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n" + "transformation:" + Stomp.Transformations.JMS_MAP_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(jsonMap.trim()));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testDurableUnsub() throws Exception {
        // get broker JMX view

        String domain = "org.apache.activemq";
        ObjectName brokerName = new ObjectName(domain + ":Type=Broker,BrokerName=localhost");

        BrokerViewMBean view = (BrokerViewMBean)broker.getManagementContext().newProxyInstance(brokerName, BrokerViewMBean.class, true);

        // connect
        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));
        assertEquals(view.getDurableTopicSubscribers().length, 0);

        // subscribe
        frame = "SUBSCRIBE\n" + "destination:/topic/" + getQueueName() + "\n" + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        // wait a bit for MBean to get refreshed
        try {
            Thread.sleep(400);
        } catch (InterruptedException e){}

        assertEquals(view.getDurableTopicSubscribers().length, 1);
        // disconnect
        frame = "DISCONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e){}

        //reconnect
        stompConnect();
        // connect
        frame = "CONNECT\n" + "login: system\n" + "passcode: manager\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        // unsubscribe
        frame = "UNSUBSCRIBE\n" + "destination:/topic/" + getQueueName() + "\n" + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e){}
        assertEquals(view.getDurableTopicSubscribers().length, 0);
    }

    public void testMessageIdHeader() throws Exception {
        stompConnection.connect("system", "manager");

        stompConnection.begin("tx1");
        stompConnection.send("/queue/" + getQueueName(), "msg", "tx1", null);
        stompConnection.commit("tx1");

        stompConnection.subscribe("/queue/" + getQueueName());
        StompFrame stompMessage = stompConnection.receive();
        assertNull(stompMessage.getHeaders().get("transaction"));
    }

    public void testPrefetchSize() throws Exception {
        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("activemq.prefetchSize", "1");
        stompConnection.subscribe("/queue/" + getQueueName(), "client", headers);

        // send messages using JMS
        sendMessage("message 1");
        sendMessage("message 2");
        sendMessage("message 3");
        sendMessage("message 4");
        sendMessage("message 5");

        StompFrame frame = stompConnection.receive();
        assertEquals(frame.getBody(), "message 1");

        stompConnection.begin("tx1");
        stompConnection.ack(frame, "tx1");

        StompFrame frame1 = stompConnection.receive();
        assertEquals(frame1.getBody(), "message 2");

        try {
            StompFrame frame2 = stompConnection.receive(500);
            if (frame2 != null) {
                fail("Should not have received the second message");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame1, "tx1");
        Thread.sleep(1000);
        stompConnection.abort("tx1");

        stompConnection.begin("tx2");

        // Previously delivered message need to get re-acked...
        stompConnection.ack(frame, "tx2");
        stompConnection.ack(frame1, "tx2");

        StompFrame frame3 = stompConnection.receive();
        assertEquals(frame3.getBody(), "message 3");
        stompConnection.ack(frame3, "tx2");

        StompFrame frame4 = stompConnection.receive();
        assertEquals(frame4.getBody(), "message 4");
        stompConnection.ack(frame4, "tx2");

        stompConnection.commit("tx2");

        stompConnection.begin("tx3");
        StompFrame frame5 = stompConnection.receive();
        assertEquals(frame5.getBody(), "message 5");
        stompConnection.ack(frame5, "tx3");
        stompConnection.commit("tx3");

        stompDisconnect();
    }

    public void testTransactionsWithMultipleDestinations() throws Exception {

        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("activemq.prefetchSize", "1");
        headers.put("activemq.exclusive", "true");

        stompConnection.subscribe("/queue/test1", "client", headers);

        stompConnection.begin("ID:tx1");

        headers.clear();
        headers.put("receipt", "ID:msg1");
        stompConnection.send("/queue/test2", "test message", "ID:tx1", headers);

        stompConnection.commit("ID:tx1");

        // make sure connection is active after commit
        Thread.sleep(1000);
        stompConnection.send("/queue/test1", "another message");

        StompFrame frame = stompConnection.receive(500);
        assertNotNull(frame);

        stompConnection.disconnect();
    }

    public void testTempDestination() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/temp-queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/temp-queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame message = stompConnection.receive(1000);
        assertEquals("Hello World", message.getBody());
    }

    public void testJMSXUserIDIsSetInMessage() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(5000);
        assertNotNull(message);
        assertEquals("system", message.getStringProperty(Stomp.Headers.Message.USERID));

    }

    public void testJMSXUserIDIsSetInStompMessage() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame message = stompConnection.receive(5000);
        assertEquals("system", message.getHeaders().get(Stomp.Headers.Message.USERID));
    }

    protected void assertClients(int expected) throws Exception {
        org.apache.activemq.broker.Connection[] clients = broker.getBroker().getClients();
        int actual = clients.length;

        assertEquals("Number of clients", expected, actual);
    }

    protected void waitForFrameToTakeEffect() throws InterruptedException {
        // bit of a dirty hack :)
        // another option would be to force some kind of receipt to be returned
        // from the frame
        Thread.sleep(2000);
    }
}
