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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.StringReader;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.AbstractSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.XppFactory;

public class StompTest extends StompTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(StompTest.class);

    protected Connection connection;
    protected Session session;
    protected ActiveMQQueue queue;
    protected XStream xstream;

    private final String xmlObject = "<pojo>\n"
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

    private final String jsonObject = "{\"pojo\":{"
        + "\"name\":\"Dejan\","
        + "\"city\":\"Belgrade\""
        + "}}";

    private String jsonMap = "{\"map\":{"
        + "\"entry\":["
        + "{\"string\":[\"name\",\"Dejan\"]},"
        + "{\"string\":[\"city\",\"Belgrade\"]}"
        + "]"
        + "}}";

    @Override
    public void setUp() throws Exception {
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

        queue = new ActiveMQQueue(getQueueName());
        super.setUp();

        stompConnect();

        connection = cf.createConnection("system", "manager");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
        xstream = new XStream();
        xstream.processAnnotations(SamplePojo.class);
        xstream.allowTypes(new Class[] { SamplePojo.class });
    }

    @Override
    public void applyBrokerPolicies() {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry persistRedelivery = new PolicyEntry();
        persistRedelivery.setPersistJMSRedelivered(true);
        policyMap.put(queue, persistRedelivery);
        brokerService.setDestinationPolicy(policyMap);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            connection.close();
        } catch(Exception e) {
            // Some tests explicitly disconnect from stomp so can ignore
        } finally {
            super.tearDown();
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

    @Test(timeout = 60000)
    public void testConnect() throws Exception {

        String connectFrame = "CONNECT\n" + "login:system\n" + "passcode:manager\n" + "request-id:1\n" + "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("response-id:1") >= 0);
    }

    @Test(timeout = 60000)
    public void testSendMessage() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testJMSXGroupIdCanBeSet() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "JMSXGroupID:TEST\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("TEST", ((ActiveMQTextMessage)message).getGroupID());
    }

    @Test(timeout = 60000)
    public void testSendMessageWithCustomHeadersAndSelector() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testSendMessageWithDelay() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "AMQ_SCHEDULED_DELAY:2000\n"  + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(1000);
        assertNull(message);
        message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
    }

    @Test(timeout = 60000)
    public void testSendMessageWithStandardHeaders() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testSendMessageWithNoPriorityReceivesDefault() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "correlation-id:c123\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World"
                + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("getJMSPriority", 4, message.getJMSPriority());
    }

    @Test(timeout = 60000)
    public void testSendFrameWithInvalidAction() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        final int connectionCount = getProxyToBroker().getCurrentConnectionsCount();

        frame = "SED\n" + "AMQ_SCHEDULED_DELAY:2000\n"  + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));

        assertTrue("Should drop connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connectionCount > getProxyToBroker().getCurrentConnectionsCount();
            }
        }));
    }

    @Test(timeout = 60000)
    public void testReceipts() throws Exception {

        StompConnection receiver = new StompConnection();
        receiver.open(createSocket());
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        receiver.sendFrame(frame);

        frame = receiver.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        receiver.sendFrame(frame);

        frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "receipt: msg-1\n" + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = receiver.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Stomp Message does not contain receipt request", frame.indexOf(Stomp.Headers.RECEIPT_REQUESTED) == -1);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("RECEIPT"));
        assertTrue("Receipt contains correct receipt-id", frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);

        frame = "DISCONNECT\n" + "receipt: dis-1\n" + "\n\n" + Stomp.NULL;
        receiver.sendFrame(frame);
        frame = receiver.receiveFrame();
        assertTrue(frame.startsWith("RECEIPT"));
        assertTrue("Receipt contains correct receipt-id", frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);
        receiver.close();

        MessageConsumer consumer = session.createConsumer(queue);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "receipt: msg-1\n" + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("RECEIPT"));
        assertTrue("Receipt contains correct receipt-id", frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);

        TextMessage message = (TextMessage)consumer.receive(10000);
        assertNotNull(message);
        assertNull("JMS Message does not contain receipt request", message.getStringProperty(Stomp.Headers.RECEIPT_REQUESTED));
    }

    @Test(timeout = 60000)
    public void testSubscriptionReceipts() throws Exception {
        final int done = 20;
        int count = 0;
        int receiptId = 0;

        do {
            StompConnection sender = new StompConnection();
            sender.open(createSocket());
            String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
            sender.sendFrame(frame);

            frame = sender.receiveFrame();
            assertTrue(frame.startsWith("CONNECTED"));

            frame = "SEND\n" + "destination:/queue/" + getQueueName()  + "\n"  + "receipt: " + (receiptId++) + "\n" + "Hello World:" + (count++) + "\n\n" +  Stomp.NULL;
            sender.sendFrame(frame);
            frame = sender.receiveFrame();
            assertTrue("" + frame, frame.startsWith("RECEIPT"));

            sender.disconnect();

            StompConnection receiver = new StompConnection();
            receiver.open(createSocket());

            frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
            receiver.sendFrame(frame);

            frame = receiver.receiveFrame();
            assertTrue(frame.startsWith("CONNECTED"));

            frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n"  + "receipt: " +  (receiptId++)  + "\n\n" + Stomp.NULL;
            receiver.sendFrame(frame);

            frame = receiver.receiveFrame();
            assertTrue("" + frame, frame.startsWith("RECEIPT"));
            assertTrue("Receipt contains receipt-id", frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);
            frame = receiver.receiveFrame();
            assertTrue("" + frame, frame.startsWith("MESSAGE"));

            // remove suscription  so we don't hang about and get next message
            frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n"  + "receipt: " +  (receiptId++)  + "\n\n" + Stomp.NULL;
            receiver.sendFrame(frame);
            frame = receiver.receiveFrame();
            assertTrue("" + frame, frame.startsWith("RECEIPT"));

            receiver.disconnect();
        } while (count < done);
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAck() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage(name.getMethodName());

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAckAndBytesMessage() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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
    }

    @Test(timeout = 60000)
    public void testBytesMessageWithNulls() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\ncontent-length:5" + " \n\n" + "\u0001\u0002\u0000\u0004\u0005" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame message = stompConnection.receive();
        assertTrue(message.getAction().startsWith("MESSAGE"));

        String length = message.getHeaders().get("content-length");
        assertEquals("5", length);

        assertEquals(5, message.getContent().length);
    }

    @Test(timeout = 60000)
    public void testSendMultipleBytesMessages() throws Exception {

        final int MSG_COUNT = 50;

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        for( int ix = 0; ix < MSG_COUNT; ix++) {
            frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\ncontent-length:5" + " \n\n" + "\u0001\u0002\u0000\u0004\u0005" + Stomp.NULL;
            stompConnection.sendFrame(frame);
        }

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        for( int ix = 0; ix < MSG_COUNT; ix++) {
            StompFrame message = stompConnection.receive();
            assertTrue(message.getAction().startsWith("MESSAGE"));

            String length = message.getHeaders().get("content-length");
            assertEquals("5", length);

            assertEquals(5, message.getContent().length);
        }
    }

    @Test(timeout = 60000)
    public void testSubscribeWithMessageSentWithProperties() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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
    }

    @Test(timeout = 60000)
    public void testMessagesAreInOrder() throws Exception {
        int ctr = 10;
        String[] data = new String[ctr];

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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
        TimeUnit.MILLISECONDS.sleep(500);

        for (int i = 0; i < ctr; ++i) {
            data[i] = getName() + ":second:" + i;
            sendMessage(data[i]);
        }

        for (int i = 0; i < ctr; ++i) {
            frame = stompConnection.receiveFrame();
            assertTrue("Message not in order", frame.indexOf(data[i]) >= 0);
        }
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAckAndSelector() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAckAndNumericSelector() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "selector: foo = 42\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Ignored
        frame = "SEND\n" + "foo:abc\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Ignored Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Matches
        frame = "SEND\n" + "foo:42\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Real Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real Message") > 0);
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAckAndBooleanSelector() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "selector: foo = true\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Ignored
        frame = "SEND\n" + "foo:false\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Ignored Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Matches
        frame = "SEND\n" + "foo:true\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Real Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real Message") > 0);
    }

    @Test(timeout = 60000)
    public void testSubscribeWithAutoAckAnFloatSelector() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "selector: foo = 3.14159\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Ignored
        frame = "SEND\n" + "foo:6.578\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Ignored Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Matches
        frame = "SEND\n" + "foo:3.14159\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Real Message" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real Message") > 0);
    }

    @Test(timeout = 60000)
    public void testSubscribeWithClientAck() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testSubscribeWithClientAckedAndContentLength() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        sendMessage(getName());
        StompFrame msg = stompConnection.receive();

        assertTrue(msg.getAction().equals("MESSAGE"));

        HashMap<String, String> ackHeaders = new HashMap<String, String>();
        ackHeaders.put("message-id", msg.getHeaders().get("message-id"));
        ackHeaders.put("content-length", "8511");

        StompFrame ack = new StompFrame("ACK", ackHeaders);
        stompConnection.sendFrame(ack.format());

        final QueueViewMBean queueView = getProxyToQueue(getQueueName());
        assertTrue("dequeue complete", Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("queueView, enqueue:" + queueView.getEnqueueCount() +", dequeue:" + queueView.getDequeueCount() + ", inflight:" + queueView.getInFlightCount());
                return queueView.getDequeueCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(25)));

        stompDisconnect();

        // message should not be received since it was acknowledged
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage)consumer.receive(500);
        assertNull(message);
    }

    @Test(timeout = 60000)
    public void testUnsubscribe() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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
        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "receipt:1" +  "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue("" + frame, frame.startsWith("RECEIPT"));

        // send a message to our queue
        sendMessage("second message");

        try {
            frame = stompConnection.receiveFrame(500);
            LOG.info("Received frame: " + frame);
            fail("No message should have been received since subscription was removed");
        } catch (SocketTimeoutException e) {
        }
    }

    @Test(timeout = 60000)
    public void testTransactionCommit() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));

        frame = "BEGIN\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transaction: tx1\n" + "\n\n" + "Hello World" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "COMMIT\n" + "transaction: tx1\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(10000);
        assertNotNull("Should have received a message", message);
    }

    @Test(timeout = 60000)
    public void testTransactionRollback() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

        // only second msg should be received since first msg was rolled back
        TextMessage message = (TextMessage)consumer.receive(10000);
        assertNotNull(message);
        assertEquals("second message", message.getText().trim());
    }

    @Test(timeout = 60000)
    public void testDisconnectedClientsAreRemovedFromTheBroker() throws Exception {
        assertClients(1);
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        assertClients(2);

        // now lets kill the stomp connection
        stompConnection.close();

        assertClients(1);
    }

    @Test(timeout = 60000)
    public void testConnectNotAuthenticatedWrongUser() throws Exception {
        String frame = "CONNECT\n" + "login: dejanb\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        try {
            String f = stompConnection.receiveFrame();
            assertTrue(f.startsWith("ERROR"));
            assertFalse("no stack trace impl leak:" + f, f.contains("at "));
        } catch (IOException socketMayBeClosedFirstByBroker) {}
    }

    @Test(timeout = 60000)
    public void testConnectNotAuthenticatedWrongPassword() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode: dejanb\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        try {
            String f = stompConnection.receiveFrame();
            assertTrue(f.startsWith("ERROR"));
            assertFalse("no stack trace impl leak:" + f, f.contains("at "));
        } catch (IOException socketMayBeClosedFirstByBroker) {}
    }

    @Test(timeout = 60000)
    public void testSendNotAuthorized() throws Exception {

        String frame = "CONNECT\n" + "login:guest\n" + "passcode:password\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/USERS." + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("ERROR"));
        assertFalse("no stack trace impl leak:" + f, f.contains("at "));
    }

    @Test(timeout = 60000)
    public void testSubscribeNotAuthorized() throws Exception {

        String frame = "CONNECT\n" + "login:guest\n" + "passcode:password\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));
        assertFalse("no stack trace impl leak:" + frame, frame.contains("at "));
    }

    @Test(timeout = 60000)
    public void testSubscribeWithReceiptNotAuthorized() throws Exception {

        String frame = "CONNECT\n" + "login:guest\n" + "passcode:password\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" +
                "ack:auto\n" + "receipt:1\n" + "\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));
        assertTrue("Error Frame did not contain receipt-id", frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);
        assertFalse("no stack trace impl leak:" + frame, frame.contains("at "));
    }

    @Test(timeout = 60000)
    public void testSubscribeWithInvalidSelector() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "selector:foo.bar = 1\n" + "ack:auto\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));
    }

    @Test(timeout = 60000)
    public void testTransformationUnknownTranslator() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:test" + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
    }

    @Test(timeout = 60000)
    public void testTransformationFailed() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testTransformationSendXMLObject() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + xmlObject + Stomp.NULL;

        stompConnection.sendFrame(frame);

        Message message = consumer.receive(2500);
        assertNotNull(message);

        LOG.info("Broke sent: {}", message);

        assertTrue(message instanceof ObjectMessage);
        ObjectMessage objectMessage = (ObjectMessage)message;
        SamplePojo object = (SamplePojo)objectMessage.getObject();
        assertEquals("Dejan", object.getName());
    }

    @Test(timeout = 60000)
    public void testTransformationSendJSONObject() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testTransformationSubscribeXML() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveJSONObject() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(jsonObject));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveXMLObject() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveObject() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveXMLObjectAndMap() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage objMessage = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(objMessage);

        MapMessage mapMessage = session.createMapMessage();
        mapMessage.setString("name", "Dejan");
        mapMessage.setString("city", "Belgrade");
        producer.send(mapMessage);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(xmlObject));

        StompFrame xmlFrame = stompConnection.receive();

        Map<String, String> map = createMapFromXml(xmlFrame.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertTrue(map.get("name").equals("Dejan"));
        assertTrue(map.get("city").equals("Belgrade"));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveJSONObjectAndMap() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage objMessage = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        producer.send(objMessage);

        MapMessage mapMessage = session.createMapMessage();
        mapMessage.setString("name", "Dejan");
        mapMessage.setString("city", "Belgrade");
        producer.send(mapMessage);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame json = stompConnection.receive();
        LOG.info("Transformed frame: {}", json);

        SamplePojo pojo = createObjectFromJson(json.getBody());
        assertTrue(pojo.getCity().equals("Belgrade"));
        assertTrue(pojo.getName().equals("Dejan"));

        json = stompConnection.receive();
        LOG.info("Transformed frame: {}", json);

        Map<String, String> map = createMapFromJson(json.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertTrue(map.get("name").equals("Dejan"));
        assertTrue(map.get("city").equals("Belgrade"));
    }

    @Test(timeout = 60000)
    public void testTransformationSendAndReceiveXmlMap() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:" + Stomp.Transformations.JMS_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_JSON + "\n\n" + jsonMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame xmlFrame = stompConnection.receive();
        LOG.info("Received Frame: {}", xmlFrame.getBody());

        Map<String, String> map = createMapFromXml(xmlFrame.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertEquals("Dejan", map.get("name"));
        assertEquals("Belgrade", map.get("city"));

        assertTrue(xmlFrame.getHeaders().containsValue("jms-map-xml"));
    }

    @Test(timeout = 60000)
    public void testTransformationSendAndReceiveJsonMap() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:" + Stomp.Transformations.JMS_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_XML + "\n\n" + xmlMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame json = stompConnection.receive();
        LOG.info("Received Frame: {}", json.getBody());

        assertNotNull(json);
        assertTrue(json.getHeaders().containsValue("jms-map-json"));

        Map<String, String> map = createMapFromJson(json.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertEquals("Dejan", map.get("name"));
        assertEquals("Belgrade", map.get("city"));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveBytesMessage() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(new byte[]{1, 2, 3, 4, 5});
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        Pattern cl = Pattern.compile("Content-length:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher clMmatcher = cl.matcher(frame);
        assertTrue(clMmatcher.find());
        assertEquals("5", clMmatcher.group(1));

        assertFalse(Pattern.compile("type:\\s*null", Pattern.CASE_INSENSITIVE).matcher(frame).find());
    }

    @Test(timeout = 60000)
    public void testTransformationNotOverrideSubscription() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        message.setStringProperty("transformation",	Stomp.Transformations.JMS_OBJECT_XML.toString());
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n" + "transformation:"	+ Stomp.Transformations.JMS_OBJECT_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.trim().endsWith(jsonObject));
    }

    @Test(timeout = 60000)
    public void testTransformationIgnoreTransformation() throws Exception {
        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        ObjectMessage message = session.createObjectMessage(new SamplePojo("Dejan", "Belgrade"));
        message.setStringProperty("transformation", Stomp.Transformations.JMS_OBJECT_XML.toString());
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();

        assertTrue(frame.endsWith("\n\n"));
    }

    @Test(timeout = 60000)
    public void testTransformationSendXMLMap() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_XML + "\n\n" + xmlMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        MapMessage message = (MapMessage) consumer.receive(2500);
        assertNotNull(message);
        assertEquals(message.getString("name"), "Dejan");
    }

    @Test(timeout = 60000)
    public void testTransformationSendJSONMap() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "transformation:" + Stomp.Transformations.JMS_MAP_JSON + "\n\n" + jsonMap + Stomp.NULL;

        stompConnection.sendFrame(frame);

        MapMessage message = (MapMessage) consumer.receive(2500);
        assertNotNull(message);
        assertEquals(message.getString("name"), "Dejan");
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveXMLMap() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        MapMessage message = session.createMapMessage();
        message.setString("name", "Dejan");
        message.setString("city", "Belgrade");
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n" + "transformation:" + Stomp.Transformations.JMS_MAP_XML + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame xmlFrame = stompConnection.receive();
        LOG.info("Received Frame: {}", xmlFrame.getBody());

        Map<String, String> map = createMapFromXml(xmlFrame.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertEquals("Dejan", map.get("name"));
        assertEquals("Belgrade", map.get("city"));
    }

    @Test(timeout = 60000)
    public void testTransformationReceiveJSONMap() throws Exception {

        MessageProducer producer = session.createProducer(new ActiveMQQueue("USERS." + getQueueName()));
        MapMessage message = session.createMapMessage();
        message.setString("name", "Dejan");
        message.setString("city", "Belgrade");
        producer.send(message);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/USERS." + getQueueName() + "\n" + "ack:auto\n" + "transformation:" + Stomp.Transformations.JMS_MAP_JSON + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame json = stompConnection.receive();
        LOG.info("Received Frame: {}", json.getBody());
        assertNotNull(json);
        Map<String, String> map = createMapFromJson(json.getBody());

        assertTrue(map.containsKey("name"));
        assertTrue(map.containsKey("city"));

        assertEquals("Dejan", map.get("name"));
        assertEquals("Belgrade", map.get("city"));
    }

    @Test(timeout = 60000)
    public void testDurableUnsub() throws Exception {
        // get broker JMX view

        String domain = "org.apache.activemq";
        ObjectName brokerName = new ObjectName(domain + ":type=Broker,brokerName=localhost");

        final BrokerViewMBean view = (BrokerViewMBean)
                brokerService.getManagementContext().newProxyInstance(brokerName, BrokerViewMBean.class, true);

        // connect
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));
        assertEquals(view.getDurableTopicSubscribers().length, 0);

        // subscribe
        frame = "SUBSCRIBE\n" + "destination:/topic/" + getQueueName() + "\n" + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // wait a bit for MBean to get refreshed
        Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                return view.getDurableTopicSubscribers().length == 1;
            }
        });

        assertEquals(view.getDurableTopicSubscribers().length, 1);

        // disconnect
        frame = "DISCONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToBroker().getCurrentConnectionsCount() == 1;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(25));

        //reconnect
        stompConnect();
        // connect
        frame = "CONNECT\n" + "login:system\n" + "passcode:manager\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        // unsubscribe
        frame = "UNSUBSCRIBE\n" + "destination:/topic/" + getQueueName() + "\n" + "activemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                return view.getDurableTopicSubscribers().length == 0 && view.getInactiveDurableTopicSubscribers().length == 0;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(25));

        assertEquals(view.getDurableTopicSubscribers().length, 0);
        assertEquals(view.getInactiveDurableTopicSubscribers().length, 0);
    }

    @Test(timeout = 60000)
    public void testDurableSubAttemptOnQueueFails() throws Exception {
        // get broker JMX view

        String domain = "org.apache.activemq";
        ObjectName brokerName = new ObjectName(domain + ":type=Broker,brokerName=localhost");

        BrokerViewMBean view = (BrokerViewMBean)
                brokerService.getManagementContext().newProxyInstance(brokerName, BrokerViewMBean.class, true);

        // connect
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));
        assertEquals(view.getQueueSubscribers().length, 0);

        // subscribe
        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));

        assertEquals(view.getQueueSubscribers().length, 0);
    }

    @Test(timeout = 60000)
    public void testMessageIdHeader() throws Exception {
        stompConnection.connect("system", "manager");

        stompConnection.begin("tx1");
        stompConnection.send("/queue/" + getQueueName(), "msg", "tx1", null);
        stompConnection.commit("tx1");

        stompConnection.subscribe("/queue/" + getQueueName());
        StompFrame stompMessage = stompConnection.receive();
        assertNull(stompMessage.getHeaders().get("transaction"));
    }

    @Test(timeout = 60000)
    public void testPrefetchSizeOfOneClientAck() throws Exception {
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

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received the second message");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame);

        StompFrame frame1 = stompConnection.receive();
        assertEquals(frame1.getBody(), "message 2");

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received the third message");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame1);
        StompFrame frame2 = stompConnection.receive();
        assertEquals(frame2.getBody(), "message 3");

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received the fourth message");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame2);
        StompFrame frame3 = stompConnection.receive();
        assertEquals(frame3.getBody(), "message 4");

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received the fifth message");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame3);
        StompFrame frame4 = stompConnection.receive();
        assertEquals(frame4.getBody(), "message 5");

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received any more messages");
            }
        } catch (SocketTimeoutException soe) {}

        stompConnection.ack(frame4);

        try {
            StompFrame frameNull = stompConnection.receive(500);
            if (frameNull != null) {
                fail("Should not have received the any more messages");
            }
        } catch (SocketTimeoutException soe) {}
    }

    @Test(timeout = 60000)
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

        StompFrame frame = stompConnection.receive(20000);
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

        StompFrame frame3 = stompConnection.receive(20000);
        assertEquals(frame3.getBody(), "message 3");
        stompConnection.ack(frame3, "tx2");

        StompFrame frame4 = stompConnection.receive(20000);
        assertEquals(frame4.getBody(), "message 4");
        stompConnection.ack(frame4, "tx2");

        stompConnection.commit("tx2");

        stompConnection.begin("tx3");
        StompFrame frame5 = stompConnection.receive(20000);
        assertEquals(frame5.getBody(), "message 5");
        stompConnection.ack(frame5, "tx3");
        stompConnection.commit("tx3");

        stompDisconnect();
    }

    @Test(timeout = 60000)
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
    }

    @Test(timeout = 60000)
    public void testTempDestination() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testJMSXUserIDIsSetInMessage() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(5000);
        assertNotNull(message);
        assertEquals("system", message.getStringProperty(Stomp.Headers.Message.USERID));
    }

    @Test(timeout = 60000)
    public void testJMSXUserIDIsSetInStompMessage() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
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

    @Test(timeout = 60000)
    public void testClientSetMessageIdIsIgnored() throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Message.MESSAGE_ID, "Thisisnotallowed");
        headers.put(Stomp.Headers.Message.TIMESTAMP, "1234");
        headers.put(Stomp.Headers.Message.REDELIVERED, "true");
        headers.put(Stomp.Headers.Message.SUBSCRIPTION, "Thisisnotallowed");
        headers.put(Stomp.Headers.Message.USERID, "Thisisnotallowed");

        stompConnection.connect("system", "manager");

        stompConnection.send("/queue/" + getQueueName(), "msg", null, headers);

        stompConnection.subscribe("/queue/" + getQueueName());
        StompFrame stompMessage = stompConnection.receive();

        Map<String, String> mess_headers = new HashMap<String, String>();
        mess_headers = stompMessage.getHeaders();

        assertFalse("Thisisnotallowed".equals(mess_headers.get(Stomp.Headers.Message.MESSAGE_ID)
                ));
        assertTrue("1234".equals(mess_headers.get(Stomp.Headers.Message.TIMESTAMP)));
        assertNull(mess_headers.get(Stomp.Headers.Message.REDELIVERED));
        assertNull(mess_headers.get(Stomp.Headers.Message.SUBSCRIPTION));
        assertEquals("system", mess_headers.get(Stomp.Headers.Message.USERID));
    }

    @Test(timeout = 60000)
    public void testExpire() throws Exception {
        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();
        long timestamp = System.currentTimeMillis() - 100;
        headers.put(Stomp.Headers.Message.EXPIRATION_TIME, String.valueOf(timestamp));
        headers.put(Stomp.Headers.Send.PERSISTENT, "true");

        stompConnection.send("/queue/" + getQueueName(), "msg", null, headers);

        stompConnection.subscribe("/queue/ActiveMQ.DLQ");
        StompFrame stompMessage = stompConnection.receive(35000);
        assertNotNull(stompMessage);
        assertEquals(stompMessage.getHeaders().get(Stomp.Headers.Message.ORIGINAL_DESTINATION), "/queue/" + getQueueName());
    }

    @Test(timeout = 60000)
    public void testDefaultJMSReplyToDest() throws Exception {
        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Send.REPLY_TO, "JustAString");
        headers.put(Stomp.Headers.Send.PERSISTENT, "true");

        stompConnection.send("/queue/" + getQueueName(), "msg-with-reply-to", null, headers);

        stompConnection.subscribe("/queue/" + getQueueName());
        StompFrame stompMessage = stompConnection.receive(1000);
        assertNotNull(stompMessage);
        assertEquals(""  + stompMessage, stompMessage.getHeaders().get(Stomp.Headers.Send.REPLY_TO), "JustAString");
    }

    @Test(timeout = 60000)
    public void testPersistent() throws Exception {
        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Message.PERSISTENT, "true");

        stompConnection.send("/queue/" + getQueueName(), "hello", null, headers);

        stompConnection.subscribe("/queue/" + getQueueName());

        StompFrame stompMessage = stompConnection.receive();
        assertNotNull(stompMessage);
        assertNotNull(stompMessage.getHeaders().get(Stomp.Headers.Message.PERSISTENT));
        assertEquals(stompMessage.getHeaders().get(Stomp.Headers.Message.PERSISTENT), "true");
    }

    @Test(timeout = 60000)
    public void testPersistentDefaultValue() throws Exception {
        stompConnection.connect("system", "manager");

        HashMap<String, String> headers = new HashMap<String, String>();

        stompConnection.send("/queue/" + getQueueName(), "hello", null, headers);

        stompConnection.subscribe("/queue/" + getQueueName());

        StompFrame stompMessage = stompConnection.receive();
        assertNotNull(stompMessage);
        assertNull(stompMessage.getHeaders().get(Stomp.Headers.Message.PERSISTENT));
    }

    @Test(timeout = 60000)
    public void testReceiptNewQueue() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + 1234 + "\n" + "id:8fee4b8-4e5c9f66-4703-e936-3" + "\n" + "receipt:8fee4b8-4e5c9f66-4703-e936-2" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame receipt = stompConnection.receive();
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        assertEquals("8fee4b8-4e5c9f66-4703-e936-2", receipt.getHeaders().get("receipt-id"));

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + 123 + "\ncontent-length:0" + " \n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + 123 + "\n" + "id:8fee4b8-4e5c9f66-4703-e936-2" + "\n" + "receipt:8fee4b8-4e5c9f66-4703-e936-1" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        receipt = stompConnection.receive();
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        assertEquals("8fee4b8-4e5c9f66-4703-e936-1", receipt.getHeaders().get("receipt-id"));

        StompFrame message = stompConnection.receive();
        assertTrue(message.getAction().startsWith("MESSAGE"));

        String length = message.getHeaders().get("content-length");
        assertEquals("0", length);
        assertEquals(0, message.getContent().length);
    }

    @Test(timeout = 60000)
    public void testTransactedClientAckBrokerStats() throws Exception {
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        sendMessage(getName());
        sendMessage(getName());

        stompConnection.begin("tx1");

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame message = stompConnection.receive();
        assertTrue(message.getAction().equals("MESSAGE"));
        stompConnection.ack(message, "tx1");

        message = stompConnection.receive();
        assertTrue(message.getAction().equals("MESSAGE"));
        stompConnection.ack(message, "tx1");

        stompConnection.commit("tx1");

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        final QueueViewMBean queueView = getProxyToQueue(getQueueName());
        Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getDequeueCount() == 2;
            }
        });
        assertEquals(2, queueView.getDispatchCount());
        assertEquals(2, queueView.getDequeueCount());
        assertEquals(0, queueView.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testReplytoModification() throws Exception {
        String replyto = "some destination";
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" + "reply-to:" + replyto + "\n\nhello world" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame message = stompConnection.receive();
        assertTrue(message.getAction().equals("MESSAGE"));
        assertEquals(replyto, message.getHeaders().get("reply-to"));
    }

    @Test(timeout = 60000)
    public void testReplyToDestinationNaming() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        doTestActiveMQReplyToTempDestination("topic");
        doTestActiveMQReplyToTempDestination("queue");
    }

    @Test(timeout = 60000)
    public void testSendNullBodyTextMessage() throws Exception {
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage(null);
        frame = stompConnection.receiveFrame();
        assertNotNull("Message not received", frame);
    }

    private void doTestActiveMQReplyToTempDestination(String type) throws Exception {
        LOG.info("Starting test on Temp Destinations using a temporary: " + type);

        final String dest = "/" + type + "/" + getQueueName();
        final String tempDest = String.format("/temp-%s/2C26441740C0ECC9tt1", type);
        LOG.info("Test is using out-bound topic: " + dest + ", and replyTo dest: " + tempDest);

        // Subscribe both to the out-bound destination and the response tempt destination
        stompConnection.subscribe(dest);
        stompConnection.subscribe(tempDest);

        // Send a Message with the ReplyTo value set.
        HashMap<String, String> properties = new HashMap<String, String>();
        properties.put(Stomp.Headers.Send.REPLY_TO, tempDest);
        LOG.info(String.format("Sending request message: SEND with %s=%s", Stomp.Headers.Send.REPLY_TO, tempDest));
        stompConnection.send(dest, "REQUEST", null, properties);

        // The subscription should receive a response with the ReplyTo property set.
        StompFrame received = stompConnection.receive();
        assertNotNull(received);
        String remoteReplyTo = received.getHeaders().get(Stomp.Headers.Send.REPLY_TO);
        assertNotNull(remoteReplyTo);
        assertTrue(remoteReplyTo.startsWith(String.format("/temp-%s/", type)));
        LOG.info(String.format("Received request message: %s with %s=%s", received.getAction(), Stomp.Headers.Send.REPLY_TO, remoteReplyTo));

        // Reply to the request using the given ReplyTo destination
        stompConnection.send(remoteReplyTo, "RESPONSE");

        // The response should be received by the Temporary Destination subscription
        StompFrame reply = stompConnection.receive();
        assertNotNull(reply);
        assertEquals("MESSAGE", reply.getAction());
        LOG.info(String.format("Response %s received", reply.getAction()));

        BrokerViewMBean broker = getProxyToBroker();
        if (type.equals("topic")) {
            assertEquals(1, broker.getTemporaryTopics().length);
        } else {
            assertEquals(1, broker.getTemporaryQueues().length);
        }
    }

    @Test(timeout = 60000)
    public void testReplyToAcrossConnections() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        doReplyToAcrossConnections("topic");
        doReplyToAcrossConnections("queue");
    }

    private void doReplyToAcrossConnections(String type) throws Exception {
        LOG.info("Starting test on Temp Destinations using a temporary: " + type);

        StompConnection responder = new StompConnection();
        stompConnect(responder);
        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        responder.sendFrame(frame);

        frame = responder.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        final String dest = "/" + type + "/" + getQueueName();
        final String tempDest = String.format("/temp-%s/2C26441740C0ECC9tt1:1:0:1", type);
        LOG.info("Test is using out-bound topic: " + dest + ", and replyTo dest: " + tempDest);

        // Subscribe to the temp destination, this is where we get our response.
        stompConnection.subscribe(tempDest);

        // Subscribe to the destination, this is where we get our request.
        HashMap<String, String> properties = new HashMap<String, String>();
        properties.put(Stomp.Headers.RECEIPT_REQUESTED, "subscribe-1");
        responder.subscribe(dest, null, properties);

        frame = responder.receiveFrame();
        assertTrue("Receipt Frame: " + frame, frame.trim().startsWith("RECEIPT"));
        assertTrue("Receipt contains correct receipt-id " + frame, frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);

        // Send a Message with the ReplyTo value set.
        properties = new HashMap<String, String>();
        properties.put(Stomp.Headers.Send.REPLY_TO, tempDest);
        properties.put(Stomp.Headers.RECEIPT_REQUESTED, "send-1");
        LOG.info(String.format("Sending request message: SEND with %s=%s", Stomp.Headers.Send.REPLY_TO, tempDest));
        stompConnection.send(dest, "REQUEST", null, properties);

        frame = stompConnection.receiveFrame();
        assertTrue("Receipt Frame: " + frame, frame.trim().startsWith("RECEIPT"));
        assertTrue("Receipt contains correct receipt-id " + frame, frame.indexOf(Stomp.Headers.Response.RECEIPT_ID) >= 0);

        // The subscription should receive a response with the ReplyTo property set.
        StompFrame received = responder.receive();
        assertNotNull(received);
        String remoteReplyTo = received.getHeaders().get(Stomp.Headers.Send.REPLY_TO);
        assertNotNull(remoteReplyTo);
        assertTrue(remoteReplyTo.startsWith(String.format("/remote-temp-%s/", type)));
        LOG.info(String.format("Received request message: %s with %s=%s", received.getAction(), Stomp.Headers.Send.REPLY_TO, remoteReplyTo));

        // Reply to the request using the given ReplyTo destination
        responder.send(remoteReplyTo, "RESPONSE");

        // The response should be received by the Temporary Destination subscription
        StompFrame reply = stompConnection.receive();
        assertNotNull(reply);
        assertEquals("MESSAGE", reply.getAction());
        assertTrue(reply.getBody().contains("RESPONSE"));
        LOG.info(String.format("Response %s received", reply.getAction()));

        BrokerViewMBean broker = getProxyToBroker();
        if (type.equals("topic")) {
            assertEquals(1, broker.getTemporaryTopics().length);
        } else {
            assertEquals(1, broker.getTemporaryQueues().length);
        }
    }

    protected void assertClients(final int expected) throws Exception {
        Wait.waitFor(new Wait.Condition()
        {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getBroker().getClients().length == expected;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(100));
        org.apache.activemq.broker.Connection[] clients = brokerService.getBroker().getClients();
        int actual = clients.length;

        assertEquals("Number of clients", expected, actual);
    }

    @Test(timeout = 60000)
    public void testDisconnectDoesNotDeadlockBroker() throws Exception {
        for (int i = 0; i < 20; ++i) {
            doTestConnectionLeak();
        }
    }

    private void doTestConnectionLeak() throws Exception {
        stompConnect();

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        boolean gotMessage = false;
        boolean gotReceipt = false;

        char[] payload = new char[1024];
        Arrays.fill(payload, 'A');

        String test = "SEND\n" +
                "x-type:DEV-3485\n"  +
                "x-uuid:" + UUID.randomUUID() + "\n"  +
                "persistent:true\n"  +
                "receipt:" + UUID.randomUUID() + "\n" +
                "destination:/queue/test.DEV-3485" +
                "\n\n" +
                new String(payload) + Stomp.NULL;

        frame = "SUBSCRIBE\n" + "destination:/queue/test.DEV-3485\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        stompConnection.sendFrame(test);

        // We only want one of them, to trigger the shutdown and potentially
        // see a deadlock.
        while (!gotMessage && !gotReceipt) {
            frame = stompConnection.receiveFrame();

            LOG.debug("Received the frame: " + frame);

            if (frame.startsWith("RECEIPT")) {
                gotReceipt = true;
            } else if(frame.startsWith("MESSAGE")) {
                gotMessage = true;
            } else {
                fail("Received a frame that we were not expecting.");
            }
        }
    }

    @Test(timeout = 60000)
    public void testHeaderValuesAreTrimmed1_0() throws Exception {

        String connectFrame = "CONNECT\n" +
                "login:system\n" +
                "passcode:manager\n" +
                "accept-version:1.0\n" +
                "host:localhost\n" +
                "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String message = "SEND\n" + "destination:/queue/" + getQueueName() +
                "\ntest1: value" +
                "\ntest2:value " +
                "\ntest3: value " +
                "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(message);

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));

        assertEquals("value", received.getHeaders().get("test1"));
        assertEquals("value", received.getHeaders().get("test2"));
        assertEquals("value", received.getHeaders().get("test3"));

        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    @Test(timeout = 60000)
    public void testSendReceiveBigMessage() throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        int size = 100;
        char[] bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        String bigBody = new String(bigBodyArray);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame sframe = stompConnection.receive();
        assertNotNull(sframe);
        assertEquals("MESSAGE", sframe.getAction());
        assertEquals(bigBody, sframe.getBody());

        size = 3000000;
        bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        bigBody = new String(bigBodyArray);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        sframe = stompConnection.receive(5000);
        assertNotNull(sframe);
        assertEquals("MESSAGE", sframe.getAction());
        assertEquals(bigBody, sframe.getBody());
    }

    @Test(timeout = 60000)
    public void testAckInTransactionTopic() throws Exception {
        doTestAckInTransaction(true);
    }

    @Test(timeout = 60000)
    public void testAckInTransactionQueue() throws Exception {
        doTestAckInTransaction(false);
    }

    @Test(timeout = 60000)
    public void testFrameHeaderEscapes() throws Exception {
        String connectFrame = "STOMP\n" +
                "login:system\n" +
                "passcode:manager\n" +
                "accept-version:1.0\n" +
                "host:localhost\n" +
                "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                "id:12345\n" +
                "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" +
                "colon:\\c\n" +
                "linefeed:\\n\n" +
                "backslash:\\\\\n" +
                "carriagereturn:\\r\n" +
                "\n" + Stomp.NULL;
        stompConnection.sendFrame(message);

        frame = stompConnection.receiveFrame();

        LOG.debug("Broker sent: " + frame);
        assertTrue(frame.startsWith("MESSAGE"));
        assertFalse(frame.contains("colon:\\c\n"));
        assertFalse(frame.contains("linefeed:\\n\n"));
        assertFalse(frame.contains("backslash:\\\\\n"));
        assertFalse(frame.contains("carriagereturn:\\\\r\n"));

    }

    public void doTestAckInTransaction(boolean topic) throws Exception {

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;

        stompConnection.sendFrame(frame);
        stompConnection.receive();
        String destination = (topic ? "/topic" : "/queue") + "/test";
        stompConnection.subscribe(destination, Stomp.Headers.Subscribe.AckModeValues.CLIENT);

        for (int j = 0; j < 5; j++) {

            for (int i = 0; i < 10; i++) {
                stompConnection.send(destination , "message" + i);
            }

            stompConnection.begin("tx"+j);

            for (int i = 0; i < 10; i++) {
                StompFrame message = stompConnection.receive();
                stompConnection.ack(message, "tx"+j);

            }
            stompConnection.commit("tx"+j);
        }

        List<Subscription> subs = getDestinationConsumers(brokerService,
                ActiveMQDestination.createDestination("test", topic ? ActiveMQDestination.TOPIC_TYPE : ActiveMQDestination.QUEUE_TYPE));


        for (Subscription subscription : subs) {
            final AbstractSubscription abstractSubscription = (AbstractSubscription) subscription;

            assertTrue("prefetchExtension should be back to Zero after commit", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    LOG.info("ext: " + abstractSubscription.getPrefetchExtension().get());
                    return abstractSubscription.getPrefetchExtension().get() == 0;
                }
            }));
        }
    }

    public static List<Subscription> getDestinationConsumers(BrokerService broker, ActiveMQDestination destination) {
        List<Subscription> result = null;
        org.apache.activemq.broker.region.Destination dest = getDestination(broker, destination);
        if (dest != null) {
            result = dest.getConsumers();
        }
        return result;
    }

    public static org.apache.activemq.broker.region.Destination getDestination(BrokerService target, ActiveMQDestination destination) {
        org.apache.activemq.broker.region.Destination result = null;
        for (org.apache.activemq.broker.region.Destination dest : getDestinationMap(target, destination).values()) {
            if (dest.getName().equals(destination.getPhysicalName())) {
                result = dest;
                break;
            }
        }
        return result;
    }

    private static Map<ActiveMQDestination, org.apache.activemq.broker.region.Destination> getDestinationMap(BrokerService target,
                                                                                                             ActiveMQDestination destination) {
        RegionBroker regionBroker = (RegionBroker) target.getRegionBroker();
        if (destination.isTemporary()) {
            return destination.isQueue() ? regionBroker.getTempQueueRegion().getDestinationMap() :
                    regionBroker.getTempTopicRegion().getDestinationMap();
        }
        return destination.isQueue() ?
                regionBroker.getQueueRegion().getDestinationMap() :
                regionBroker.getTopicRegion().getDestinationMap();
    }


    protected SamplePojo createObjectFromJson(String data) throws Exception {
        HierarchicalStreamReader in = new JettisonMappedXmlDriver().createReader(new StringReader(data));
        return createObject(in);
    }

    protected SamplePojo createObjectFromXml(String data) throws Exception {
        HierarchicalStreamReader in = new XppReader(new StringReader(data), XppFactory.createDefaultParser());
        return createObject(in);
    }

    private SamplePojo createObject(HierarchicalStreamReader in) throws Exception {
        SamplePojo pojo = (SamplePojo) xstream.unmarshal(in);
        return pojo;
    }

    protected Map<String, String> createMapFromJson(String data) throws Exception {
        HierarchicalStreamReader in = new JettisonMappedXmlDriver().createReader(new StringReader(data));
        return createMapObject(in);
    }

    protected Map<String, String> createMapFromXml(String data) throws Exception {
        HierarchicalStreamReader in = new XppReader(new StringReader(data), XppFactory.createDefaultParser());
        return createMapObject(in);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> createMapObject(HierarchicalStreamReader in) throws Exception {
        Map<String, String> map = (Map<String, String>)xstream.unmarshal(in);
        return map;
    }
}
