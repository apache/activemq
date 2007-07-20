/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.activemq.broker.*;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.*;
import javax.jms.Connection;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class StompTest extends CombinationTestSupport {

    private BrokerService broker;
    private TransportConnector connector;
    private StompConnection stompConnection = new StompConnection();
    private Connection connection;
    private Session session;
    private ActiveMQQueue queue;
    protected String bindAddress = "stomp://localhost:0";

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        connector = broker.addConnector(bindAddress);
        broker.start();

        URI connectUri = connector.getConnectUri();
        stompConnection.open("127.0.0.1", connectUri.getPort());

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        connection = cf.createConnection();
        session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        queue = new ActiveMQQueue(getQueueName());
        connection.start();
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        return new Socket();
    }

    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

    protected void tearDown() throws Exception {
        connection.close();
        stompConnection.close();
        broker.stop();
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

    public void sendBytesMessage(byte[] msg) throws Exception{
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(msg);
        producer.send(message);
    }

    public void testConnect() throws Exception {

        String connect_frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "request-id: 1\n" + "\n" + Stomp.NULL;
        stompConnection.sendFrame(connect_frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("response-id:1") >= 0);

    }

    public void testSendMessage() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SEND\n" +
            "destination:/queue/" + getQueueName() + "\n\n" +
            "Hello World" +
            Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());

        // Make sure that the timestamp is valid - should
        // be very close to the current time.
        long tnow = System.currentTimeMillis();
        long tmsg = message.getJMSTimestamp();
        assertTrue( Math.abs(tnow - tmsg) < 1000 );
    }

    public void testJMSXGroupIdCanBeSet() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SEND\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "JMSXGroupID: TEST\n\n" +
            "Hello World" +
            Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("TEST", ((ActiveMQTextMessage)message).getGroupID());
    }


    public void testSendMessageWithCustomHeadersAndSelector() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue, "foo = 'abc'");

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SEND\n" +
            "foo:abc\n" +
            "bar:123\n" +
            "destination:/queue/" + getQueueName() + "\n\n" +
            "Hello World" +
            Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("foo", "abc", message.getStringProperty("foo"));
        assertEquals("bar", "123", message.getStringProperty("bar"));
    }

    public void testSendMessageWithStandardHeaders() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SEND\n" +
            "correlation-id:c123\n" +
            "priority:3\n" +
            "type:t345\n" +
            "JMSXGroupID:abc\n" +
            "foo:abc\n" +
            "bar:123\n" +
            "destination:/queue/" + getQueueName() + "\n\n" +
            "Hello World" +
            Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("JMSCorrelationID", "c123", message.getJMSCorrelationID());
        assertEquals("getJMSType", "t345", message.getJMSType());
        assertEquals("getJMSPriority", 3, message.getJMSPriority());
        assertEquals("foo", "abc", message.getStringProperty("foo"));
        assertEquals("bar", "123", message.getStringProperty("bar"));

        assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
        ActiveMQTextMessage amqMessage = (ActiveMQTextMessage) message;
        assertEquals("GroupID", "abc", amqMessage.getGroupID());
    }

    public void testSubscribeWithAutoAck() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage(getName());

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

        public void testSubscribeWithAutoAckAndBytesMessage() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendBytesMessage(new byte[] {1,2,3,4,5});

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        Pattern cl = Pattern.compile("Content-length:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher cl_matcher = cl.matcher(frame);
        assertTrue(cl_matcher.find());
        assertEquals("5", cl_matcher.group(1));

        assertFalse(Pattern.compile("type:\\s*null", Pattern.CASE_INSENSITIVE).matcher(frame).find());

        frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithMessageSentWithProperties() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);


        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("Hello World");
        message.setStringProperty("s", "value");
        message.setBooleanProperty("n", false);
        message.setByteProperty("byte", (byte) 9);
        message.setDoubleProperty("d", 2.0);
        message.setFloatProperty("f", (float) 6.0);
        message.setIntProperty("i", 10);
        message.setLongProperty("l", 121);
        message.setShortProperty("s", (short) 12);
        producer.send(message);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

//        System.out.println("out: "+frame);

        frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testMessagesAreInOrder() throws Exception {
        int ctr = 10;
        String[] data = new String[ctr];

        String frame =
                "CONNECT\n" +
                "login: brianm\n" +
                "passcode: wombats\n\n" +
                Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
                "SUBSCRIBE\n" +
                "destination:/queue/" + getQueueName() + "\n" +
                "ack:auto\n\n" +
                Stomp.NULL;
        stompConnection.sendFrame(frame);

        for (int i = 0; i < ctr; ++i) {
            data[i] = getName() + i;
            sendMessage(data[i]);
        }

        for (int i = 0; i < ctr; ++i) {
            frame = stompConnection.receiveFrame();
            assertTrue("Message not in order", frame.indexOf(data[i]) >=0 );
        }

        // sleep a while before publishing another set of messages
        waitForFrameToTakeEffect();

        for (int i = 0; i < ctr; ++i) {
            data[i] = getName() + ":second:" + i;
            sendMessage(data[i]);
        }

        for (int i = 0; i < ctr; ++i) {
            frame = stompConnection.receiveFrame();
            assertTrue("Message not in order", frame.indexOf(data[i]) >=0 );
        }

        frame =
                "DISCONNECT\n" +
                "\n\n" +
                Stomp.NULL;
        stompConnection.sendFrame(frame);
    }


    public void testSubscribeWithAutoAckAndSelector() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "selector: foo = 'zzz'\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        sendMessage("Ignored message", "foo", "1234");
        sendMessage("Real message", "foo", "zzz");

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        assertTrue("Should have received the real message but got: " + frame, frame.indexOf("Real message") > 0);

       frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
       stompConnection.sendFrame(frame);
    }


    public void testSubscribeWithClientAck() throws Exception {

       String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
       stompConnection.sendFrame(frame);

       frame = stompConnection.receiveFrame();
       assertTrue(frame.startsWith("CONNECTED"));


       frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "ack:client\n\n"+
            Stomp.NULL;


       stompConnection.sendFrame(frame);
       sendMessage(getName());
       frame = stompConnection.receiveFrame();
       assertTrue(frame.startsWith("MESSAGE"));

       frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
       stompConnection.sendFrame(frame);

       // message should be received since message was not acknowledged
       MessageConsumer consumer = session.createConsumer(queue);
       TextMessage message = (TextMessage) consumer.receive(1000);
       assertNotNull(message);
       assertTrue(message.getJMSRedelivered());
    }

    public void testUnsubscribe() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        //send a message to our queue
        sendMessage("first message");

        //receive message from socket
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        //remove suscription
        frame =
            "UNSUBSCRIBE\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        waitForFrameToTakeEffect();

        //send a message to our queue
        sendMessage("second message");


        try {
            frame = stompConnection.receiveFrame();
            log.info("Received frame: " + frame);
            fail("No message should have been received since subscription was removed");
        }catch (SocketTimeoutException e){

        }

    }


    public void testTransactionCommit() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));

        frame =
            "BEGIN\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "transaction: tx1\n" +
            "\n\n" +
            "Hello World" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "COMMIT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        waitForFrameToTakeEffect();

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull("Should have received a message", message);
    }

    public void testTransactionRollback() throws Exception {
        MessageConsumer consumer = session.createConsumer(queue);

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));

        frame =
            "BEGIN\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "transaction: tx1\n" +
            "\n" +
            "first message" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        //rollback first message
        frame =
            "ABORT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "BEGIN\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/" + getQueueName() + "\n" +
            "transaction: tx1\n" +
            "\n" +
            "second message" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame =
            "COMMIT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        stompConnection.sendFrame(frame);

        // This test case is currently failing
        waitForFrameToTakeEffect();

        //only second msg should be received since first msg was rolled back
        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("second message", message.getText().trim());
    }

    public void testDisconnectedClientsAreRemovedFromTheBroker() throws Exception {
        assertClients(1);
        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;

        stompConnection.sendFrame(frame);

        // This test case is currently failing
        waitForFrameToTakeEffect();

        assertClients(2);

        // now lets kill the stomp connection
        stompConnection.close();

        Thread.sleep(2000);

        assertClients(1);
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
