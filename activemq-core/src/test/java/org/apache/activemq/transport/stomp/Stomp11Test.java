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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stomp11Test extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompTest.class);

    protected String bindAddress = "stomp://localhost:61613";
    protected String confUri = "xbean:org/apache/activemq/transport/stomp/stomp-auth-broker.xml";
    protected String jmsUri = "vm://localhost";

    private BrokerService broker;
    private StompConnection stompConnection = new StompConnection();
    private Connection connection;
    private Session session;
    private ActiveMQQueue queue;

    @Override
    protected void setUp() throws Exception {

        broker = BrokerFactory.createBroker(new URI(confUri));
        broker.start();
        broker.waitUntilStarted();

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

    @Override
    protected void tearDown() throws Exception {
        try {
            stompDisconnect();
        } catch(Exception e) {
            // Some tests explicitly disconnect from stomp so can ignore
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void stompDisconnect() throws IOException {
        if (stompConnection != null) {
            stompConnection.close();
            stompConnection = null;
        }
    }

    public void testConnect() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "request-id: 1\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("response-id:1") >= 0);
        assertTrue(f.indexOf("version:1.1") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testConnectWithVersionOptions() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.0,1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.1") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testConnectWithValidFallback() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.0,10.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.0") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testConnectWithInvalidFallback() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:9.0,10.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("ERROR"));
        assertTrue(f.indexOf("version") >= 0);
        assertTrue(f.indexOf("message:") >= 0);
    }

    public void testHeartbeats() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:0,1000\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.1") >= 0);
        assertTrue(f.indexOf("heart-beat:") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        LOG.debug("Broker sent: " + f);

        stompConnection.getStompSocket().getOutputStream().write('\n');

        DataInputStream in = new DataInputStream(stompConnection.getStompSocket().getInputStream());
        in.read();
        {
            long startTime = System.currentTimeMillis();
            int input = in.read();
            assertEquals("did not receive the correct hear beat value", '\n', input);
            long endTime = System.currentTimeMillis();
            assertTrue("Broker did not send KeepAlive in time", (endTime - startTime) >= 900);
        }
        {
            long startTime = System.currentTimeMillis();
            int input = in.read();
            assertEquals("did not receive the correct hear beat value", '\n', input);
            long endTime = System.currentTimeMillis();
            assertTrue("Broker did not send KeepAlive in time", (endTime - startTime) >= 900);
        }

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testHeartbeatsDropsIdleConnection() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:1000,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.1") >= 0);
        assertTrue(f.indexOf("heart-beat:") >= 0);
        assertTrue(f.indexOf("session:") >= 0);
        LOG.debug("Broker sent: " + f);

        long startTime = System.currentTimeMillis();

        try {
            f = stompConnection.receiveFrame();
            LOG.debug("Broker sent: " + f);
            fail();
        } catch(Exception e) {
        }

        long endTime = System.currentTimeMillis();
        assertTrue("Broker did close idle connection in time.", (endTime - startTime) >= 1000);
    }

    public void testSendAfterMissingHeartbeat() throws Exception {

        String connectFrame = "STOMP\n" + "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:1000,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);
        String f = stompConnection.receiveFrame();
        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.1") >= 0);
        assertTrue(f.indexOf("heart-beat:") >= 0);
        assertTrue(f.indexOf("session:") >= 0);
        LOG.debug("Broker sent: " + f);

        Thread.sleep(5000);

        try {
            String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;
            stompConnection.sendFrame(message);
            fail("SEND frame has been accepted after missing heart beat");
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void testRejectInvalidHeartbeats1() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("ERROR"));
        assertTrue(f.indexOf("heart-beat") >= 0);
        assertTrue(f.indexOf("message:") >= 0);
    }

    public void testRejectInvalidHeartbeats2() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:T,0\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("ERROR"));
        assertTrue(f.indexOf("heart-beat") >= 0);
        assertTrue(f.indexOf("message:") >= 0);
    }

    public void testRejectInvalidHeartbeats3() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "heart-beat:100,10,50\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("ERROR"));
        assertTrue(f.indexOf("heart-beat") >= 0);
        assertTrue(f.indexOf("message:") >= 0);
    }

    public void testSubscribeAndUnsubscribe() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(message);

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame stompFrame = stompConnection.receive();
        assertTrue(stompFrame.getAction().equals("MESSAGE"));

        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        Thread.sleep(2000);

        stompConnection.sendFrame(message);

        try {
            frame = stompConnection.receiveFrame();
            LOG.info("Received frame: " + frame);
            fail("No message should have been received since subscription was removed");
        } catch (SocketTimeoutException e) {
        }

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testSubscribeWithNoId() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testUnsubscribeWithNoId() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        Thread.sleep(2000);

        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testAckMessageWithId() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(message);

        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "id:12345\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));

        frame = "ACK\n" + "subscription:12345\n" + "message-id:" +
                received.getHeaders().get("message-id") + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testAckMessageWithNoId() throws Exception {

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(message);

        String subscribe = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                           "id:12345\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(subscribe);

        StompFrame received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));

        String ack = "ACK\n" + "message-id:" +
                     received.getHeaders().get("message-id") + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(ack);

        StompFrame error = stompConnection.receive();
        assertTrue(error.getAction().equals("ERROR"));

        String unsub = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(unsub);

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    public void testQueueBrowerSubscription() throws Exception {

        final int MSG_COUNT = 10;

        String connectFrame = "STOMP\n" +
                              "login: system\n" +
                              "passcode: manager\n" +
                              "accept-version:1.1\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));

        for(int i = 0; i < MSG_COUNT; ++i) {
            String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" +
                             "receipt:0\n" +
                             "\n" + "Hello World {" + i + "}" + Stomp.NULL;
            stompConnection.sendFrame(message);
            StompFrame repsonse = stompConnection.receive();
            assertEquals("0", repsonse.getHeaders().get(Stomp.Headers.Response.RECEIPT_ID));
        }

        String subscribe = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                           "id:12345\n" + "browser:true\n\n" + Stomp.NULL;
        stompConnection.sendFrame(subscribe);

        for(int i = 0; i < MSG_COUNT; ++i) {
            StompFrame message = stompConnection.receive();
            assertEquals(Stomp.Responses.MESSAGE, message.getAction());
            assertEquals("12345", message.getHeaders().get(Stomp.Headers.Message.SUBSCRIPTION));
        }

        // We should now get a browse done message
        StompFrame browseDone = stompConnection.receive();
        LOG.debug("Browse Done: " + browseDone.toString());
        assertEquals(Stomp.Responses.MESSAGE, browseDone.getAction());
        assertEquals("12345", browseDone.getHeaders().get(Stomp.Headers.Message.SUBSCRIPTION));
        assertEquals("end", browseDone.getHeaders().get(Stomp.Headers.Message.BROWSER));
        assertTrue(browseDone.getHeaders().get(Stomp.Headers.Message.DESTINATION) != null);

        String unsub = "UNSUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                       "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(unsub);

        Thread.sleep(2000);

        subscribe = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "id:12345\n\n" + Stomp.NULL;
        stompConnection.sendFrame(subscribe);

        for(int i = 0; i < MSG_COUNT; ++i) {
            StompFrame message = stompConnection.receive();
            assertEquals(Stomp.Responses.MESSAGE, message.getAction());
            assertEquals("12345", message.getHeaders().get(Stomp.Headers.Message.SUBSCRIPTION));
        }

        stompConnection.sendFrame(unsub);

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }


    public void testSendMessageWithStandardHeadersEncoded() throws Exception {

        MessageConsumer consumer = session.createConsumer(queue);

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n" +
                "accept-version:1.1" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" + "correlation-id:c1\\:\\n\\23\n" + "priority:3\n" + "type:t34:5\n" + "JMSXGroupID:abc\n" + "foo:a\\bc\n" + "bar:123\n" + "destination:/queue/" + getQueueName() + "\n\n" + "Hello World"
                + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());
        assertEquals("JMSCorrelationID", "c1\\:\n\\23", message.getJMSCorrelationID());
        assertEquals("getJMSType", "t34:5", message.getJMSType());
        assertEquals("getJMSPriority", 3, message.getJMSPriority());
        assertEquals("foo", "a\\bc", message.getStringProperty("foo"));
        assertEquals("bar", "123", message.getStringProperty("bar"));

        assertEquals("JMSXGroupID", "abc", message.getStringProperty("JMSXGroupID"));
        ActiveMQTextMessage amqMessage = (ActiveMQTextMessage)message;
        assertEquals("GroupID", "abc", amqMessage.getGroupID());
    }


    public void testSubscribeWithMessageSentWithEncodedProperties() throws Exception {

        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n" +  "accept-version:1.1" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "id:12345\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("Hello World");
        message.setStringProperty("s", "\\value:");
        producer.send(message);

        frame = stompConnection.receiveFrame();
        assertTrue("" + frame, frame.startsWith("MESSAGE"));

        int start =  frame.indexOf("\ns:") + 3;
        final String expectedEncoded = "\\\\value\\c";
        final String headerVal = frame.substring(start, start + expectedEncoded.length());
        assertEquals("" + frame, expectedEncoded, headerVal);

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

}
