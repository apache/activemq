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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;

import javax.jms.Connection;

import org.apache.activemq.broker.TransportConnector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stomp12Test extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(Stomp12Test.class);

    private Connection connection;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        stompConnect();

        connection = cf.createConnection("system", "manager");
        connection.start();
    }

    @Override
    protected void addStompConnector() throws Exception {
        TransportConnector connector = brokerService.addConnector("stomp://0.0.0.0:"+port);
        port = connector.getConnectUri().getPort();
    }

    @Override
    protected Socket createSocket() throws IOException {
        return new Socket("127.0.0.1", this.port);
    }

    @Override
    protected String getQueueName() {
        return getClass().getName() + "." + getName();
    }

    @Test
    public void testTelnetStyleSends() throws Exception {

        stompConnection.setVersion(Stomp.V1_2);

        String connect = "CONNECT\r\n" +
                         "accept-version:1.2\r\n" +
                         "login:system\r\n" +
                         "passcode:manager\r\n" +
                         "\r\n" +
                         "\u0000\r\n";

        stompConnection.sendFrame(connect);

        String f = stompConnection.receiveFrame();
        LOG.info("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.2") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String send = "SUBSCRIBE\r\n" +
                      "id:1\r\n" +
                      "destination:/queue/" + getQueueName() + "\r\n" +
                      "receipt:1\r\n" +
                      "\r\n"+
                      "\u0000\r\n";

        stompConnection.sendFrame(send);

        StompFrame receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        String receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        String disconnect = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(disconnect);
    }

    @Test
    public void testClientAckWithoutAckId() throws Exception {

        stompConnection.setVersion(Stomp.V1_2);

        String connect = "STOMP\r\n" +
                         "accept-version:1.2\r\n" +
                         "login:system\r\n" +
                         "passcode:manager\r\n" +
                         "\r\n" +
                         "\u0000\r\n";

        stompConnection.sendFrame(connect);

        String f = stompConnection.receiveFrame();
        LOG.info("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.2") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String subscribe = "SUBSCRIBE\n" +
                           "id:1\n" +
                           "activemq.prefetchSize=1\n" +
                           "ack:client\n" +
                           "destination:/queue/" + getQueueName() + "\n" +
                           "receipt:1\n" +
                           "\n" + Stomp.NULL;

        stompConnection.sendFrame(subscribe);

        StompFrame receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        String receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "1" + Stomp.NULL;
        stompConnection.sendFrame(message);

        StompFrame received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("1", received.getBody());

        String frame = "ACK\n" + "message-id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        received = stompConnection.receive();
        assertTrue(received.getAction().equals("ERROR"));
        LOG.info("Broker sent: " + received);

        String disconnect = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(disconnect);
    }

    @Test
    public void testClientAck() throws Exception {

        stompConnection.setVersion(Stomp.V1_2);

        String connect = "STOMP\r\n" +
                         "accept-version:1.2\r\n" +
                         "login:system\r\n" +
                         "passcode:manager\r\n" +
                         "\r\n" +
                         "\u0000\r\n";

        stompConnection.sendFrame(connect);

        String f = stompConnection.receiveFrame();
        LOG.info("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.2") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String subscribe = "SUBSCRIBE\n" +
                           "id:1\n" +
                           "ack:client\n" +
                           "destination:/queue/" + getQueueName() + "\n" +
                           "receipt:1\n" +
                           "\n" + Stomp.NULL;

        stompConnection.sendFrame(subscribe);

        StompFrame receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        String receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "1" + Stomp.NULL;
        stompConnection.sendFrame(message);
        message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "2" + Stomp.NULL;
        stompConnection.sendFrame(message);

        StompFrame received = stompConnection.receive();
        LOG.info("Stomp Message: {}", received);
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("1", received.getBody());

        received = stompConnection.receive();
        LOG.info("Stomp Message: {}", received);
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("2", received.getBody());

        String frame = "ACK\n" + "id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "DISCONNECT\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e){}

        // reconnect and send some messages to the offline subscribers and then try to get
        // them after subscribing again.
        stompConnect();
        stompConnection.sendFrame(connect);
        frame = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + frame);
        assertTrue(frame.startsWith("CONNECTED"));

        stompConnection.sendFrame(subscribe);

        receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "3" + Stomp.NULL;
        stompConnection.sendFrame(message);

        received = stompConnection.receive();
        LOG.info("Stomp Message: {}", received);
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("3", received.getBody());

        frame = "ACK\n" + "id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String disconnect = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(disconnect);
    }

    @Test
    public void testClientIndividualAck() throws Exception {

        stompConnection.setVersion(Stomp.V1_2);

        String connect = "STOMP\r\n" +
                         "accept-version:1.2\r\n" +
                         "login:system\r\n" +
                         "passcode:manager\r\n" +
                         "\r\n" +
                         "\u0000\r\n";

        stompConnection.sendFrame(connect);

        String f = stompConnection.receiveFrame();
        LOG.info("Broker sent: " + f);

        assertTrue(f.startsWith("CONNECTED"));
        assertTrue(f.indexOf("version:1.2") >= 0);
        assertTrue(f.indexOf("session:") >= 0);

        String subscribe = "SUBSCRIBE\n" +
                           "id:1\n" +
                           "ack:client-individual\n" +
                           "destination:/queue/" + getQueueName() + "\n" +
                           "receipt:1\n" +
                           "\n" + Stomp.NULL;

        stompConnection.sendFrame(subscribe);

        StompFrame receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        String receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        String message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "1" + Stomp.NULL;
        stompConnection.sendFrame(message);
        message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "2" + Stomp.NULL;
        stompConnection.sendFrame(message);

        StompFrame received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("1", received.getBody());

        received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("2", received.getBody());

        String frame = "ACK\n" + "id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        try {
            Thread.sleep(400);
        } catch (InterruptedException e){}

        // reconnect and send some messages to the offline subscribers and then try to get
        // them after subscribing again.
        stompConnect();
        stompConnection.sendFrame(connect);
        frame = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + frame);
        assertTrue(frame.startsWith("CONNECTED"));

        stompConnection.sendFrame(subscribe);

        receipt = stompConnection.receive();
        LOG.info("Broker sent: " + receipt);
        assertTrue(receipt.getAction().startsWith("RECEIPT"));
        receiptId = receipt.getHeaders().get("receipt-id");
        assertEquals("1", receiptId);

        message = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + "3" + Stomp.NULL;
        stompConnection.sendFrame(message);

        received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("1", received.getBody());

        frame = "ACK\n" + "id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        received = stompConnection.receive();
        assertTrue(received.getAction().equals("MESSAGE"));
        assertTrue(received.getHeaders().containsKey(Stomp.Headers.Message.ACK_ID));
        assertEquals("3", received.getBody());

        frame = "ACK\n" + "id:" +
                received.getHeaders().get(Stomp.Headers.Message.ACK_ID) + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        String disconnect = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(disconnect);
    }

    @Test
    public void testQueueBrowerSubscription() throws Exception {

        final int MSG_COUNT = 10;

        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.2\n" +
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

    @Test
    public void testQueueBrowerNotInAutoAckMode() throws Exception {
        String connectFrame = "STOMP\n" +
                              "login:system\n" +
                              "passcode:manager\n" +
                              "accept-version:1.2\n" +
                              "host:localhost\n" +
                              "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);

        String f = stompConnection.receiveFrame();
        LOG.debug("Broker sent: " + f);
        assertTrue(f.startsWith("CONNECTED"));

        String subscribe = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" +
                           "ack:client\n" + "id:12345\n" + "browser:true\n\n" + Stomp.NULL;
        stompConnection.sendFrame(subscribe);

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

        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }
}
