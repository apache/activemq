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

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import javax.jms.Connection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stomp12Test extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(Stomp12Test.class);

    protected String bindAddress = "stomp://localhost:61613";
    protected String confUri = "xbean:org/apache/activemq/transport/stomp/stomp-auth-broker.xml";
    protected String jmsUri = "vm://localhost";

    private BrokerService broker;
    private StompConnection stompConnection = new StompConnection();
    private Connection connection;

    @Override
    protected void setUp() throws Exception {

        broker = BrokerFactory.createBroker(new URI(confUri));
        broker.start();
        broker.waitUntilStarted();

        stompConnect();

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(jmsUri);
        connection = cf.createConnection("system", "manager");
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

}
