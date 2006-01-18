/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.SocketTimeoutException;


import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.stomp.Stomp;

public class StompTest extends CombinationTestSupport {

    private BrokerService broker;
    private TransportConnector connector;
    private Socket stompSocket;
    private ByteArrayOutputStream inputBuffer;
    private Connection connection;
    private Session session;
    private ActiveMQQueue queue;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        connector = broker.addConnector("stomp://localhost:0");
        broker.start();
        
        URI connectUri = connector.getConnectUri();
        stompSocket = new Socket(connectUri.getHost(), connectUri.getPort());
        inputBuffer = new ByteArrayOutputStream();
        
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        connection = cf.createConnection();
        session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        queue = new ActiveMQQueue("TEST");
        connection.start();


    }
    
    protected void tearDown() throws Exception {
        connection.close();
        stompSocket.close();
        broker.stop();
    }

    public void sendFrame(String data) throws Exception {
        byte[] bytes = data.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        for (int i = 0; i < bytes.length; i++) {
            outputStream.write(bytes[i]);
        }
        outputStream.flush();
    }

    public String receiveFrame(long timeOut) throws Exception {
        stompSocket.setSoTimeout((int) timeOut);
        InputStream is = stompSocket.getInputStream();
        int c=0;
        for(;;) {
            c = is.read();
            if( c < 0 ) {
                throw new IOException("socket closed.");
            } else if( c == 0 ) {
                byte[] ba = inputBuffer.toByteArray();
                inputBuffer.reset();
                return new String(ba, "UTF-8");
            } else {
                inputBuffer.write(c);
            }
        } 
    }

    public void sendMessage(String msg) throws Exception {

        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage(msg);
        producer.send(message);

    }

    public void testConnect() throws Exception {
          
        String connect_frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "\n" + Stomp.NULL;
        sendFrame(connect_frame);
     
        String f = receiveFrame(10000);
        assertTrue(f.startsWith("CONNECTED"));
        
    }
    
    public void testSendMessage() throws Exception {
        
        MessageConsumer consumer = session.createConsumer(queue);
        
        String frame = 
            "CONNECT\n" + 
            "login: brianm\n" + 
            "passcode: wombats\n\n"+
            Stomp.NULL;
        sendFrame(frame);
     
        frame = receiveFrame(10000);
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SEND\n" +
            "destination:/queue/TEST\n\n" +
            "Hello World" +
            Stomp.NULL;

        sendFrame(frame);

        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("Hello World", message.getText());


    }

    public void testSubscribeWithAutoAck() throws Exception {

        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        sendFrame(frame);

        frame = receiveFrame(100000);
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/TEST\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        sendMessage(getName());

        frame = receiveFrame(10000);
        assertTrue(frame.startsWith("MESSAGE"));

       frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
       sendFrame(frame);


    }


    public void testSubscribeWithClientAck() throws Exception {

       String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
       sendFrame(frame);

       frame = receiveFrame(10000);
       assertTrue(frame.startsWith("CONNECTED"));


       frame =
            "SUBSCRIBE\n" +
            "destination:/queue/TEST\n" +
            "ack:client\n\n"+
            Stomp.NULL;


       sendFrame(frame);
       sendMessage(getName());
       frame = receiveFrame(10000);
       assertTrue(frame.startsWith("MESSAGE"));

       frame =
            "DISCONNECT\n" +
            "\n\n"+
            Stomp.NULL;
       sendFrame(frame);

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
        sendFrame(frame);
        frame = receiveFrame(100000);
        assertTrue(frame.startsWith("CONNECTED"));

        frame =
            "SUBSCRIBE\n" +
            "destination:/queue/TEST\n" +
            "ack:auto\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        //send a message to our queue
        sendMessage(getName());


        //receive message from socket
        frame = receiveFrame(10000);
        assertTrue(frame.startsWith("MESSAGE"));

        //remove suscription
        frame =
            "UNSUBSCRIBE\n" +
            "destination:/queue/TEST\n" +
            "\n\n" +
            Stomp.NULL;
                sendFrame(frame);

        //send a message to our queue
        sendMessage(getName());


        try {
            frame = receiveFrame(1000);
            fail("No message should have been received since subscription was removed");
        }catch (SocketTimeoutException e){

        }

    }


    public void testTransactionCommit() throws Exception {
        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        sendFrame(frame);

        String f = receiveFrame(1000);
        assertTrue(f.startsWith("CONNECTED"));

        frame =
            "BEGIN\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/TEST\n" +
            "transaction: tx1\n" +
            "\n\n" +
            "Hello World" +
            Stomp.NULL;
        sendFrame(frame);


        frame =
            "COMMIT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        // This test case is currently failing

        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);


    }

    public void testTransactionRollback() throws Exception {
        String frame =
            "CONNECT\n" +
            "login: brianm\n" +
            "passcode: wombats\n\n"+
            Stomp.NULL;
        sendFrame(frame);

        String f = receiveFrame(1000);
        assertTrue(f.startsWith("CONNECTED"));

        frame =
            "BEGIN\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/TEST\n" +
            "transaction: tx1\n" +
            "\n\n" +
            "first message" +
            Stomp.NULL;
        sendFrame(frame);

        //rollback first message
        frame =
            "ABORT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        frame =
            "SEND\n" +
            "destination:/queue/TEST\n" +
            "transaction: tx1\n" +
            "\n\n" +
            "second message" +
            Stomp.NULL;
        sendFrame(frame);

        frame =
            "COMMIT\n" +
            "transaction: tx1\n" +
            "\n\n" +
            Stomp.NULL;
        sendFrame(frame);

        // This test case is currently failing

        //only second msg should be received since first msg was rolled back
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage message = (TextMessage) consumer.receive(1000);
        assertNotNull(message);
        assertEquals("second message", message.getText());


    }




}
