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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @version $Revision$
 */
public class StompSubscriptionRemoveTest extends TestCase {
    private static final Log log = LogFactory.getLog(StompSubscriptionRemoveTest.class);

    private Socket stompSocket;
    private ByteArrayOutputStream inputBuffer;

    /**
     * @param args
     * @throws Exception
     */
    public void testRemoveSubscriber() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);

        broker.addConnector("stomp://localhost:61613").setName("Stomp");
        broker.addConnector("tcp://localhost:61616").setName("Default");
        broker.start();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(getDestinationName()));
        Message message = session.createTextMessage("Testas");
        for (int idx = 0; idx < 2000; ++idx) {
            producer.send(message);
            log.debug("Sending: " + idx);
        }
        producer.close();
        // consumer.close();
        session.close();
        connection.close();

        stompSocket = new Socket("localhost", 61613);
        inputBuffer = new ByteArrayOutputStream();

        String connect_frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "\n";
        sendFrame(connect_frame);

        String f = receiveFrame(100000);
        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getDestinationName() + "\n" + "ack:client\n\n";
        sendFrame(frame);
        int messagesCount = 0;
        int count = 0;
        while (count < 2) {
            String receiveFrame = receiveFrame(10000);
            DataInput input = new DataInputStream(new ByteArrayInputStream(receiveFrame.getBytes()));
            String line;
            while (true) {
                line = input.readLine();
                if (line == null) {
                    throw new IOException("connection was closed");
                }
                else {
                    line = line.trim();
                    if (line.length() > 0) {
                        break;
                    }
                }
            }
            line = input.readLine();
            if (line == null) {
                throw new IOException("connection was closed");
            }
            String messageId = line.substring(line.indexOf(':') + 1);
            messageId = messageId.trim();
            String ackmessage = "ACK\n" + "message-id:" + messageId + "\n\n";
            sendFrame(ackmessage);
            log.debug(receiveFrame);
            //Thread.sleep(1000);
            ++messagesCount;
            ++count;
        }
        
        sendFrame("DISCONNECT\n\n");
        Thread.sleep(1000);
        stompSocket.close();

        stompSocket = new Socket("localhost", 61613);
        inputBuffer = new ByteArrayOutputStream();

        connect_frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "\n";
        sendFrame(connect_frame);

        f = receiveFrame(5000);
        
        frame = "SUBSCRIBE\n" + "destination:/queue/" + getDestinationName() + "\n" + "ack:client\n\n";
        sendFrame(frame);
        try {
            while (count != 2000) {
                String receiveFrame = receiveFrame(5000);
                DataInput input = new DataInputStream(new ByteArrayInputStream(receiveFrame.getBytes()));
                String line;
                while (true) {
                    line = input.readLine();
                    if (line == null) {
                        throw new IOException("connection was closed");
                    }
                    else {
                        line = line.trim();
                        if (line.length() > 0) {
                            break;
                        }
                    }
                }
                                
                line = input.readLine();
                if (line == null) {
                    throw new IOException("connection was closed");
                }
                String messageId = line.substring(line.indexOf(':') + 1);
                messageId = messageId.trim();
                String ackmessage = "ACK\n" + "message-id:" + messageId + "\n\n";
                sendFrame(ackmessage);
                log.debug("Received: " + receiveFrame);
                //Thread.sleep(1000);
                ++messagesCount;
                ++count;
            }

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
                
        sendFrame("DISCONNECT\n\n");
        stompSocket.close();
        broker.stop();
        
        log.info("Total messages received: " + messagesCount);
        assertTrue("Messages received after connection loss: " + messagesCount, messagesCount >= 2000);

        // The first ack messages has no chance complete, so we receiving more messages

        // Don't know how to list subscriptions for the broker. Currently you
        // can check using JMX console. You'll see
        // Subscription without any connections
    }

    public void sendFrame(String data) throws Exception {
        byte[] bytes = data.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        outputStream.write(bytes);
        outputStream.write(0);
        outputStream.flush();
    }

    public String receiveFrame(long timeOut) throws Exception {
        stompSocket.setSoTimeout((int) timeOut);
        InputStream is = stompSocket.getInputStream();
        int c = 0;
        for (;;) {
            c = is.read();
            if (c < 0) {
                throw new IOException("socket closed.");
            }
            else if (c == 0) {
                c = is.read();
                byte[] ba = inputBuffer.toByteArray();
                inputBuffer.reset();
                return new String(ba, "UTF-8");
            }
            else {
                inputBuffer.write(c);
            }
        }
    }

    protected String getDestinationName() {
        return getClass().getName() + "." + getName();
    }
}
