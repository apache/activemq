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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompSubscriptionRemoveTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompSubscriptionRemoveTest.class);
    private static final String COMMAND_MESSAGE = "MESSAGE";
    private static final String HEADER_MESSAGE_ID = "message-id";

    @Test(timeout = 60000)
    public void testRemoveSubscriber() throws Exception {
        stompConnect();
        Connection connection = cf.createConnection("system", "manager");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(getQueueName()));
        Message message = session.createTextMessage("Testas");
        for (int idx = 0; idx < 2000; ++idx) {
            producer.send(message);
            LOG.debug("Sending: " + idx);
        }
        producer.close();
        session.close();
        connection.close();

        String connectFrame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        stompConnection.receiveFrame();
        String frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        int messagesCount = 0;
        int count = 0;
        while (count < 2) {
            String receiveFrame = stompConnection.receiveFrame();
            LOG.debug("Received: " + receiveFrame);
            assertEquals("Unexpected frame received", COMMAND_MESSAGE, getCommand(receiveFrame));
            String messageId = getHeaderValue(receiveFrame, HEADER_MESSAGE_ID);
            String ackmessage = "ACK\n" + HEADER_MESSAGE_ID + ":" + messageId + "\n\n"+ Stomp.NULL;
            stompConnection.sendFrame(ackmessage);
            // Thread.sleep(1000);
            ++messagesCount;
            ++count;
        }

        stompDisconnect();
        Thread.sleep(1000);
        stompConnect();

        connectFrame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(connectFrame);

        stompConnection.receiveFrame();

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        try {
            while (count != 2000) {
                String receiveFrame = stompConnection.receiveFrame();
                LOG.debug("Received: " + receiveFrame);
                assertEquals("Unexpected frame received", COMMAND_MESSAGE, getCommand(receiveFrame));
                String messageId = getHeaderValue(receiveFrame, HEADER_MESSAGE_ID);
                String ackmessage = "ACK\n" + HEADER_MESSAGE_ID + ":" + messageId.trim() + "\n\n" + Stomp.NULL;
                stompConnection.sendFrame(ackmessage);
                // Thread.sleep(1000);
                ++messagesCount;
                ++count;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        stompConnection.sendFrame("DISCONNECT\n\n");
        stompConnection.close();

        LOG.info("Total messages received: " + messagesCount);
        assertTrue("Messages received after connection loss: " + messagesCount, messagesCount >= 2000);

        // The first ack messages has no chance complete, so we receiving more
        // messages

        // Don't know how to list subscriptions for the broker. Currently you
        // can check using JMX console. You'll see
        // Subscription without any connections
    }

    // These two methods could move to a utility class
    protected String getCommand(String frame) {
        return frame.substring(0, frame.indexOf('\n') + 1).trim();
    }

    protected String getHeaderValue(String frame, String header) throws IOException {
        DataInput input = new DataInputStream(new ByteArrayInputStream(frame.getBytes()));
        String line;
        for (int idx = 0; /* forever, sort of */; ++idx) {
            line = input.readLine();
            if (line == null) {
                // end of message, no headers
                return null;
            }
            line = line.trim();
            if (line.length() == 0) {
                // start body, no headers from here on
                return null;
            }
            if (idx > 0) { // Ignore command line
                int pos = line.indexOf(':');
                if (header.equals(line.substring(0, pos))) {
                    return line.substring(pos + 1).trim();
                }
            }
        }
    }
}
