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
package org.apache.activemq.tool;

import java.util.Date;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A simple tool for publishing messages
 * 
 * 
 */
public class ProducerTool extends ToolSupport {

    protected int messageCount = 10;
    protected long sleepTime;
    protected boolean verbose = true;
    protected int messageSize = 255;

    public static void main(String[] args) {
        runTool(args, new ProducerTool());
    }

    protected static void runTool(String[] args, ProducerTool tool) {
        if (args.length > 0) {
            tool.url = args[0];
        }
        if (args.length > 1) {
            tool.topic = args[1].equalsIgnoreCase("true");
        }
        if (args.length > 2) {
            tool.subject = args[2];
        }
        if (args.length > 3) {
            tool.durable = args[3].equalsIgnoreCase("true");
        }
        if (args.length > 4) {
            tool.messageCount = Integer.parseInt(args[4]);
        }
        if (args.length > 5) {
            tool.messageSize = Integer.parseInt(args[5]);
        }
        tool.run();
    }

    public void run() {
        try {
            System.out.println("Connecting to URL: " + url);
            System.out.println("Publishing a Message with size " + messageSize + " to " + (topic ? "topic" : "queue") + ": " + subject);
            System.out.println("Using " + (durable ? "durable" : "non-durable") + " publishing");

            Connection connection = createConnection();
            Session session = createSession(connection);
            MessageProducer producer = createProducer(session);
            // connection.start();

            sendLoop(session, producer);

            System.out.println("Done.");
            close(connection, session);
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    protected MessageProducer createProducer(Session session) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        if (durable) {
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        } else {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }
        return producer;
    }

    protected void sendLoop(Session session, MessageProducer producer) throws Exception {

        for (int i = 0; i < messageCount; i++) {

            TextMessage message = session.createTextMessage(createMessageText(i));

            if (verbose) {
                String msg = message.getText();
                if (msg.length() > 50) {
                    msg = msg.substring(0, 50) + "...";
                }
                System.out.println("Sending message: " + msg);
            }

            producer.send(message);
            Thread.sleep(sleepTime);
        }
        producer.send(session.createMessage());
    }

    /**
     * @param i
     * @return
     */
    private String createMessageText(int index) {
        StringBuffer buffer = new StringBuffer(messageSize);
        buffer.append("Message: " + index + " sent at: " + new Date());
        if (buffer.length() > messageSize) {
            return buffer.substring(0, messageSize);
        }
        for (int i = buffer.length(); i < messageSize; i++) {
            buffer.append(' ');
        }
        return buffer.toString();
    }
}
