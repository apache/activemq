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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;

/**
 * A simple tool for consuming messages
 *
 * @version $Revision$
 */
public class ConsumerTool extends ToolSupport implements MessageListener {

    protected int count = 0;
    protected int dumpCount = 10;
    protected boolean verbose = true;
    protected int maxiumMessages = 0;
    private boolean pauseBeforeShutdown;


    public static void main(String[] args) {
        ConsumerTool tool = new ConsumerTool();
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
            tool.maxiumMessages = Integer.parseInt(args[4]);
        }
        tool.run();
    }

    public void run() {
        try {
            System.out.println("Connecting to URL: " + url);
            System.out.println("Consuming " + (topic ? "topic" : "queue") + ": " + subject);
            System.out.println("Using " + (durable ? "durable" : "non-durable") + " subscription");

            Connection connection = createConnection();
            Session session = createSession(connection);
            MessageConsumer consumer = null;
            if (durable && topic) {
                consumer = session.createDurableSubscriber((Topic) destination, consumerName);
            }
            else {
                consumer = session.createConsumer(destination);
            }
            if (maxiumMessages <= 0) {
                consumer.setMessageListener(this);
            }
            connection.start();

            if (maxiumMessages > 0) {
                consumeMessagesAndClose(connection, session, consumer);
            }
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                TextMessage txtMsg = (TextMessage) message;
                if (verbose) {
                	
                	String msg = txtMsg.getText();
                	if( msg.length() > 50 )
                		msg = msg.substring(0, 50)+"...";
                	
                    System.out.println("Received: " + msg);
                }
            }
            else {
                if (verbose) {
                    System.out.println("Received: " + message);
                }
            }
            /*
            if (++count % dumpCount == 0) {
                dumpStats(connection);
            }
            */
        }
        catch (JMSException e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }


    protected void consumeMessagesAndClose(Connection connection, Session session, MessageConsumer consumer) throws JMSException, IOException {
        System.out.println("We are about to wait until we consume: " + maxiumMessages + " message(s) then we will shutdown");

        for (int i = 0; i < maxiumMessages; i++) {
            Message message = consumer.receive();
            onMessage(message);
        }
        System.out.println("Closing connection");
        consumer.close();
        session.close();
        connection.close();
        if (pauseBeforeShutdown) {
            System.out.println("Press return to shut down");
            System.in.read();
        }
    }
}
