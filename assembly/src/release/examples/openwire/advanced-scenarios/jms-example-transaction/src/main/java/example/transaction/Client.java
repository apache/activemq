/**
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
package example.transaction;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

import jakarta.jms.*;
import java.util.Scanner;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class Client {
    private static final String BROKER_URL = "tcp://localhost:61616";

    // this is set to true
    private static final Boolean TRANSACTED = true;
    private static final Boolean NON_TRANSACTED = false;

    public static void main(String[] args) {
        String url = BROKER_URL;
        if (args.length > 0) {
            url = args[0].trim();
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "password", url);
        Connection connection = null;

        try {
            connection = connectionFactory.createConnection();
            connection.start();
            Topic destination = new ActiveMQTopic("transacted.client.example");

            Session senderSession = connection.createSession(TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            Session receiverSession = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer receiver = receiverSession.createConsumer(destination);
            receiver.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    if (message instanceof TextMessage) {
                        try {
                            String value = ((TextMessage) message).getText();
                            System.out.println("We received a new message: " + value);
                        } catch (JMSException e) {
                            System.out.println("Could not read the receiver's topic because of a JMSException");
                        }
                    }
                }
            });

            MessageProducer sender = senderSession.createProducer(destination);


            connection.start();
            acceptInputFromUser(senderSession, sender);
            senderSession.close();
            receiverSession.close();

        } catch (Exception e) {
            System.out.println("Caught exception!");
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    System.out.println("Could not close an open connection...");
                }
            }
        }
    }

    private static void acceptInputFromUser(Session senderSession, MessageProducer sender) throws JMSException {
        System.out.println("Type a message. Type COMMIT to send to receiver, type ROLLBACK to cancel");
        Scanner inputReader = new Scanner(System.in);

        while (true) {
            String line = inputReader.nextLine();
            if (line == null) {
                System.out.println("Done!");
                break;
            } else if (line.length() > 0) {
                if (line.trim().equals("ROLLBACK")) {
                    System.out.println("Rolling back...");
                    senderSession.rollback();
                    System.out.println("Messages have been rolledback");
                } else if (line.trim().equals("COMMIT")) {
                    System.out.println("Committing... ");
                    senderSession.commit();
                    System.out.println("Messages should have been sent");
                } else {
                    TextMessage message = senderSession.createTextMessage();
                    message.setText(line);
                    System.out.println("Batching up:'" + message.getText() + "'");
                    sender.send(message);
                }
            }
        }
    }
}
