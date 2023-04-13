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
package example.wildcard;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

import jakarta.jms.*;
import java.util.Scanner;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class Client {
    private static final Boolean NON_TRANSACTED = false;
    private static final String BROKER_URL = "tcp://localhost:61616";

    public static void main(String[] args) {
        String url = BROKER_URL;
        if (args.length > 0) {
            url = args[0].trim();
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "password", url);
        Connection connection = null;

        try {
            Topic senderTopic = new ActiveMQTopic(System.getProperty("topicName"));

            connection = connectionFactory.createConnection("admin", "password");

            Session senderSession = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            MessageProducer sender = senderSession.createProducer(senderTopic);

            Session receiverSession = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);

            String policyType = System.getProperty("wildcard", ".*");
            String receiverTopicName = senderTopic.getTopicName() + policyType;
            Topic receiverTopic = receiverSession.createTopic(receiverTopicName);

            MessageConsumer receiver = receiverSession.createConsumer(receiverTopic);
            receiver.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        if (message instanceof TextMessage) {
                            String text = ((TextMessage) message).getText();
                            System.out.println("We received a new message: " + text);
                        }
                    } catch (JMSException e) {
                        System.out.println("Could not read the receiver's topic because of a JMSException");
                    }
                }
            });

            connection.start();
            System.out.println("Listening on '" + receiverTopicName + "'");
            System.out.println("Enter a message to send: ");

            Scanner inputReader = new Scanner(System.in);

            while (true) {
                String line = inputReader.nextLine();
                if (line == null) {
                    System.out.println("Done!");
                    break;
                } else if (line.length() > 0) {
                    try {
                        TextMessage message = senderSession.createTextMessage();
                        message.setText(line);
                        System.out.println("Sending a message: " + message.getText());
                        sender.send(message);
                    } catch (JMSException e) {
                        System.out.println("Exception during publishing a message: ");
                    }
                }
            }

            receiver.close();
            receiverSession.close();
            sender.close();
            senderSession.close();

        } catch (Exception e) {
            System.out.println("Caught exception!");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    System.out.println("When trying to close connection: ");
                }
            }
        }

    }
}
