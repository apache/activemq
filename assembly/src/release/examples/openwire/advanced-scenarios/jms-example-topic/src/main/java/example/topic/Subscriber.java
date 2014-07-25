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
package example.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class Subscriber implements MessageListener {
    private static final String BROKER_URL = "tcp://localhost:61616";

    private static final Boolean NON_TRANSACTED = false;


    private final CountDownLatch countDownLatch;
    public Subscriber(CountDownLatch latch) {
        countDownLatch = latch;
    }

    public static void main(String[] args) {
        String url = BROKER_URL;
        if (args.length > 0) {
            url = args[0].trim();
        }
        System.out.println("\nWaiting to receive messages... Either waiting for END message or press Ctrl+C to exit");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "password", url);
        Connection connection = null;
        final CountDownLatch latch = new CountDownLatch(1);

        try {

            connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic("test-topic");

            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(new Subscriber(latch));

            latch.await();
            consumer.close();
            session.close();

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

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                String text = ((TextMessage) message).getText();
                if ("END".equalsIgnoreCase(text)) {
                    System.out.println("Received END message!");
                    countDownLatch.countDown();
                }
                else {
                    System.out.println("Received message:" +text);
                }
            }
        } catch (JMSException e) {
            System.out.println("Got a JMS Exception!");
        }
    }
}
