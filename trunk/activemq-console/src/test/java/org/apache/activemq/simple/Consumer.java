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

/**
 * The SimpleQueueSender class consists only of a main method,
 * which sends several messages to a queue.
 *
 * Run this program in conjunction with SimpleQueueReceiver.
 * Specify a queue name on the command line when you run the
 * program.  By default, the program sends one message.  Specify
 * a number after the queue name to send that number of messages.
 */
package org.apache.activemq.simple;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

/**
 *
 */
public final class Consumer {

    /**
     *
     */
    private Consumer() {
    }

    public static void main(String[] args) throws JMSException, InterruptedException {

        String url = "tcp://localhost:61616";
        if (args.length > 0) {
            url = args[0];
        }

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Destination destination = new ActiveMQQueue("TEST.QUEUE");

        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);

        for (;;) {
            System.out.println("Waiting for message.");
            Message message = consumer.receive();
            if (message == null) {
                break;
            }
            System.out.println("Got message: " + message);
        }

        connection.close();
    }
}

// END SNIPPET: demo
