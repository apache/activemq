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
package org.apache.activemq.demo;

// START SNIPPET: demo

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple queue sender which does not use JNDI
 * 
 * 
 */
public final class DefaultQueueSender {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueueSender.class);

    private DefaultQueueSender() {    
    }

    public static void main(String[] args) {

        String uri = "tcp://localhost:61616";
        String text = "Hello World!";

        Connection connection = null;

        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        int idx = 0;
        String arg = args[0];
        if (arg.equals("-uri")) {
            if (args.length == 1) {
                printUsage();
                System.exit(1);
            }
            uri = args[1];
            idx += 2;
        }
        String queueName = args[idx];
        LOG.info("Connecting to: " + uri);
        LOG.info("Queue name is " + queueName);

        if (++idx < args.length) {
            text = args[idx];
        }

        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(uri);
            connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);

            Message message = session.createTextMessage(text);
            producer.send(message);
        } catch (JMSException e) {
            LOG.info("Exception occurred: " + e.toString());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                }
            }
        }
    }

    protected static void printUsage() {
        System.out.println("Usage: java DefaultQueueSender [-uri <connection-uri>] " + "<queue-name> [<message-body>]");
    }
}

// END SNIPPET: demo
