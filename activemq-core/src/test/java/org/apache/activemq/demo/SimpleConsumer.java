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
 * The SimpleQueueReceiver class consists only of a main method,
 * which fetches one or more messages from a queue using
 * synchronous message delivery.  Run this program in conjunction
 * with SimpleQueueSender.  Specify a queue name on the command
 * line when you run the program.
 */
package org.apache.activemq.demo;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * A simple polymorphic JMS consumer which can work with Queues or Topics which
 * uses JNDI to lookup the JMS connection factory and destination
 * 
 * @version $Revision: 1.2 $
 */
public class SimpleConsumer {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SimpleConsumer.class);

    /**
     * @param args the queue used by the example
     */
    public static void main(String[] args) {
        String destinationName = null;
        Context jndiContext = null;
        ConnectionFactory connectionFactory = null;
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        /*
         * Read destination name from command line and display it.
         */
        if (args.length != 1) {
            log.info("Usage: java SimpleConsumer <destination-name>");
            System.exit(1);
        }
        destinationName = args[0];
        log.info("Destination name is " + destinationName);

        /*
         * Create a JNDI API InitialContext object
         */
        try {
            jndiContext = new InitialContext();
        } catch (NamingException e) {
            log.info("Could not create JNDI API " + "context: " + e.toString());
            System.exit(1);
        }

        /*
         * Look up connection factory and destination.
         */
        try {
            connectionFactory = (ConnectionFactory)jndiContext.lookup("ConnectionFactory");
            destination = (Destination)jndiContext.lookup(destinationName);
        } catch (NamingException e) {
            log.info("JNDI API lookup failed: " + e.toString());
            System.exit(1);
        }

        /*
         * Create connection. Create session from connection; false means
         * session is not transacted. Create receiver, then start message
         * delivery. Receive all text messages from destination until a non-text
         * message is received indicating end of message stream. Close
         * connection.
         */
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumer = session.createConsumer(destination);
            connection.start();
            while (true) {
                Message m = consumer.receive(1);
                if (m != null) {
                    if (m instanceof TextMessage) {
                        TextMessage message = (TextMessage)m;
                        log.info("Reading message: " + message.getText());
                    } else {
                        break;
                    }
                }
            }
        } catch (JMSException e) {
            log.info("Exception occurred: " + e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                }
            }
        }
    }
}
