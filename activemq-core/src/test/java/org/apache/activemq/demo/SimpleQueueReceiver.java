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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class SimpleQueueReceiver {

    private static final Log LOG = LogFactory.getLog(SimpleQueueReceiver.class);

    private SimpleQueueReceiver() {
    }

    /**
     * Main method.
     * 
     * @param args the queue used by the example
     */
    public static void main(String[] args) {
        String queueName = null;
        Context jndiContext = null;
        QueueConnectionFactory queueConnectionFactory = null;
        QueueConnection queueConnection = null;
        QueueSession queueSession = null;
        Queue queue = null;
        QueueReceiver queueReceiver = null;
        TextMessage message = null;

        /*
         * Read queue name from command line and display it.
         */
        if (args.length != 1) {
            LOG.info("Usage: java " + "SimpleQueueReceiver <queue-name>");
            System.exit(1);
        }
        queueName = args[0];
        LOG.info("Queue name is " + queueName);

        /*
         * Create a JNDI API InitialContext object if none exists yet.
         */
        try {
            jndiContext = new InitialContext();
        } catch (NamingException e) {
            LOG.info("Could not create JNDI API " + "context: " + e.toString());
            System.exit(1);
        }

        /*
         * Look up connection factory and queue. If either does not exist, exit.
         */
        try {
            queueConnectionFactory = (QueueConnectionFactory)jndiContext.lookup("QueueConnectionFactory");
            queue = (Queue)jndiContext.lookup(queueName);
        } catch (NamingException e) {
            LOG.info("JNDI API lookup failed: " + e.toString());
            System.exit(1);
        }

        /*
         * Create connection. Create session from connection; false means
         * session is not transacted. Create receiver, then start message
         * delivery. Receive all text messages from queue until a non-text
         * message is received indicating end of message stream. Close
         * connection.
         */
        try {
            queueConnection = queueConnectionFactory.createQueueConnection();
            queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            queueReceiver = queueSession.createReceiver(queue);
            queueConnection.start();
            while (true) {
                Message m = queueReceiver.receive(1);
                if (m != null) {
                    if (m instanceof TextMessage) {
                        message = (TextMessage)m;
                        LOG.info("Reading message: " + message.getText());
                    } else {
                        break;
                    }
                }
            }
        } catch (JMSException e) {
            LOG.info("Exception occurred: " + e.toString());
        } finally {
            if (queueConnection != null) {
                try {
                    queueConnection.close();
                } catch (JMSException e) {
                }
            }
        }
    }
}
