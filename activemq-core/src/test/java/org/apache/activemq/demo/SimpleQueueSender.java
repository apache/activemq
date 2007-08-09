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

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class SimpleQueueSender {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SimpleQueueSender.class);

    /**
     * Main method.
     * 
     * @param args the queue used by the example and, optionally, the number of
     *                messages to send
     */
    public static void main(String[] args) {
        String queueName = null;
        Context jndiContext = null;
        QueueConnectionFactory queueConnectionFactory = null;
        QueueConnection queueConnection = null;
        QueueSession queueSession = null;
        Queue queue = null;
        QueueSender queueSender = null;
        TextMessage message = null;
        final int NUM_MSGS;

        if ((args.length < 1) || (args.length > 2)) {
            log.info("Usage: java SimpleQueueSender " + "<queue-name> [<number-of-messages>]");
            System.exit(1);
        }
        queueName = args[0];
        log.info("Queue name is " + queueName);
        if (args.length == 2) {
            NUM_MSGS = (new Integer(args[1])).intValue();
        } else {
            NUM_MSGS = 1;
        }

        /*
         * Create a JNDI API InitialContext object if none exists yet.
         */
        try {
            jndiContext = new InitialContext();
        } catch (NamingException e) {
            log.info("Could not create JNDI API context: " + e.toString());
            System.exit(1);
        }

        /*
         * Look up connection factory and queue. If either does not exist, exit.
         */
        try {
            queueConnectionFactory = (QueueConnectionFactory)jndiContext.lookup("QueueConnectionFactory");
            queue = (Queue)jndiContext.lookup(queueName);
        } catch (NamingException e) {
            log.info("JNDI API lookup failed: " + e);
            System.exit(1);
        }

        /*
         * Create connection. Create session from connection; false means
         * session is not transacted. Create sender and text message. Send
         * messages, varying text slightly. Send end-of-messages message.
         * Finally, close connection.
         */
        try {
            queueConnection = queueConnectionFactory.createQueueConnection();
            queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            queueSender = queueSession.createSender(queue);
            message = queueSession.createTextMessage();
            for (int i = 0; i < NUM_MSGS; i++) {
                message.setText("This is message " + (i + 1));
                log.info("Sending message: " + message.getText());
                queueSender.send(message);
            }

            /*
             * Send a non-text control message indicating end of messages.
             */
            queueSender.send(queueSession.createMessage());
        } catch (JMSException e) {
            log.info("Exception occurred: " + e.toString());
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

// END SNIPPET: demo
