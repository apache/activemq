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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple polymorphic JMS producer which can work with Queues or Topics which
 * uses JNDI to lookup the JMS connection factory and destination
 * 
 * @version $Revision: 1.2 $
 */
public final class SimpleProducer {

    private static final Log LOG = LogFactory.getLog(SimpleProducer.class);

    private SimpleProducer() {
    }

    /**
     * @param args the destination name to send to and optionally, the number of
     *                messages to send
     */
    public static void main(String[] args) {
        Context jndiContext = null;
        ConnectionFactory connectionFactory = null;
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageProducer producer = null;
        String destinationName = null;
        final int numMsgs;

        if ((args.length < 1) || (args.length > 2)) {
            LOG.info("Usage: java SimpleProducer <destination-name> [<number-of-messages>]");
            System.exit(1);
        }
        destinationName = args[0];
        LOG.info("Destination name is " + destinationName);
        if (args.length == 2) {
            numMsgs = (new Integer(args[1])).intValue();
        } else {
            numMsgs = 1;
        }

        /*
         * Create a JNDI API InitialContext object
         */
        try {
            jndiContext = new InitialContext();
        } catch (NamingException e) {
            LOG.info("Could not create JNDI API context: " + e.toString());
            System.exit(1);
        }

        /*
         * Look up connection factory and destination.
         */
        try {
            connectionFactory = (ConnectionFactory)jndiContext.lookup("ConnectionFactory");
            destination = (Destination)jndiContext.lookup(destinationName);
        } catch (NamingException e) {
            LOG.info("JNDI API lookup failed: " + e);
            System.exit(1);
        }

        /*
         * Create connection. Create session from connection; false means
         * session is not transacted. Create sender and text message. Send
         * messages, varying text slightly. Send end-of-messages message.
         * Finally, close connection.
         */
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            for (int i = 0; i < numMsgs; i++) {
                message.setText("This is message " + (i + 1));
                LOG.info("Sending message: " + message.getText());
                producer.send(message);
            }

            /*
             * Send a non-text control message indicating end of messages.
             */
            producer.send(session.createMessage());
        } catch (JMSException e) {
            LOG.info("Exception occurred: " + e);
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

// END SNIPPET: demo
