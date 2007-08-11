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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class Producer {

    private static final Log LOG = LogFactory.getLog(Producer.class);

    private Producer() {
    }

    public static void main(String[] args) throws JMSException, InterruptedException {

        String url = "peer://localhost1/groupA?persistent=false";
        if (args.length > 0) {
            url = args[0];
        }

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Destination destination = new ActiveMQQueue("TEST.QUEUE");

        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage();
        for (int i = 0; i < 1000; i++) {
            message.setText("This is message " + (i + 1));
            LOG.info("Sending message: " + message.getText());
            producer.send(message);
            Thread.sleep(1000);
        }
        connection.close();

    }
}

// END SNIPPET: demo
