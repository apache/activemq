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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AMQ4472Test {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4472Test.class);

    @Test
    public void testLostMessage() {
        Connection connection = null;
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.useJmx=false");
            connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination test_data_destination = session.createQueue("test"+System.currentTimeMillis());

            MessageConsumer consumer = session.createConsumer(test_data_destination);
            LOG.info("Consumer 1 connected");

            MessageProducer producer = session.createProducer(test_data_destination);
            producer.send(session.createTextMessage("Message 1"));

            // committing the session prior to the close
            session.commit();

            // starting a new transaction
            producer.send(session.createTextMessage("Message 2"));

            // in a new transaction, with prefetch>0, the message
            // 1 will be pending till second commit
            LOG.info("Closing consumer 1...");
            consumer.close();

            // create a consumer
            consumer = session.createConsumer(test_data_destination);
            LOG.info("Consumer 2 connected");

            // retrieve message previously committed to tmp queue
            Message message = consumer.receive(10000);
            if (message != null) {
                LOG.info("Got message 1:", message);
                assertEquals("expected message", "Message 1", ((TextMessage) message).getText());
                session.commit();
            } else {
                LOG.error("Expected message but it never arrived");
            }
            assertNotNull(message);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (JMSException e) {
            }
        }
    }

}