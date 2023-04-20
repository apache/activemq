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
package org.apache.activemq.jakarta;

import static org.junit.Assert.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

public class JakartaTest {

    @Ignore // NOTE: Remove @Ignore to test manually with local running ActiveMQ broker until we have a Jakarta-supported broker
    @Test
    public void testJakartaConnection() throws JMSException {
        ActiveMQConnectionFactory activemqConnectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:61616");
        Connection connection = activemqConnectionFactory.createConnection();
        assertTrue(jakarta.jms.Connection.class.isAssignableFrom(connection.getClass()));
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertTrue(jakarta.jms.Session.class.isAssignableFrom(session.getClass()));

        Destination queue = session.createQueue("JAKARTA.TEST");
        assertTrue(jakarta.jms.Destination.class.isAssignableFrom(queue.getClass()));

        MessageProducer messageProducer = session.createProducer(queue);
        assertTrue(jakarta.jms.MessageProducer.class.isAssignableFrom(messageProducer.getClass()));

        String messageText = "Test Jakarta API";
        TextMessage sendMessage = session.createTextMessage(messageText);
        assertTrue(jakarta.jms.Message.class.isAssignableFrom(sendMessage.getClass()));
        assertTrue(jakarta.jms.TextMessage.class.isAssignableFrom(sendMessage.getClass()));

        messageProducer.send(sendMessage);

        MessageConsumer messageConsumer = session.createConsumer(queue);
        assertTrue(jakarta.jms.MessageConsumer.class.isAssignableFrom(messageConsumer.getClass()));

        Message recvMessage = messageConsumer.receive(5000l);
        assertNotNull(recvMessage);
        assertTrue(jakarta.jms.Message.class.isAssignableFrom(sendMessage.getClass()));
        assertTrue(jakarta.jms.TextMessage.class.isAssignableFrom(sendMessage.getClass()));
        assertEquals(messageText, TextMessage.class.cast(recvMessage).getText());

        if(messageConsumer != null) {
            try { messageConsumer.close(); } catch (JMSException e) { }
        }

        if(messageProducer != null) {
            try { messageProducer.close(); } catch (JMSException e) { }
        }

        if(session != null) {
            try { session.close(); } catch (JMSException e) { }
        }

        if(connection != null) {
            try { connection.close(); } catch (JMSException e) { }
        }
    }

}
