/**
 *
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
package org.apache.activemq.broker.message.security;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.spring.ConsumerBean;

/**
 * 
 */
public class MessageAuthenticationTest extends EmbeddedBrokerTestSupport {

    private Connection connection;

    public void testSendInvalidMessage() throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        ConsumerBean messageList = new ConsumerBean();
        messageList.setVerbose(true);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = new ActiveMQQueue("MyQueue");

        MessageConsumer c1 = session.createConsumer(destination);

        c1.setMessageListener(messageList);

        MessageProducer producer = session.createProducer(destination);
        assertNotNull(producer);

        producer.send(createMessage(session, "invalidBody", "myHeader", "xyz"));
        producer.send(createMessage(session, "validBody", "myHeader", "abc"));

        messageList.assertMessagesArrived(1);
        assertEquals("validBody", ((TextMessage) messageList.flushMessages().get(0)).getText());
    }

    private javax.jms.Message createMessage(Session session, String body, String header, String value) throws JMSException {
        TextMessage msg = session.createTextMessage(body);
        msg.setStringProperty(header, value);
        return msg;
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.setMessageAuthorizationPolicy(new MessageAuthorizationPolicy() {
            public boolean isAllowedToConsume(ConnectionContext context, Message message) {
                try {
                    Object value = message.getProperty("myHeader");
                    return "abc".equals(value);
                }
                catch (IOException e) {
                    System.out.println("Caught: " + e);
                    e.printStackTrace();
                    return false;
                }
            }
        });
        answer.addConnector(bindAddress);
        return answer;
    }

}
