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
package org.apache.activemq.test.rollback;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.MessageCreator;

public class CloseRollbackRedeliveryQueueTest extends EmbeddedBrokerTestSupport {

    private static final transient Log LOG = LogFactory.getLog(CloseRollbackRedeliveryQueueTest.class);

    protected int numberOfMessagesOnQueue = 1;
    private Connection connection;
   
    public void testVerifySessionCloseRedeliveryWithFailoverTransport() throws Throwable {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(destination);

        Message message = consumer.receive(1000);
        String id = message.getJMSMessageID();
        assertNotNull(message);
        LOG.info("got message " + message);
        // close will rollback the current tx
        session.close();
        
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = session.createConsumer(destination);

        message = consumer.receive(1000);
        session.commit();
        assertNotNull(message);
        assertEquals("redelivered message", id, message.getJMSMessageID());
        assertEquals(3, message.getLongProperty("JMSXDeliveryCount"));
    }
    
    public void testVerifyConsumerAndSessionCloseRedeliveryWithFailoverTransport() throws Throwable {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(destination);

        Message message = consumer.receive(1000);
        String id = message.getJMSMessageID();
        assertNotNull(message);
        LOG.info("got message " + message);
        consumer.close();
        session.close();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = session.createConsumer(destination);

        message = consumer.receive(1000);
        session.commit();
        assertNotNull(message);
        assertEquals("redelivered message", id, message.getJMSMessageID());
        assertEquals(3, message.getLongProperty("JMSXDeliveryCount"));
    }

    public void testVerifyConsumerCloseSessionRollbackRedeliveryWithFailoverTransport() throws Throwable {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(destination);

        Message message = consumer.receive(1000);
        String id = message.getJMSMessageID();
        assertNotNull(message);
        LOG.info("got message " + message);
        consumer.close();
        session.rollback();
        
        consumer = session.createConsumer(destination);
        message = consumer.receive(1000);
        session.commit();
        assertNotNull(message);
        assertEquals("redelivered message", id, message.getJMSMessageID());
        assertEquals(3, message.getLongProperty("JMSXDeliveryCount"));
    }
    
    protected void setUp() throws Exception {
        super.setUp();

        connection = createConnection();
        connection.start();

        // lets fill the queue up
        for (int i = 0; i < numberOfMessagesOnQueue; i++) {
            template.send(createMessageCreator(i));
        }

    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        // failover: enables message audit - which could get in the way of redelivery 
        return new ActiveMQConnectionFactory("failover:" + bindAddress);
    }
    
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected MessageCreator createMessageCreator(final int i) {
        return new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                TextMessage answer = session.createTextMessage("Message: " + i);
                answer.setIntProperty("Counter", i);
                return answer;
            }
        };
    }
}
