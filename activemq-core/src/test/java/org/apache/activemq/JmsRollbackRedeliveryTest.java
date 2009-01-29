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

package org.apache.activemq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JmsRollbackRedeliveryTest extends AutoFailTestSupport {
    protected static final Log LOG = LogFactory.getLog(JmsRollbackRedeliveryTest.class);
    final int nbMessages = 10;
    final String destinationName = "Destination";
    boolean consumerClose = true;
    boolean rollback = true;
    
    public void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
    }
    

    public void testRedelivery() throws Exception {
        doTestRedelivery("vm://localhost", false);
    }

    public void testRedeliveryWithInterleavedProducer() throws Exception {
        doTestRedelivery("vm://localhost", true);
    }

    public void doTestRedelivery(String brokerUrl, boolean interleaveProducer) throws Exception {

        final int nbMessages = 10;
        final String destinationName = "Destination";

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        
        Connection connection = connectionFactory.createConnection();
        connection.start();

        if (interleaveProducer) {
            populateDestinationWithInterleavedProducer(nbMessages, destinationName, connection);
        } else {
            populateDestination(nbMessages, destinationName, connection);
        }
        
        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            while (received.get() < nbMessages) {
                Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(destinationName);
                MessageConsumer consumer = session.createConsumer(destination);
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " (" + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        session.rollback();
                    }
                }
                consumer.close();
                session.close();
            }
        }
    }
       
    public void testRedeliveryOnSingleConsumer() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        populateDestinationWithInterleavedProducer(nbMessages, destinationName, connection);

        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(destinationName);
            MessageConsumer consumer = session.createConsumer(destination);            
            while (received.get() < nbMessages) {
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " (" + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        session.rollback();
                    }
                }
            }
            consumer.close();
            session.close();
        }
    }
    
    public void testRedeliveryOnSingleSession() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost"); 
        Connection connection = connectionFactory.createConnection();
        connection.start();

        populateDestination(nbMessages, destinationName, connection);

        // Consume messages and rollback transactions
        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(destinationName);
            while (received.get() < nbMessages) {
                MessageConsumer consumer = session.createConsumer(destination);            
                TextMessage msg = (TextMessage) consumer.receive(6000000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " (" + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    } else {
                        LOG.info("Rollback message " + msg.getText() + " id: " +  msg.getJMSMessageID());
                        session.rollback();
                    }
                }
                consumer.close();
            }
            session.close();
        }
    }
    
    public void testRedeliveryOnSessionCloseWithNoRollback() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        populateDestination(nbMessages, destinationName, connection);

        {
            AtomicInteger received = new AtomicInteger();
            Map<String, Boolean> rolledback = new ConcurrentHashMap<String, Boolean>();
            while (received.get() < nbMessages) {
                Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(destinationName);

                MessageConsumer consumer = session.createConsumer(destination);            
                TextMessage msg = (TextMessage) consumer.receive(1000);
                if (msg != null) {
                    if (msg != null && rolledback.put(msg.getText(), Boolean.TRUE) != null) {
                        LOG.info("Received message " + msg.getText() + " (" + received.getAndIncrement() + ")" + msg.getJMSMessageID());
                        assertTrue(msg.getJMSRedelivered());
                        session.commit();
                    }
                }
                session.close();
            }
        }
    }
    
    private void populateDestination(final int nbMessages,
            final String destinationName, Connection connection)
            throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(destinationName);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }

    
    private void populateDestinationWithInterleavedProducer(final int nbMessages,
            final String destinationName, Connection connection)
            throws JMSException {
        Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination1 = session1.createQueue(destinationName);
        MessageProducer producer1 = session1.createProducer(destination1);
        Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination2 = session2.createQueue(destinationName);
        MessageProducer producer2 = session2.createProducer(destination2);
        
        for (int i = 1; i <= nbMessages; i++) {
            if (i%2 == 0) {
                producer1.send(session1.createTextMessage("<hello id='" + i + "'/>"));
            } else {
                producer2.send(session2.createTextMessage("<hello id='" + i + "'/>"));
            }
        }
        producer1.close();
        session1.close();
        producer2.close();
        session2.close();
    }

}
