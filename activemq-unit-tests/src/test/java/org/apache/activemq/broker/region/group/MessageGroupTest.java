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
package org.apache.activemq.broker.region.group;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageGroupTest extends JmsTestSupport {
	
	 private static final Logger LOG = LoggerFactory.getLogger(CombinationTestSupport.class);

    public void testGroupedMessagesDeliveredToOnlyOneConsumer() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the messages.
        for (int i = 0; i < 4; i++) {     	
        	TextMessage message = session.createTextMessage("message " + i);
            message.setStringProperty("JMSXGroupID", "TEST-GROUP");
            message.setIntProperty("JMSXGroupSeq", i + 1);
            LOG.info("sending message: " + message);
            producer.send(message);
        }

        // All the messages should have been sent down connection 1.. just get
        // the first 3
        for (int i = 0; i < 3; i++) {
            TextMessage m1 = (TextMessage)consumer1.receive(500);
            assertNotNull("m1 is null for index: " + i, m1);
            assertEquals(m1.getIntProperty("JMSXGroupSeq"), i + 1);
        }
        
        // Setup a second connection
        Connection connection1 = factory.createConnection(userName, password);
        connection1.start();
        Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(destination);

        // Close the first consumer.
        consumer1.close();

        // The last messages should now go the the second consumer.
        for (int i = 0; i < 1; i++) {
            TextMessage m1 = (TextMessage)consumer2.receive(500);
            assertNotNull("m1 is null for index: " + i, m1);
            assertEquals(m1.getIntProperty("JMSXGroupSeq"), 4 + i);
        }

        //assert that there are no other messages left for the consumer 2
        Message m = consumer2.receive(100);
        assertNull("consumer 2 has some messages left", m);
    }	
    
    public void testAddingConsumer() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        MessageProducer producer = session.createProducer(destination);
        //MessageConsumer consumer = session.createConsumer(destination);
        
    	TextMessage message = session.createTextMessage("message");
        message.setStringProperty("JMSXGroupID", "TEST-GROUP");
        
        LOG.info("sending message: " + message);
        producer.send(message);
        
        MessageConsumer consumer = session.createConsumer(destination);
        
        TextMessage msg = (TextMessage)consumer.receive();
        assertNotNull(msg);
        boolean first = msg.getBooleanProperty("JMSXGroupFirstForConsumer");
        assertTrue(first);
    }    
    
    public void testClosingMessageGroup() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        // Send the messages.
        for (int i = 0; i < 4; i++) {     	
        	TextMessage message = session.createTextMessage("message " + i);
            message.setStringProperty("JMSXGroupID", "TEST-GROUP");
            LOG.info("sending message: " + message);
            producer.send(message);
        }



        // All the messages should have been sent down consumer1.. just get
        // the first 3
        for (int i = 0; i < 3; i++) {
            TextMessage m1 = (TextMessage)consumer1.receive(500);
            assertNotNull("m1 is null for index: " + i, m1);
        }
        
        // Setup a second consumer
        Connection connection1 = factory.createConnection(userName, password);
        connection1.start();
        Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(destination);
        
        //assert that there are no messages for the consumer 2
        Message m = consumer2.receive(100);
        assertNull("consumer 2 has some messages", m);

        // Close the group
    	TextMessage message = session.createTextMessage("message " + 5);
        message.setStringProperty("JMSXGroupID", "TEST-GROUP");
        message.setIntProperty("JMSXGroupSeq", -1);
        LOG.info("sending message: " + message);
        producer.send(message);
        
        //Send some more messages
        for (int i = 0; i < 4; i++) {     	
        	message = session.createTextMessage("message " + i);
            message.setStringProperty("JMSXGroupID", "TEST-GROUP");
            LOG.info("sending message: " + message);
            producer.send(message);
        }
        
        // Receive the fourth message
        TextMessage m1 = (TextMessage)consumer1.receive(500);
        assertNotNull("m1 is null for index: " + 4, m1);
        
        // Receive the closing message
        m1 = (TextMessage)consumer1.receive(500);
        assertNotNull("m1 is null for index: " + 5, m1);        
        
        //assert that there are no messages for the consumer 1
        m = consumer1.receive(100);
        assertNull("consumer 1 has some messages left", m);

        // The messages should now go to the second consumer.
        for (int i = 0; i < 4; i++) {
            m1 = (TextMessage)consumer2.receive(500);
            assertNotNull("m1 is null for index: " + i, m1);
        }

    }
	
}
