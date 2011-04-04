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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

/**
 * 
 */
public class JmsQueueBrowserTest extends JmsTestSupport {
    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
            .getLog(JmsQueueBrowserTest.class);
    

    /**
     * Tests the queue browser. Browses the messages then the consumer tries to receive them. The messages should still
     * be in the queue even when it was browsed.
     *
     * @throws Exception
     */
    public void testReceiveBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        Message[] outbound = new Message[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }

        producer.send(outbound[0]);
        producer.send(outbound[1]);
        producer.send(outbound[2]);

        // Get the first.
        assertEquals(outbound[0], consumer.receive(1000));
        consumer.close();
        //Thread.sleep(200);

        QueueBrowser browser = session.createBrowser((Queue) destination);
        Enumeration enumeration = browser.getEnumeration();

        // browse the second
        assertTrue("should have received the second message", enumeration.hasMoreElements());
        assertEquals(outbound[1], (Message) enumeration.nextElement());

        // browse the third.
        assertTrue("Should have received the third message", enumeration.hasMoreElements());
        assertEquals(outbound[2], (Message) enumeration.nextElement());

        // There should be no more.
        boolean tooMany = false;
        while (enumeration.hasMoreElements()) {
            LOG.info("Got extra message: " + ((TextMessage) enumeration.nextElement()).getText());
            tooMany = true;
        }
        assertFalse(tooMany);
        browser.close();

        // Re-open the consumer.
        consumer = session.createConsumer(destination);
        // Receive the second.
        assertEquals(outbound[1], consumer.receive(1000));
        // Receive the third.
        assertEquals(outbound[2], consumer.receive(1000));
        consumer.close();

    }
    
    public void testBrowseReceive() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
       
        connection.start();

        Message[] outbound = new Message[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};

        
        MessageProducer producer = session.createProducer(destination);
        producer.send(outbound[0]);
        
        // create browser first
        QueueBrowser browser = session.createBrowser((Queue) destination);
        Enumeration enumeration = browser.getEnumeration();
        
        // create consumer
        MessageConsumer consumer = session.createConsumer(destination);
        
        // browse the first message
        assertTrue("should have received the first message", enumeration.hasMoreElements());
        assertEquals(outbound[0], (Message) enumeration.nextElement());
        
        // Receive the first message.
        assertEquals(outbound[0], consumer.receive(1000));
        consumer.close();
        browser.close();
        producer.close();

    }
    
    public void testQueueBrowserWith2Consumers() throws Exception {
        final int numMessages = 1000;
        connection.setAlwaysSyncSend(false);
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");
        ActiveMQQueue destinationPrefetch10 = new ActiveMQQueue("TEST?jms.prefetchSize=10");
        ActiveMQQueue destinationPrefetch1 = new ActiveMQQueue("TEST?jms.prefetchsize=1");      
        connection.start();

        ActiveMQConnection connection2 = (ActiveMQConnection)factory.createConnection(userName, password);
        connection2.start();
        connections.add(connection2);
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destinationPrefetch10);
  
        for (int i=0; i<numMessages; i++) {
            TextMessage message = session.createTextMessage("Message: " + i);
            producer.send(message);   
        }
        
        QueueBrowser browser = session2.createBrowser(destinationPrefetch1);
        Enumeration<Message> browserView = browser.getEnumeration();
    
        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < numMessages; i++) {
            Message m1 = consumer.receive(5000);
            assertNotNull("m1 is null for index: " + i, m1);
            messages.add(m1);
        }

        int i = 0;
        for (; i < numMessages && browserView.hasMoreElements(); i++) {
            Message m1 = messages.get(i);
            Message m2 = browserView.nextElement();
            assertNotNull("m2 is null for index: " + i, m2);
            assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
        }
        
        // currently browse max page size is ignored for a queue browser consumer
        // only guarantee is a page size - but a snapshot of pagedinpending is
        // used so it is most likely more
        assertTrue("got at least our expected minimum in the browser: ", i > BaseDestination.MAX_PAGE_SIZE);

        assertFalse("nothing left in the browser", browserView.hasMoreElements());
        assertNull("consumer finished", consumer.receiveNoWait());
    }

    public void testBrowseClose() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue destination = new ActiveMQQueue("TEST");

        connection.start();

        TextMessage[] outbound = new TextMessage[]{session.createTextMessage("First Message"),
                                           session.createTextMessage("Second Message"),
                                           session.createTextMessage("Third Message")};


        MessageProducer producer = session.createProducer(destination);
        producer.send(outbound[0]);
        producer.send(outbound[1]);
        producer.send(outbound[2]);


        // create browser first
        QueueBrowser browser = session.createBrowser((Queue) destination);
        Enumeration enumeration = browser.getEnumeration();


        // browse some messages
        assertEquals(outbound[0], (Message) enumeration.nextElement());
        assertEquals(outbound[1], (Message) enumeration.nextElement());
        //assertEquals(outbound[2], (Message) enumeration.nextElement());


        browser.close();

        // create consumer
        MessageConsumer consumer = session.createConsumer(destination);

        // Receive the first message.
        TextMessage msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[0].getText() + " but received " + msg.getText(),  outbound[0], msg);
        msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[1].getText() + " but received " + msg.getText(), outbound[1], msg);
        msg = (TextMessage)consumer.receive(1000);
        assertEquals("Expected " + outbound[2].getText() + " but received " + msg.getText(), outbound[2], msg);

        consumer.close();
        producer.close();

    }
}
