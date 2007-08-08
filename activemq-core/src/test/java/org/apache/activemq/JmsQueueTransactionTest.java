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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.test.JmsResourceProvider;

/**
 * @version $Revision: 1.2 $
 */
public class JmsQueueTransactionTest extends JmsTransactionTestSupport {
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(JmsQueueTransactionTest.class);

    /**
     * @see org.apache.activemq.JmsTransactionTestSupport#getJmsResourceProvider()
     */
    protected JmsResourceProvider getJmsResourceProvider() {
        JmsResourceProvider p = new JmsResourceProvider();
        p.setTopic(false);
        return p;
    }

    /**
     * Tests if the the connection gets reset, the messages will still be
     * received.
     * 
     * @throws Exception
     */
    public void testReceiveTwoThenCloseConnection() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message")};

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        session.commit();

        producer.send(outbound[0]);
        producer.send(outbound[1]);
        session.commit();

        log.info("Sent 0: " + outbound[0]);
        log.info("Sent 1: " + outbound[1]);

        ArrayList messages = new ArrayList();
        Message message = consumer.receive(1000);
        assertEquals(outbound[0], message);

        message = consumer.receive(1000);
        assertNotNull(message);
        assertEquals(outbound[1], message);

        // Close and reopen connection.
        reconnect();

        // Consume again.. the previous message should
        // get redelivered.
        message = consumer.receive(5000);
        assertNotNull("Should have re-received the first message again!", message);
        messages.add(message);
        assertEquals(outbound[0], message);

        message = consumer.receive(5000);
        assertNotNull("Should have re-received the second message again!", message);
        messages.add(message);
        assertEquals(outbound[1], message);
        session.commit();

        Message inbound[] = new Message[messages.size()];
        messages.toArray(inbound);

        assertTextMessagesEqual("Rollback did not work", outbound, inbound);
    }

    /**
     * Tests sending and receiving messages with two sessions(one for producing
     * and another for consuming).
     * 
     * @throws Exception
     */
    public void testSendReceiveInSeperateSessionTest() throws Exception {
        session.close();
        int batchCount = 10;

        for (int i = 0; i < batchCount; i++) {
            // Session that sends messages
            {
                Session session = resourceProvider.createSession(connection);
                MessageProducer producer = resourceProvider.createProducer(session, destination);
                // consumer = resourceProvider.createConsumer(session,
                // destination);
                producer.send(session.createTextMessage("Test Message: " + i));
                session.commit();
                session.close();
            }

            // Session that consumes messages
            {
                Session session = resourceProvider.createSession(connection);
                MessageConsumer consumer = resourceProvider.createConsumer(session, destination);

                TextMessage message = (TextMessage)consumer.receive(1000 * 5);
                assertNotNull("Received only " + i + " messages in batch ", message);
                assertEquals("Test Message: " + i, message.getText());

                session.commit();
                session.close();
            }
        }
    }

    /**
     * Tests the queue browser. Browses the messages then the consumer tries to
     * receive them. The messages should still be in the queue even when it was
     * browsed.
     * 
     * @throws Exception
     */
    public void testReceiveBrowseReceive() throws Exception {
        Message[] outbound = new Message[] {session.createTextMessage("First Message"), session.createTextMessage("Second Message"), session.createTextMessage("Third Message")};

        // lets consume any outstanding messages from previous test runs
        while (consumer.receive(1000) != null) {
        }
        session.commit();

        producer.send(outbound[0]);
        producer.send(outbound[1]);
        producer.send(outbound[2]);
        session.commit();

        // Get the first.
        assertEquals(outbound[0], consumer.receive(1000));
        consumer.close();

        QueueBrowser browser = session.createBrowser((Queue)destination);
        Enumeration enumeration = browser.getEnumeration();

        // browse the second
        assertTrue("should have received the second message", enumeration.hasMoreElements());
        assertEquals(outbound[1], (Message)enumeration.nextElement());

        // browse the third.
        assertTrue("Should have received the third message", enumeration.hasMoreElements());
        assertEquals(outbound[2], (Message)enumeration.nextElement());

        // There should be no more.
        boolean tooMany = false;
        while (enumeration.hasMoreElements()) {
            log.info("Got extra message: " + ((TextMessage)enumeration.nextElement()).getText());
            tooMany = true;
        }
        assertFalse(tooMany);
        browser.close();

        // Re-open the consumer.
        consumer = resourceProvider.createConsumer(session, destination);
        // Receive the second.
        assertEquals(outbound[1], consumer.receive(1000));
        // Receive the third.
        assertEquals(outbound[2], consumer.receive(1000));
        consumer.close();

        session.commit();
    }

}
