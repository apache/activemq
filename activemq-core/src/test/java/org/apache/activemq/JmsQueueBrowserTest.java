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

import java.util.Enumeration;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQQueue;

/**
 * @version $Revision: 1.4 $
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
        Session session = connection.createSession(false, 0);
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
}
