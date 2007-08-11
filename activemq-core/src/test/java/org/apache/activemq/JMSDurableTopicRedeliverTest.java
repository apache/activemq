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

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.4 $
 */
public class JMSDurableTopicRedeliverTest extends JmsTopicRedeliverTest {

    private static final Log LOG = LogFactory.getLog(JMSDurableTopicRedeliverTest.class);

    protected void setUp() throws Exception {
        durable = true;
        super.setUp();
    }

    /**
     * Sends and consumes the messages.
     * 
     * @throws Exception
     */
    public void testRedeliverNewSession() throws Exception {
        String text = "TEST: " + System.currentTimeMillis();
        Message sendMessage = session.createTextMessage(text);

        if (verbose) {
            LOG.info("About to send a message: " + sendMessage + " with text: " + text);
        }
        producer.send(producerDestination, sendMessage);

        // receive but don't acknowledge
        Message unackMessage = consumer.receive(1000);
        assertNotNull(unackMessage);
        String unackId = unackMessage.getJMSMessageID();
        assertEquals(((TextMessage)unackMessage).getText(), text);
        assertFalse(unackMessage.getJMSRedelivered());
        assertEquals(unackMessage.getIntProperty("JMSXDeliveryCount"), 1);
        consumeSession.close();
        consumer.close();

        // receive then acknowledge
        consumeSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = createConsumer();
        Message ackMessage = consumer.receive(1000);
        assertNotNull(ackMessage);
        ackMessage.acknowledge();
        String ackId = ackMessage.getJMSMessageID();
        assertEquals(((TextMessage)ackMessage).getText(), text);
        assertTrue(ackMessage.getJMSRedelivered());
        assertEquals(ackMessage.getIntProperty("JMSXDeliveryCount"), 2);
        assertEquals(unackId, ackId);
        consumeSession.close();
        consumer.close();

        consumeSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer = createConsumer();
        assertNull(consumer.receive(1000));
    }
}
