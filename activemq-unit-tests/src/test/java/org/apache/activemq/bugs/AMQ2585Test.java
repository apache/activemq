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

package org.apache.activemq.bugs;

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.EmbeddedBrokerAndConnectionTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.spring.ConsumerBean;

public class AMQ2585Test extends EmbeddedBrokerAndConnectionTestSupport {
    private final Destination destination = new ActiveMQQueue("MyQueue");
    final static String LENGTH10STRING = "1234567890";
    private Session session;
    private MessageProducer producer;
    private ConsumerBean messageList;

    public void testOneMessageWithProperties() throws Exception {
        TextMessage message = session.createTextMessage(LENGTH10STRING);
        message.setStringProperty(LENGTH10STRING, LENGTH10STRING);
        producer.send(message);

        messageList.assertMessagesArrived(1);

        ActiveMQTextMessage received = ((ActiveMQTextMessage) messageList
                .flushMessages().get(0));

        assertEquals(LENGTH10STRING, received.getText());
        assertTrue(received.getProperties().size() > 0);
        assertTrue(received.propertyExists(LENGTH10STRING));
        assertEquals(LENGTH10STRING, received.getStringProperty(LENGTH10STRING));

        /**
         * As specified by getSize(), the size (memory usage) of the body should
         * be length of text * 2. Unsure of how memory usage is calculated for
         * properties, but should probably not be less than the sum of (string)
         * lengths for the key name and value.
         */

        final int sizeShouldBeNoLessThan = LENGTH10STRING.length() * 4 + Message.DEFAULT_MINIMUM_MESSAGE_SIZE;
        assertTrue("Message size was smaller than expected: " + received.getSize(),
                received.getSize() >= sizeShouldBeNoLessThan);
        assertFalse(LENGTH10STRING.length() * 2 == received.getSize());
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = bindAddress + "?marshal=true";
        super.setUp();
        messageList = new ConsumerBean();
        messageList.setVerbose(true);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer messageConsumer = session.createConsumer(destination);

        messageConsumer.setMessageListener(messageList);

        producer = session.createProducer(destination);
    }
}
