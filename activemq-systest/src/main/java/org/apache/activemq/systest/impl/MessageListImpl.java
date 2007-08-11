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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.MessageList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

/**
 * A simple in-JVM implementation of a {@link MessageList}
 * 
 * @version $Revision: 1.1 $
 */
public class MessageListImpl extends Assert implements MessageList {

    private int numberOfMessages;
    private int charactersPerMessage;
    private List payloads;
    private String customHeader = "testHeader";
    private boolean useTextMessage = true;
    private int sentCounter;

    public MessageListImpl(int numberOfMessages, int charactersPerMessage) {
        this.numberOfMessages = numberOfMessages;
        this.charactersPerMessage = charactersPerMessage;
    }

    public void start() throws Exception {
        payloads = new ArrayList(numberOfMessages);
        for (int i = 0; i < numberOfMessages; i++) {
            payloads.add(createTextPayload(i, charactersPerMessage));
        }
    }

    public void stop() throws Exception {
        payloads = null;
    }

    public void stop(AgentStopper stopper) {
        payloads = null;
    }

    public void assertMessagesCorrect(List actual) throws JMSException {
        int actualSize = actual.size();

        System.out.println("Consumed so far: " + actualSize + " message(s)");

        assertTrue("Not enough messages received: " + actualSize + " when expected: " + getSize(), actualSize >= getSize());

        for (int i = 0; i < actualSize; i++) {
            assertMessageCorrect(i, actual);
        }

        assertEquals("Number of messages received", getSize(), actualSize);
    }

    public void assertMessageCorrect(int index, List actualMessages) throws JMSException {
        Object expected = getPayloads().get(index);

        Message message = (Message) actualMessages.get(index);

        assertHeadersCorrect(message, index);

        Object actual = getMessagePayload(message);
        assertEquals("Message payload for message: " + index, expected, actual);
    }

    public void sendMessages(Session session, MessageProducer producer) throws JMSException {
        int counter = 0;
        for (Iterator iter = getPayloads().iterator(); iter.hasNext(); counter++) {
            Object element = (Object) iter.next();
            if (counter == sentCounter) {
                sentCounter++;
                Message message = createMessage(session, element, counter);
                appendHeaders(message, counter);
                producer.send(message);
            }
        }
    }

    public void sendMessages(Session session, MessageProducer producer, int percent) throws JMSException {
        int numberOfMessages = (getPayloads().size() * percent) / 100;
        int counter = 0;
        int lastMessageId = sentCounter + numberOfMessages;
        for (Iterator iter = getPayloads().iterator(); iter.hasNext() && counter < lastMessageId; counter++) {
            Object element = (Object) iter.next();
            if (counter == sentCounter) {
                sentCounter++;
                Message message = createMessage(session, element, counter);
                appendHeaders(message, counter);
                producer.send(message);
            }
        }
    }

    public int getSize() {
        return getPayloads().size();
    }

    // Properties
    // -------------------------------------------------------------------------
    public String getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(String customHeader) {
        this.customHeader = customHeader;
    }

    public List getPayloads() {
        return payloads;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void appendHeaders(Message message, int counter) throws JMSException {
        message.setIntProperty(getCustomHeader(), counter);
        message.setStringProperty("cheese", "Edam");
    }

    protected void assertHeadersCorrect(Message message, int index) throws JMSException {
        assertEquals("String property 'cheese' for message: " + index, "Edam", message.getStringProperty("cheese"));
        assertEquals("int property '" + getCustomHeader() + "' for message: " + index, index, message.getIntProperty(getCustomHeader()));
    }

    protected Object getMessagePayload(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            return textMessage.getText();
        }
        else if (message instanceof ObjectMessage) {
            ObjectMessage objectMessage = (ObjectMessage) message;
            return objectMessage.getObject();
        }
        else {
            return null;
        }
    }

    protected Message createMessage(Session session, Object element, int counter) throws JMSException {
        if (element instanceof String && useTextMessage) {
            return session.createTextMessage((String) element);
        }
        else if (element == null) {
            return session.createMessage();
        }
        else {
            return session.createObjectMessage((Serializable) element);
        }
    }

    protected Object createTextPayload(int messageCounter, int charactersPerMessage) {
        return "Body of message: " + messageCounter + " sent at: " + new Date();
    }
}
