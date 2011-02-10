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
package org.apache.activemq.test;

import java.io.File;
import java.lang.reflect.Array;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Useful base class for unit test cases
 * 
 * @version $Revision: 1.4 $
 */
public abstract class TestSupport extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestSupport.class);
    
    protected ActiveMQConnectionFactory connectionFactory;
    protected boolean topic = true;

    public TestSupport() {
        super();
    }

    public TestSupport(String name) {
        super(name);
    }

    /**
     * Creates an ActiveMQMessage.
     * 
     * @return ActiveMQMessage
     */
    protected ActiveMQMessage createMessage() {
        return new ActiveMQMessage();
    }

    /**
     * Creates a destination.
     * 
     * @param subject - topic or queue name.
     * @return Destination - either an ActiveMQTopic or ActiveMQQUeue.
     */
    protected Destination createDestination(String subject) {
        if (topic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    /**
     * Tests if firstSet and secondSet are equal.
     * 
     * @param messsage - string to be displayed when the assertion fails.
     * @param firstSet[] - set of messages to be compared with its counterpart
     *                in the secondset.
     * @param secondSet[] - set of messages to be compared with its counterpart
     *                in the firstset.
     * @throws JMSException
     */
    protected void assertTextMessagesEqual(Message[] firstSet, Message[] secondSet) throws JMSException {
        assertTextMessagesEqual("", firstSet, secondSet);
    }

    /**
     * Tests if firstSet and secondSet are equal.
     * 
     * @param messsage - string to be displayed when the assertion fails.
     * @param firstSet[] - set of messages to be compared with its counterpart
     *                in the secondset.
     * @param secondSet[] - set of messages to be compared with its counterpart
     *                in the firstset.
     */
    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet) throws JMSException {
        assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);

        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            assertTextMessageEqual("Message " + (i + 1) + " did not match : ", m1, m2);
        }
    }

    /**
     * Tests if m1 and m2 are equal.
     * 
     * @param m1 - message to be compared with m2.
     * @param m2 - message to be compared with m1.
     * @throws JMSException
     */
    protected void assertEquals(TextMessage m1, TextMessage m2) throws JMSException {
        assertEquals("", m1, m2);
    }

    /**
     * Tests if m1 and m2 are equal.
     * 
     * @param message - string to be displayed when the assertion fails.
     * @param m1 - message to be compared with m2.
     * @param m2 - message to be compared with m1.
     */
    protected void assertTextMessageEqual(String message, TextMessage m1, TextMessage m2) throws JMSException {
        assertFalse(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);

        if (m1 == null) {
            return;
        }

        assertEquals(message, m1.getText(), m2.getText());
    }

    /**
     * Tests if m1 and m2 are equal.
     * 
     * @param m1 - message to be compared with m2.
     * @param m2 - message to be compared with m1.
     * @throws JMSException
     */
    protected void assertEquals(Message m1, Message m2) throws JMSException {
        assertEquals("", m1, m2);
    }

    /**
     * Tests if m1 and m2 are equal.
     * 
     * @param message - error message.
     * @param m1 - message to be compared with m2.
     * @param m2 -- message to be compared with m1.
     */
    protected void assertEquals(String message, Message m1, Message m2) throws JMSException {
        assertFalse(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);

        if (m1 == null) {
            return;
        }

        assertTrue(message + ": expected {" + m1 + "}, but was {" + m2 + "}", m1.getClass() == m2.getClass());

        if (m1 instanceof TextMessage) {
            assertTextMessageEqual(message, (TextMessage)m1, (TextMessage)m2);
        } else {
            assertEquals(message, m1, m2);
        }
    }
    
    /**
     * Test if base directory contains spaces
     */
    protected void assertBaseDirectoryContainsSpaces() {
    	assertFalse("Base directory cannot contain spaces.", new File(System.getProperty("basedir", ".")).getAbsoluteFile().toString().contains(" "));
    }

    /**
     * Creates an ActiveMQConnectionFactory.
     * 
     * @return ActiveMQConnectionFactory
     * @throws Exception
     */
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }

    /**
     * Factory method to create a new connection.
     * 
     * @return connection
     * @throws Exception
     */
    protected Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }

    /**
     * Creates an ActiveMQ connection factory.
     * 
     * @return connectionFactory
     * @throws Exception
     */
    public ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }

        return connectionFactory;
    }

    /**
     * Returns the consumer subject.
     * 
     * @return String
     */
    protected String getConsumerSubject() {
        return getSubject();
    }

    /**
     * Returns the producer subject.
     * 
     * @return String
     */
    protected String getProducerSubject() {
        return getSubject();
    }

    /**
     * Returns the subject.
     * 
     * @return String
     */
    protected String getSubject() {
        return getClass().getName() + "." + getName();
    }

    protected void assertArrayEqual(String message, Object[] expected, Object[] actual) {
        assertEquals(message + ". Array length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(message + ". element: " + i, expected[i], actual[i]);
        }
    }

    protected void assertPrimitiveArrayEqual(String message, Object expected, Object actual) {
        int length = Array.getLength(expected);
        assertEquals(message + ". Array length", length, Array.getLength(actual));
        for (int i = 0; i < length; i++) {
            assertEquals(message + ". element: " + i, Array.get(expected, i), Array.get(actual, i));
        }
    }
}
