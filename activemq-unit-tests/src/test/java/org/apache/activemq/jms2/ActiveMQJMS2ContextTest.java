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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Enumeration;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQContext;
import org.junit.Test;

public class ActiveMQJMS2ContextTest extends ActiveMQJMS2TestBase {

    @Test
    public void testConnectionFactoryCreateContext() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            recvMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionFactoryCreateContextSession() {
        activemqConnectionFactory.createContext(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testConnectionFactoryCreateContextUserPass() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            recvMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionFactoryCreateContextUserPassSession() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
        }
    }

    @Test
    public void testConnectionFactoryCreateContexMultiContext() {
        JMSContext secondJMSContext = null;
        JMSContext thirdJMSContext = null;

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));

            Destination testDestination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, testDestination, "Test-" + methodNameDestinationName);
            recvMessage(jmsContext, testDestination, "Test-" + methodNameDestinationName);

            secondJMSContext = jmsContext.createContext(Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            fail(e.getMessage());
        }

        // First context closed
        String secondTestDestinationName = methodNameDestinationName + ".SECOND";
        Destination secondTestDestination = secondJMSContext.createQueue(secondTestDestinationName);

        try {
            sendMessage(secondJMSContext, secondTestDestination, "Test-" + methodNameDestinationName);
            recvMessage(secondJMSContext, secondTestDestination, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        } finally {
            if(secondJMSContext != null) {
                try { secondJMSContext.close(); } catch (JMSRuntimeException e) { fail(e.getMessage()); }
            }
        }

        // Attempt to obtain a third context after all contexts have been closed
        boolean caught = false;
        try {
            thirdJMSContext = secondJMSContext.createContext(Session.AUTO_ACKNOWLEDGE);
            fail("JMSRuntimeException expected");
        } catch (JMSRuntimeException e) {
            assertNull(thirdJMSContext);
            caught = true;
            assertEquals("Context already closed", e.getMessage());
        }
        assertTrue(caught);
    }

    @Test
    public void testConnectionFactoryCreateContextBrowse() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            browseMessage(jmsContext, destination, "Test-" + methodNameDestinationName, true);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionFactoryCreateContextBrowseAutoStart() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            browseMessage(jmsContext, destination, "Test-" + methodNameDestinationName, true);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionFactoryCreateContextBrowseAutoStartFalse() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.setAutoStart(false);
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            browseMessage(jmsContext, destination, "Test-" + methodNameDestinationName, false);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionFactoryCreateContextBrowseAutoStartFalseStartDelayed() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.setAutoStart(false);
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            sendMessage(jmsContext, destination, "Test-" + methodNameDestinationName);
            jmsContext.start();
            browseMessage(jmsContext, destination, "Test-" + methodNameDestinationName, true);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDisableMessageID() {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.setAutoStart(false);
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            jmsContext.start();
            JMSProducer jmsProducer = jmsContext.createProducer();
            jmsProducer.setDisableMessageID(true);
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            Message sendMessage = jmsContext.createTextMessage("Test-" + methodNameDestinationName);
            jmsProducer.send(destination, sendMessage);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            Message recvMessage = jmsConsumer.receive(5000l);
            assertNotNull(recvMessage);
            // Verify messageID since ActiveMQ does not disableMessageID
            assertEquals(sendMessage.getJMSMessageID(), recvMessage.getJMSMessageID());
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionSharedConnectionConsumer() throws JMSException {
        connection.createSharedConnectionConsumer(null, null, null, null, 10);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConnectionSharedDurableConnectionConsumer() throws JMSException {
        connection.createSharedDurableConnectionConsumer(null, null, null, null, 10);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionAckMode() throws JMSException {
        connection.createSession(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testSessionDurableConsumer() throws JMSException {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.setAutoStart(false);
            jmsContext.setClientID(testName.getMethodName());
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Topic topic = jmsContext.createTopic(methodNameDestinationName);

            JMSConsumer jmsConsumerRegisterDurableConsumer = jmsContext.createDurableConsumer(topic, testName.getMethodName());
            jmsConsumerRegisterDurableConsumer.close();
            sendMessage(jmsContext, topic, "Test-" + methodNameDestinationName);
            recvMessageDurable(jmsContext, topic, testName.getMethodName(), null, false, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSessionDurableConsumerSelectorNoLocal() throws JMSException {
        try(JMSContext jmsContext = activemqConnectionFactory.createContext()) {
            assertNotNull(jmsContext);
            jmsContext.setAutoStart(false);
            jmsContext.setClientID(testName.getMethodName());
            jmsContext.start();
            assertTrue(ActiveMQContext.class.isAssignableFrom(jmsContext.getClass()));
            Topic topic = jmsContext.createTopic(methodNameDestinationName);

            JMSConsumer jmsConsumerRegisterDurableConsumer = jmsContext.createDurableConsumer(topic, testName.getMethodName(), "TRUE", false);
            jmsConsumerRegisterDurableConsumer.close();
            sendMessage(jmsContext, topic, "Test-" + methodNameDestinationName);
            recvMessageDurable(jmsContext, topic, testName.getMethodName(), "TRUE", false, "Test-" + methodNameDestinationName);
        } catch (JMSException e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedConsumer() throws JMSException {
        session.createSharedConsumer(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedConsumerSelector() throws JMSException {
        session.createSharedConsumer(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedDurableConsumer() throws JMSException {
        session.createSharedDurableConsumer(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSessionSharedDurableConsumerSelector() throws JMSException {
        session.createSharedDurableConsumer(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerDeliveryDelayGet() throws JMSException {
        messageProducer.getDeliveryDelay();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerDeliveryDelaySet() throws JMSException {
        messageProducer.setDeliveryDelay(1000l);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendMessageCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue(methodNameDestinationName), null, (CompletionListener)null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendMessageQoSParamsCompletionListener() throws JMSException {
         messageProducer.send(null, 1, 4, 0l, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendDestinationMessageCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue(methodNameDestinationName), null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testProducerSendDestinationMessageQosParamsCompletionListener() throws JMSException {
         messageProducer.send(session.createQueue(methodNameDestinationName), null, 1, 4, 0l, null);
    }

    protected static void sendMessage(JMSContext jmsContext, Destination testDestination, String textBody) {
        assertNotNull(jmsContext);
        JMSProducer jmsProducer = jmsContext.createProducer();
        jmsProducer.send(testDestination, textBody);
    }

    protected static void browseMessage(JMSContext jmsContext, Destination testDestination, String expectedTextBody, boolean expectFound) throws JMSException {
        assertNotNull(jmsContext);
        assertTrue(Queue.class.isAssignableFrom(testDestination.getClass()));
        Queue testQueue = Queue.class.cast(testDestination);
        try(QueueBrowser queueBrowser = jmsContext.createBrowser(testQueue)) {
            Enumeration<?> messageEnumeration = queueBrowser.getEnumeration();
            assertNotNull(messageEnumeration);

            boolean found = false; 
            while(!found && messageEnumeration.hasMoreElements()) {
                javax.jms.Message message = (javax.jms.Message)messageEnumeration.nextElement();
                assertNotNull(message);
                assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
                assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
                found = true;
            }
            assertEquals(expectFound, found);
        }
    }

    protected static void recvMessage(JMSContext jmsContext, Destination testDestination, String expectedTextBody) throws JMSException {
        assertNotNull(jmsContext);
        try(JMSConsumer jmsConsumer = jmsContext.createConsumer(testDestination)) {
            javax.jms.Message message = jmsConsumer.receive(1000l);
            assertNotNull(message);
            assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
            assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
        }
    }

    protected static void recvMessageDurable(JMSContext jmsContext, Topic testTopic, String subscriptionName, String selector, boolean noLocal, String expectedTextBody) throws JMSException {
        assertNotNull(jmsContext);
        try(JMSConsumer jmsConsumer = jmsContext.createDurableConsumer(testTopic, subscriptionName, selector, noLocal)) {
            javax.jms.Message message = jmsConsumer.receive(1000l);
            assertNotNull(message);
            assertTrue(TextMessage.class.isAssignableFrom(message.getClass()));
            assertEquals(expectedTextBody, TextMessage.class.cast(message).getText());
        }
    }
}
