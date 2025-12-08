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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ActiveMQConsumerTest {

    private ActiveMQConnectionFactory connectionFactory;
    private JMSContext jmsContext;
    private Queue testQueue;

    @Before
    public void setUp() throws Exception {
        connectionFactory = new ActiveMQConnectionFactory("vm://localhost?marshal=false&broker.persistent=false");
        jmsContext = connectionFactory.createContext();
        jmsContext.start();
        testQueue = jmsContext.createQueue("test.queue.receiveBody");
    }

    @After
    public void tearDown() {
        if (jmsContext != null) {
            jmsContext.close();
        }
    }

    @Test
    public void testReceiveBody() {
        // Send a message
        String testMessage = "Test message body";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Receive body using receiveBody(Class)
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            String body = consumer.receiveBody(String.class);
            assertNotNull("Received body should not be null", body);
            assertEquals("Received body should match sent message", testMessage, body);
        }
    }

    @Test
    public void testReceiveBodyWithTimeout() {
        // Send a message
        String testMessage = "Test message body with timeout";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Receive body using receiveBody(Class, long)
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            String body = consumer.receiveBody(String.class, 5000L);
            assertNotNull("Received body should not be null", body);
            assertEquals("Received body should match sent message", testMessage, body);
        }
    }

    @Test
    public void testReceiveBodyNoWait() {
        // Send a message
        String testMessage = "Test message body no wait";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Receive body using receiveBodyNoWait(Class)
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            String body = consumer.receiveBodyNoWait(String.class);
            assertNotNull("Received body should not be null", body);
            assertEquals("Received body should match sent message", testMessage, body);
        }
    }

    @Test
    public void testReceiveBodyReturnsNullWhenNoMessage() {
        // Don't send any message
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            // Use a short timeout to avoid blocking too long
            String body = consumer.receiveBody(String.class, 100L);
            assertNull("Received body should be null when no message available", body);
        }
    }

    @Test
    public void testReceiveBodyNoWaitReturnsNullWhenNoMessage() {
        // Don't send any message
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            String body = consumer.receiveBodyNoWait(String.class);
            assertNull("Received body should be null when no message available", body);
        }
    }

    @Test
    public void testReceiveBodyWithWrongType() {
        // Send a text message
        String testMessage = "Test message";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Try to receive as wrong type
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            try {
                consumer.receiveBody(Integer.class);
                fail("Should throw JMSRuntimeException for wrong type");
            } catch (JMSRuntimeException e) {
                // Expected - MessageFormatException wrapped in JMSRuntimeException
                assertNotNull("Exception should have a cause", e.getCause());
                assertTrue("Cause should be MessageFormatException", 
                    e.getCause() instanceof MessageFormatException);
            }
        }
    }

    @Test
    public void testReceiveBodyWithTimeoutWithWrongType() {
        // Send a text message
        String testMessage = "Test message";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Try to receive as wrong type
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            try {
                consumer.receiveBody(Integer.class, 5000L);
                fail("Should throw JMSRuntimeException for wrong type");
            } catch (JMSRuntimeException e) {
                // Expected - MessageFormatException wrapped in JMSRuntimeException
                assertNotNull("Exception should have a cause", e.getCause());
                assertTrue("Cause should be MessageFormatException", 
                    e.getCause() instanceof MessageFormatException);
            }
        }
    }

    @Test
    public void testReceiveBodyNoWaitWithWrongType() {
        // Send a text message
        String testMessage = "Test message";
        jmsContext.createProducer().send(testQueue, testMessage);

        // Try to receive as wrong type
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            try {
                consumer.receiveBodyNoWait(Integer.class);
                fail("Should throw JMSRuntimeException for wrong type");
            } catch (JMSRuntimeException e) {
                // Expected - MessageFormatException wrapped in JMSRuntimeException
                assertNotNull("Exception should have a cause", e.getCause());
                assertTrue("Cause should be MessageFormatException", 
                    e.getCause() instanceof MessageFormatException);
            }
        }
    }

    @Test
    public void testReceiveBodyMultipleMessages() {
        // Send multiple messages
        String[] messages = {"Message 1", "Message 2", "Message 3"};
        for (String msg : messages) {
            jmsContext.createProducer().send(testQueue, msg);
        }

        // Receive all messages using receiveBody
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            for (String expected : messages) {
                String body = consumer.receiveBody(String.class);
                assertNotNull("Received body should not be null", body);
                assertEquals("Received body should match sent message", expected, body);
            }
        }
    }

    @Test
    public void testReceiveBodyWithTimeoutExpires() {
        // Don't send any message
        try (jakarta.jms.JMSConsumer consumer = jmsContext.createConsumer(testQueue)) {
            // Use a short timeout
            long startTime = System.currentTimeMillis();
            String body = consumer.receiveBody(String.class, 200L);
            long elapsed = System.currentTimeMillis() - startTime;
            
            assertNull("Received body should be null when timeout expires", body);
            // Verify it actually waited (at least 100ms, but not too long)
            assertTrue("Should have waited at least some time", elapsed >= 100);
        }
    }
}

