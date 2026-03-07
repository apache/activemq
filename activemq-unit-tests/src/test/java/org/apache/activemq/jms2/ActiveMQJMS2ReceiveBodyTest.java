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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Map;

import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;

import org.junit.Test;

public class ActiveMQJMS2ReceiveBodyTest extends ActiveMQJMS2TestBase {

    @Test
    public void testReceiveBodyTextMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, "hello");

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                String body = jmsConsumer.receiveBody(String.class, 5000);
                assertEquals("hello", body);
            }
        }
    }

    @Test
    public void testReceiveBodyBytesMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            byte[] payload = new byte[] {1, 2, 3, 4, 5};
            jmsContext.createProducer().send(destination, payload);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                byte[] body = jmsConsumer.receiveBody(byte[].class, 5000);
                assertArrayEquals(payload, body);
            }
        }
    }

    @Test
    public void testReceiveBodyObjectMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, (Serializable) "testObject");

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                Serializable body = jmsConsumer.receiveBody(Serializable.class, 5000);
                assertEquals("testObject", body);
            }
        }
    }

    @Test
    public void testReceiveBodyMapMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, Map.of("key", "value"));

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                @SuppressWarnings("unchecked")
                Map<String, Object> body = jmsConsumer.receiveBody(Map.class, 5000);
                assertNotNull(body);
                assertEquals("value", body.get("key"));
            }
        }
    }

    @Test
    public void testReceiveBodyBlockingTextMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, "blocking-test");

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                String body = jmsConsumer.receiveBody(String.class);
                assertEquals("blocking-test", body);
            }
        }
    }

    @Test
    public void testReceiveBodyNoWaitTextMessage() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, "hello");

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                // Give time for message to arrive in prefetch
                Thread.sleep(500);
                String body = jmsConsumer.receiveBodyNoWait(String.class);
                assertEquals("hello", body);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e.getMessage());
        }
    }

    @Test
    public void testReceiveBodyNoWaitReturnsNullWhenEmpty() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                String body = jmsConsumer.receiveBodyNoWait(String.class);
                assertNull(body);
            }
        }
    }

    @Test
    public void testReceiveBodyWithTimeoutReturnsNullOnExpiry() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                String body = jmsConsumer.receiveBody(String.class, 100);
                assertNull(body);
            }
        }
    }

    @Test(expected = MessageFormatRuntimeException.class)
    public void testReceiveBodyThrowsMessageFormatRuntimeExceptionOnTypeMismatch() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                jmsConsumer.receiveBody(String.class, 5000);
            }
        }
    }

    @Test(expected = MessageFormatRuntimeException.class)
    public void testReceiveBodyBlockingThrowsMessageFormatRuntimeExceptionOnTypeMismatch() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                jmsConsumer.receiveBody(String.class);
            }
        }
    }

    @Test(expected = MessageFormatRuntimeException.class)
    public void testReceiveBodyNoWaitThrowsMessageFormatRuntimeExceptionOnTypeMismatch() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                // Give time for message to arrive in prefetch
                try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                jmsConsumer.receiveBodyNoWait(String.class);
            }
        }
    }

    @Test
    public void testReceiveBodyMessageNotAcknowledgedOnTypeMismatch() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // Attempt with wrong type — message should not be acknowledged
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // Message should still be available via receive()
                assertNotNull("Message should be redelivered after type mismatch",
                        jmsConsumer.receive(5000));
            }
        }
    }

    @Test
    public void testReceiveBodyCorrectTypeAfterMismatch() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            byte[] payload = new byte[] {1, 2, 3};
            jmsContext.createProducer().send(destination, payload);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // Attempt with wrong type
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // Retry with the correct type — should succeed
                byte[] body = jmsConsumer.receiveBody(byte[].class, 5000);
                assertArrayEquals(payload, body);
            }
        }
    }

    // ---- Plain Message type: receiveBody must always throw ----

    @Test
    public void testReceiveBodyPlainMessageThrows() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            Message plainMessage = jmsContext.createMessage();
            jmsContext.createProducer().send(destination, plainMessage);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException for plain Message");
                } catch (MessageFormatRuntimeException e) {
                    // expected per spec
                }
            }
        }
    }

    @Test
    public void testReceiveBodyPlainMessageAutoAckRedelivered() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            Message plainMessage = jmsContext.createMessage();
            jmsContext.createProducer().send(destination, plainMessage);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // AUTO_ACK: message should be re-enqueued and available via receive()
                Message redelivered = jmsConsumer.receive(5000);
                assertNotNull("Plain Message should be redelivered in AUTO_ACK mode", redelivered);
            }
        }
    }

    // ---- StreamMessage type: receiveBody must always throw ----

    @Test
    public void testReceiveBodyStreamMessageThrows() throws JMSException {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            StreamMessage streamMessage = jmsContext.createStreamMessage();
            streamMessage.writeString("payload");
            jmsContext.createProducer().send(destination, streamMessage);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException for StreamMessage");
                } catch (MessageFormatRuntimeException e) {
                    // expected per spec
                }
            }
        }
    }

    @Test
    public void testReceiveBodyStreamMessageAutoAckRedelivered() throws JMSException {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            StreamMessage streamMessage = jmsContext.createStreamMessage();
            streamMessage.writeString("payload");
            jmsContext.createProducer().send(destination, streamMessage);

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // AUTO_ACK: message should be re-enqueued and available via receive()
                Message redelivered = jmsConsumer.receive(5000);
                assertNotNull("StreamMessage should be redelivered in AUTO_ACK mode", redelivered);
            }
        }
    }

    // ---- CLIENT_ACKNOWLEDGE: message treated as delivered on failure ----

    @Test
    public void testReceiveBodyClientAckMessageTreatedAsDelivered() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.CLIENT_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // receiveBody with wrong type should throw
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // CLIENT_ACK: message is treated as delivered, not re-enqueued.
                // A subsequent receive without recover should not return the same message.
                Message msg = jmsConsumer.receiveNoWait();
                assertNull("Message should not be re-enqueued in CLIENT_ACK mode", msg);
            }
        }
    }

    @Test
    public void testReceiveBodyClientAckRecoverRedelivers() throws JMSException {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.CLIENT_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // After recover(), the message should be redelivered
                jmsContext.recover();
                Message redelivered = jmsConsumer.receive(5000);
                assertNotNull("Message should be redelivered after session.recover()", redelivered);
                assertTrue("Redelivered flag should be set", redelivered.getJMSRedelivered());
            }
        }
    }

    // ---- TRANSACTED: message treated as delivered within transaction ----

    @Test
    public void testReceiveBodyTransactedMessageTreatedAsDelivered() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});
            jmsContext.commit();

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // receiveBody with wrong type should throw
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // TRANSACTED: message is treated as delivered, not re-enqueued.
                // A subsequent receive within the same transaction should not return it.
                Message msg = jmsConsumer.receiveNoWait();
                assertNull("Message should not be re-enqueued in TRANSACTED mode", msg);
            }
        }
    }

    @Test
    public void testReceiveBodyTransactedRollbackRedelivers() throws JMSException {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});
            jmsContext.commit();

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // After rollback(), the message should be redelivered
                jmsContext.rollback();
                Message redelivered = jmsConsumer.receive(5000);
                assertNotNull("Message should be redelivered after transaction rollback", redelivered);
                assertTrue("Redelivered flag should be set", redelivered.getJMSRedelivered());
            }
        }
    }

    // ---- Credit window: consumer must not get stuck after failed receiveBody ----

    @Test
    public void testReceiveBodyClientAckCreditWindowNotExhausted() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.CLIENT_ACKNOWLEDGE)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            // Send two messages: first will fail receiveBody, second should still be receivable
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});
            jmsContext.createProducer().send(destination, "hello");

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // First message: type mismatch
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // Second message should still be consumable (credit window expanded)
                String body = jmsConsumer.receiveBody(String.class, 5000);
                assertEquals("hello", body);
            }
        }
    }

    @Test
    public void testReceiveBodyTransactedCreditWindowNotExhausted() {
        try (JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            // Send two messages: first will fail receiveBody, second should still be receivable
            jmsContext.createProducer().send(destination, new byte[] {1, 2, 3});
            jmsContext.createProducer().send(destination, "hello");
            jmsContext.commit();

            try (JMSConsumer jmsConsumer = jmsContext.createConsumer(destination)) {
                jmsContext.start();

                // First message: type mismatch
                try {
                    jmsConsumer.receiveBody(String.class, 5000);
                    fail("Should have thrown MessageFormatRuntimeException");
                } catch (MessageFormatRuntimeException e) {
                    // expected
                }

                // Second message should still be consumable (credit window expanded)
                String body = jmsConsumer.receiveBody(String.class, 5000);
                assertEquals("hello", body);
            }
        }
    }
}
