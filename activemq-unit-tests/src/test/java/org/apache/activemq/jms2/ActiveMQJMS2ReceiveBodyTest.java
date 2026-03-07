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

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQMessageConsumer;
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

    // -----------------------------------------------------------------------
    // Jakarta Messaging 3.1 spec compliance: receiveBody type-mismatch
    // behaviour varies by acknowledgement mode.
    // -----------------------------------------------------------------------

    private static BytesMessage createNonEmptyBytesMessage(Session sess) throws Exception {
        BytesMessage bm = sess.createBytesMessage();
        bm.writeBytes(new byte[] {1, 2, 3});
        return bm;
    }

    /**
     * AUTO_ACKNOWLEDGE: on type mismatch the message must be put back on
     * the prefetch queue so the caller can read it again with the correct
     * type (the caller has no other recovery mechanism).
     */
    @Test
    public void testReceiveBodyAutoAckRequeuesOnTypeMismatch() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            // Wrong type — should throw and re-enqueue
            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            // Message should still be available via receive()
            Message msg = cons.receive(5000);
            assertNotNull("Message must be redelivered after AUTO_ACK type mismatch", msg);
            cons.close();
        }
    }

    /**
     * DUPS_OK_ACKNOWLEDGE: same as AUTO — message goes back to the prefetch
     * queue for immediate re-read.
     */
    @Test
    public void testReceiveBodyDupsOkRequeuesOnTypeMismatch() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            Message msg = cons.receive(5000);
            assertNotNull("Message must be redelivered after DUPS_OK type mismatch", msg);
            cons.close();
        }
    }

    /**
     * CLIENT_ACKNOWLEDGE: on type mismatch the message is treated as
     * delivered (not put back on the prefetch queue). The application can
     * call session.recover() to redeliver all unacknowledged messages, or
     * continue receiving and later call message.acknowledge() to retire
     * them all.
     */
    @Test
    public void testReceiveBodyClientAckTypeMismatchThenRecover() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            // Type mismatch — message is delivered but body unreadable
            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            // session.recover() redelivers all unacknowledged messages
            sess.recover();

            Message msg = cons.receive(5000);
            assertNotNull("Message must be redelivered after CLIENT_ACK session.recover()", msg);
            msg.acknowledge();
            cons.close();
        }
    }

    /**
     * CLIENT_ACKNOWLEDGE: the application can also acknowledge past the
     * failed message to retire it.
     */
    @Test
    public void testReceiveBodyClientAckTypeMismatchThenAcknowledge() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.send(sess.createTextMessage("hello"));
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            // First message: type mismatch
            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            // Second message: correct type — acknowledge retires both
            Message msg = cons.receive(5000);
            assertNotNull("Second message should be available", msg);
            assertTrue(msg instanceof jakarta.jms.TextMessage);
            msg.acknowledge();

            // After acknowledge, recover should yield nothing
            sess.recover();
            Message stale = cons.receiveNoWait();
            assertNull("No messages should remain after acknowledge", stale);
            cons.close();
        }
    }

    /**
     * CLIENT_ACKNOWLEDGE: verify the credit window is expanded so that a
     * subsequent receive does not stall when all prefetch credits were
     * consumed by failed receiveBody calls.
     */
    @Test
    public void testReceiveBodyClientAckCreditWindowExpanded() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            // Send enough messages to fill a typical prefetch window
            int messageCount = 10;
            for (int i = 0; i < messageCount; i++) {
                prod.send(createNonEmptyBytesMessage(sess));
            }
            // Send a final text message as a sentinel
            prod.send(sess.createTextMessage("sentinel"));
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            // Fail on all bytes messages
            for (int i = 0; i < messageCount; i++) {
                try {
                    ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                    fail("Expected MessageFormatException on message " + i);
                } catch (MessageFormatException e) {
                    // expected
                }
            }

            // The sentinel must be reachable — credit window must have expanded
            String body = ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
            assertEquals("sentinel", body);
            cons.close();
        }
    }

    /**
     * SESSION_TRANSACTED: on type mismatch the message is treated as
     * delivered within the transaction. The application can call
     * session.rollback() to redeliver all messages in the transaction.
     */
    @Test
    public void testReceiveBodyTransactedTypeMismatchThenRollback() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.close();
            sess.commit();

            MessageConsumer cons = sess.createConsumer(dest);

            // Type mismatch in transaction
            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            // Rollback redelivers the message
            sess.rollback();

            Message msg = cons.receive(5000);
            assertNotNull("Message must be redelivered after transacted rollback", msg);
            sess.commit();
            cons.close();
        }
    }

    /**
     * SESSION_TRANSACTED: the application can also commit past the failed
     * message to retire it.
     */
    @Test
    public void testReceiveBodyTransactedTypeMismatchThenCommit() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(createNonEmptyBytesMessage(sess));
            prod.send(sess.createTextMessage("hello"));
            prod.close();
            sess.commit();

            MessageConsumer cons = sess.createConsumer(dest);

            // First message: type mismatch
            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException");
            } catch (MessageFormatException e) {
                // expected
            }

            // Second message: success
            Message msg = cons.receive(5000);
            assertNotNull("Second message should be available", msg);
            assertTrue(msg instanceof jakarta.jms.TextMessage);

            // Commit retires both messages
            sess.commit();

            // Nothing left
            Message stale = cons.receiveNoWait();
            assertNull("No messages should remain after commit", stale);
            sess.commit();
            cons.close();
        }
    }

    /**
     * SESSION_TRANSACTED: verify the credit window is expanded so that a
     * subsequent receive does not stall when prefetch credits were consumed
     * by failed receiveBody calls.
     */
    @Test
    public void testReceiveBodyTransactedCreditWindowExpanded() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            int messageCount = 10;
            for (int i = 0; i < messageCount; i++) {
                prod.send(createNonEmptyBytesMessage(sess));
            }
            prod.send(sess.createTextMessage("sentinel"));
            prod.close();
            sess.commit();

            MessageConsumer cons = sess.createConsumer(dest);

            // Fail on all bytes messages
            for (int i = 0; i < messageCount; i++) {
                try {
                    ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                    fail("Expected MessageFormatException on message " + i);
                } catch (MessageFormatException e) {
                    // expected
                }
            }

            // The sentinel must be reachable
            String body = ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
            assertEquals("sentinel", body);
            sess.commit();
            cons.close();
        }
    }

    /**
     * Jakarta Messaging 3.1 spec: receiveBody may be used to receive any type
     * of message except for StreamMessage and Message (the base type with no
     * body).  A plain Message should cause MessageFormatException.
     */
    @Test
    public void testReceiveBodyThrowsOnPlainMessage() throws Exception {
        try (Connection conn = activemqConnectionFactory.createConnection(DEFAULT_JMS_USER, DEFAULT_JMS_PASS)) {
            conn.start();
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination dest = sess.createQueue(methodNameDestinationName);

            MessageProducer prod = sess.createProducer(dest);
            prod.send(sess.createMessage());
            prod.close();

            MessageConsumer cons = sess.createConsumer(dest);

            try {
                ((ActiveMQMessageConsumer) cons).receiveBody(String.class, 5000L);
                fail("Expected MessageFormatException for plain Message");
            } catch (MessageFormatException e) {
                // expected
            }

            // Message should be re-enqueued (AUTO_ACK mode)
            Message msg = cons.receive(5000);
            assertNotNull("Plain Message should still be available after type mismatch", msg);
            cons.close();
        }
    }
}
