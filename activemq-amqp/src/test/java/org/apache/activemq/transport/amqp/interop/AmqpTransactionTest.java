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
package org.apache.activemq.transport.amqp.interop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.junit.Test;

/**
 * Test various aspects of Transaction support.
 */
public class AmqpTransactionTest extends AmqpClientTestSupport {

    @Test(timeout = 30000)
    public void testBeginAndCommitTransaction() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();
        assertNotNull(session);

        session.begin();
        assertTrue(session.isInTransaction());
        session.commit();

        connection.close();
    }

    @Test(timeout = 30000)
    public void testBeginAndRollbackTransaction() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();
        assertNotNull(session);

        session.begin();
        assertTrue(session.isInTransaction());
        session.rollback();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueueWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        final QueueViewMBean queue = getProxyToQueue(getTestName());

        session.begin();

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        assertEquals(0, queue.getQueueSize());

        session.commit();

        assertEquals(1, queue.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueueWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        final QueueViewMBean queue = getProxyToQueue(getTestName());

        session.begin();

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        assertEquals(0, queue.getQueueSize());

        session.rollback();

        assertEquals(0, queue.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        final QueueViewMBean queue = getProxyToQueue(getTestName());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        assertEquals(1, queue.getQueueSize());

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        session.begin();

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();

        session.commit();

        assertEquals(0, queue.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveAfterConnectionClose() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender(getTestName());
        final QueueViewMBean queue = getProxyToQueue(getTestName());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        assertEquals(1, queue.getQueueSize());

        AmqpReceiver receiver = session.createReceiver(getTestName());

        session.begin();

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();

        // this will force a rollback on the TX (It should at least)
        connection.close();

        connection = client.connect();
        session = connection.createSession();
        receiver = session.createReceiver(getTestName());
        session.begin();
        receiver.flow(1);

        received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();

        session.commit();

        assertEquals(0, queue.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        final QueueViewMBean queue = getProxyToQueue(getTestName());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        assertEquals(1, queue.getQueueSize());

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        session.begin();

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();

        session.rollback();

        assertEquals(1, queue.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testMultipleSessionReceiversInSingleTXNWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Load up the Queue with some messages
        {
            AmqpSession session = connection.createSession();
            AmqpSender sender = session.createSender("queue://" + getTestName());
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            sender.send(message);
            sender.send(message);
            sender.send(message);
            sender.close();
        }

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Create some sender sessions
        AmqpSession session1 = connection.createSession();
        AmqpSession session2 = connection.createSession();
        AmqpSession session3 = connection.createSession();

        // Sender linked to each session
        AmqpReceiver receiver1 = session1.createReceiver("queue://" + getTestName());
        AmqpReceiver receiver2 = session2.createReceiver("queue://" + getTestName());
        AmqpReceiver receiver3 = session3.createReceiver("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(3, queue.getQueueSize());

        // Begin the transaction that all senders will operate in.
        txnSession.begin();

        assertTrue(txnSession.isInTransaction());

        receiver1.flow(1);
        receiver2.flow(1);
        receiver3.flow(1);

        AmqpMessage message1 = receiver1.receive(5, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver2.receive(5, TimeUnit.SECONDS);
        AmqpMessage message3 = receiver3.receive(5, TimeUnit.SECONDS);

        message1.accept(txnSession);
        message2.accept(txnSession);
        message3.accept(txnSession);

        assertEquals(3, queue.getQueueSize());

        txnSession.commit();

        assertEquals(0, queue.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testMultipleSessionReceiversInSingleTXNWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Load up the Queue with some messages
        {
            AmqpSession session = connection.createSession();
            AmqpSender sender = session.createSender("queue://" + getTestName());
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            sender.send(message);
            sender.send(message);
            sender.send(message);
            sender.close();
        }

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Create some sender sessions
        AmqpSession session1 = connection.createSession();
        AmqpSession session2 = connection.createSession();
        AmqpSession session3 = connection.createSession();

        // Sender linked to each session
        AmqpReceiver receiver1 = session1.createReceiver("queue://" + getTestName());
        AmqpReceiver receiver2 = session2.createReceiver("queue://" + getTestName());
        AmqpReceiver receiver3 = session3.createReceiver("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(3, queue.getQueueSize());

        // Begin the transaction that all senders will operate in.
        txnSession.begin();

        assertTrue(txnSession.isInTransaction());

        receiver1.flow(1);
        receiver2.flow(1);
        receiver3.flow(1);

        AmqpMessage message1 = receiver1.receive(5, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver2.receive(5, TimeUnit.SECONDS);
        AmqpMessage message3 = receiver3.receive(5, TimeUnit.SECONDS);

        message1.accept(txnSession);
        message2.accept(txnSession);
        message3.accept(txnSession);

        assertEquals(3, queue.getQueueSize());

        txnSession.rollback();

        assertEquals(3, queue.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testMultipleSessionSendersInSingleTXNWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Create some sender sessions
        AmqpSession session1 = connection.createSession();
        AmqpSession session2 = connection.createSession();
        AmqpSession session3 = connection.createSession();

        // Sender linked to each session
        AmqpSender sender1 = session1.createSender("queue://" + getTestName());
        AmqpSender sender2 = session2.createSender("queue://" + getTestName());
        AmqpSender sender3 = session3.createSender("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(0, queue.getQueueSize());

        // Begin the transaction that all senders will operate in.
        txnSession.begin();

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");

        assertTrue(txnSession.isInTransaction());

        sender1.send(message, txnSession.getTransactionId());
        sender2.send(message, txnSession.getTransactionId());
        sender3.send(message, txnSession.getTransactionId());

        assertEquals(0, queue.getQueueSize());

        txnSession.commit();

        assertEquals(3, queue.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testMultipleSessionSendersInSingleTXNWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Create some sender sessions
        AmqpSession session1 = connection.createSession();
        AmqpSession session2 = connection.createSession();
        AmqpSession session3 = connection.createSession();

        // Sender linked to each session
        AmqpSender sender1 = session1.createSender("queue://" + getTestName());
        AmqpSender sender2 = session2.createSender("queue://" + getTestName());
        AmqpSender sender3 = session3.createSender("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(0, queue.getQueueSize());

        // Begin the transaction that all senders will operate in.
        txnSession.begin();

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");

        assertTrue(txnSession.isInTransaction());

        sender1.send(message, txnSession.getTransactionId());
        sender2.send(message, txnSession.getTransactionId());
        sender3.send(message, txnSession.getTransactionId());

        assertEquals(0, queue.getQueueSize());

        txnSession.rollback();

        assertEquals(0, queue.getQueueSize());
    }

    @Test(timeout = 60000)
    public void testAcceptedButNotSettledInTXRemainsAquiredCanBeAccepted() throws Exception {
        doTestAcceptedButNotSettledInTXRemainsAquired(Accepted.getInstance());
    }

    @Test(timeout = 60000)
    public void testAcceptedButNotSettledInTXRemainsAquiredCanBeReleased() throws Exception {
        doTestAcceptedButNotSettledInTXRemainsAquired(Released.getInstance());
    }

    @Test(timeout = 60000)
    public void testAcceptedButNotSettledInTXRemainsAquiredCanBeRejected() throws Exception {
        doTestAcceptedButNotSettledInTXRemainsAquired(new Rejected());
    }

    @Test(timeout = 60000)
    public void testAcceptedButNotSettledInTXRemainsAquiredCanBeModifiedAsFailed() throws Exception {
        Modified outcome = new Modified();
        outcome.setDeliveryFailed(true);
        doTestAcceptedButNotSettledInTXRemainsAquired(outcome);
    }

    @Test(timeout = 60000)
    public void testAcceptedButNotSettledInTXRemainsAquiredCanBeModifiedAsUndeliverable() throws Exception {
        Modified outcome = new Modified();
        outcome.setDeliveryFailed(true);
        outcome.setUndeliverableHere(true);
        doTestAcceptedButNotSettledInTXRemainsAquired(outcome);
    }

    private void doTestAcceptedButNotSettledInTXRemainsAquired(Outcome outcome) throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        session.begin();

        receiver.flow(10);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept(false);

        session.rollback();

        // Message should remain acquired an not be redelivered.
        assertEquals(1, queue.getQueueSize());
        assertNull(receiver.receive(2, TimeUnit.SECONDS));

        if (outcome instanceof Released || outcome instanceof Rejected) {
            // Receiver should be able to release the still acquired message and the
            // broker should redispatch it to the client again.
            received.release();
            received = receiver.receive(3, TimeUnit.SECONDS);
            assertNotNull(received);
            received.accept();
            received = receiver.receive(2, TimeUnit.SECONDS);
            assertNull(received);
            assertEquals(0, queue.getQueueSize());
        } else if (outcome instanceof Accepted) {
            // Receiver should be able to accept the still acquired message and the
            // broker should then mark it as consumed.
            received.accept();
            received = receiver.receive(2, TimeUnit.SECONDS);
            assertNull(received);
            assertEquals(0, queue.getQueueSize());
        } else if (outcome instanceof Modified) {
            // Depending on the undeliverable here state the message will either be
            // redelivered or DLQ'd
            Modified modified = (Modified) outcome;
            received.modified(Boolean.TRUE.equals(modified.getDeliveryFailed()), Boolean.TRUE.equals(modified.getUndeliverableHere()));
            if (Boolean.TRUE.equals(modified.getUndeliverableHere())) {
                received = receiver.receive(2, TimeUnit.SECONDS);
                assertNull(received);
                assertEquals(0, queue.getQueueSize());
            } else {
                received = receiver.receive(3, TimeUnit.SECONDS);
                assertNotNull(received);
                received.accept();
                received = receiver.receive(2, TimeUnit.SECONDS);
                assertNull(received);
                assertEquals(0, queue.getQueueSize());
            }
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testTransactionallyAcquiredMessageCanBeTransactionallyConsumed() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        final QueueViewMBean queue = getProxyToQueue(getTestName());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        session.begin();

        receiver.flow(10);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept(false);

        session.rollback();

        // Message should remain acquired an not be redelivered.
        assertEquals(1, queue.getQueueSize());
        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        // Consume under TX but settle this time
        session.begin();
        received.accept(false);
        session.rollback();

        // Should still be acquired
        assertEquals(1, queue.getQueueSize());
        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        // Consume under TX and settle but rollback, message should be redelivered.
        session.begin();
        received.accept();
        session.rollback();

        assertEquals(1, queue.getQueueSize());
        received = receiver.receive(1, TimeUnit.SECONDS);
        assertNotNull(received);

        // Consume under TX and commit it this time.
        session.begin();
        received.accept(false);
        session.commit();

        // Check that it is now consumed and no more message available
        assertTrue(received.getWrappedDelivery().remotelySettled());
        assertEquals(0, queue.getQueueSize());
        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        connection.close();
    }

    //----- Tests Ported from AmqpNetLite client -----------------------------//

    @Test(timeout = 60000)
    public void testSendersCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
        final int NUM_MESSAGES = 5;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Normal Session which won't create an TXN itself
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        // Commit TXN work from a sender.
        txnSession.begin();
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.commit();

        // Rollback an additional batch of TXN work from a sender.
        txnSession.begin();
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.rollback();

        // Commit more TXN work from a sender.
        txnSession.begin();
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.commit();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(NUM_MESSAGES * 2);
        for (int i = 0; i < NUM_MESSAGES * 2; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            message.accept(txnSession);
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiversCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
        final int NUM_MESSAGES = 10;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Normal Session which won't create an TXN itself
        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < NUM_MESSAGES + 1; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
        }

        // Read all messages from the Queue, do not accept them yet.
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        ArrayList<AmqpMessage> messages = new ArrayList<>(NUM_MESSAGES);
        receiver.flow((NUM_MESSAGES + 2) * 2);
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            messages.add(message);
        }

        // Commit half the consumed messages
        txnSession.begin();
        for (int i = 0; i < NUM_MESSAGES / 2; ++i) {
            messages.get(i).accept(txnSession);
        }
        txnSession.commit();

        // Rollback the other half the consumed messages
        txnSession.begin();
        for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            messages.get(i).accept(txnSession);
        }
        txnSession.rollback();

        {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
        }

        // Commit the other half the consumed messages
        // This is a variation from the .NET client tests which doesn't settle the
        // messages in the TX until commit is called but on ActiveMQ they will be
        // redispatched regardless and not stay in the acquired state.
        txnSession.begin();
        for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            message.accept();
        }
        txnSession.commit();

        // The final message should still be pending.
        {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            receiver.flow(1);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
        }

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCommitAndRollbackWithMultipleSessionsInSingleTX() throws Exception {
        final int NUM_MESSAGES = 10;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Normal Session which won't create an TXN itself
        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
        }

        // Read all messages from the Queue, do not accept them yet.
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(2);
        AmqpMessage message1 = receiver.receive(5, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver.receive(5, TimeUnit.SECONDS);

        // Accept the first one in a TXN and send a new message in that TXN as well
        txnSession.begin();
        {
            message1.accept(txnSession);

            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", NUM_MESSAGES);

            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.commit();

        // Accept the second one in a TXN and send a new message in that TXN as well but rollback
        txnSession.begin();
        {
            message2.accept(txnSession);

            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", NUM_MESSAGES + 1);
            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.rollback();

        // Variation here from .NET code, the client settles the accepted message where
        // the .NET client does not and instead releases here to have it redelivered.

        receiver.flow(NUM_MESSAGES);
        for (int i = 1; i <= NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(i, message.getApplicationProperty("msgId"));
            message.accept();
        }

        // Should be nothing left.
        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiversCommitAndRollbackWithMultipleSessionsInSingleTXNoSettlement() throws Exception {
        final int NUM_MESSAGES = 10;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Normal Session which won't create an TXN itself
        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < NUM_MESSAGES + 1; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
        }

        // Read all messages from the Queue, do not accept them yet.
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        ArrayList<AmqpMessage> messages = new ArrayList<>(NUM_MESSAGES);
        receiver.flow((NUM_MESSAGES + 2) * 2);
        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            messages.add(message);
        }

        // Commit half the consumed messages
        txnSession.begin();
        for (int i = 0; i < NUM_MESSAGES / 2; ++i) {
            messages.get(i).accept(txnSession, false);
        }
        txnSession.commit();

        // Rollback the other half the consumed messages
        txnSession.begin();
        for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            messages.get(i).accept(txnSession, false);
        }
        txnSession.rollback();

        // After rollback message should still be acquired so we read last sent message.
        {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.release();
        }

        // Commit the other half the consumed messages
        txnSession.begin();
        for (int i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i) {
            messages.get(i).accept(txnSession);
        }
        txnSession.commit();

        // The final message should still be pending.
        {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            receiver.flow(1);
            assertNotNull(message);
            assertEquals(NUM_MESSAGES, message.getApplicationProperty("msgId"));
            message.accept();
        }

        // We should have now drained the Queue
        AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
        receiver.flow(1);
        assertNull(message);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCommitAndRollbackWithMultipleSessionsInSingleTXNoSettlement() throws Exception {
        final int NUM_MESSAGES = 10;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        // Root TXN session controls all TXN send lifetimes.
        AmqpSession txnSession = connection.createSession();

        // Normal Session which won't create an TXN itself
        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < NUM_MESSAGES; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", i);
            sender.send(message, txnSession.getTransactionId());
        }

        // Read all messages from the Queue, do not accept them yet.
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(2);
        AmqpMessage message1 = receiver.receive(5, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver.receive(5, TimeUnit.SECONDS);

        // Accept the first one in a TXN and send a new message in that TXN as well
        txnSession.begin();
        {
            message1.accept(txnSession, false);

            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", NUM_MESSAGES);

            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.commit();

        // Accept the second one in a TXN and send a new message in that TXN as well but rollback
        txnSession.begin();
        {
            message2.accept(txnSession, false);

            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message");
            message.setApplicationProperty("msgId", NUM_MESSAGES + 1);
            sender.send(message, txnSession.getTransactionId());
        }
        txnSession.rollback();

        message2.release();

        // Should be ten message available for dispatch given that we sent and committed one, and
        // releases another we had previously received.
        receiver.flow(10);
        for (int i = 1; i <= NUM_MESSAGES; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("Expected a message for: " + i, message);
            assertEquals(i, message.getApplicationProperty("msgId"));
            message.accept();
        }

        // Should be nothing left.
        receiver.flow(1);
        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        connection.close();
    }
}
