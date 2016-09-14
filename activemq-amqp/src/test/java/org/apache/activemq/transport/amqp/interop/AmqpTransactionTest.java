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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

/**
 * Test various aspects of Transaction support.
 */
public class AmqpTransactionTest extends AmqpClientTestSupport {

    @Test(timeout = 30000)
    public void testBeginAndCommitTransaction() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
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
        AmqpConnection connection = client.connect();
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
        AmqpConnection connection = client.connect();
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

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueueWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
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

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
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

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageWithRollback() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
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

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testMultipleSessionReceiversInSingleTXNWithCommit() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();

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
        AmqpConnection connection = client.connect();

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
        AmqpConnection connection = client.connect();

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
        AmqpConnection connection = client.connect();

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
}
