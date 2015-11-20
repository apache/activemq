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
}
