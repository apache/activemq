/*
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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

/**
 * Tests various behaviors of broker side drain support.
 */
public class AmqpReceiverDrainTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testReceiverCanDrainMessagesQueue() throws Exception {
        doTestReceiverCanDrainMessages(false);
    }

    @Test(timeout = 60000)
    public void testReceiverCanDrainMessagesTopic() throws Exception {
        doTestReceiverCanDrainMessages(true);
    }

    private void doTestReceiverCanDrainMessages(boolean topic) throws Exception {
        final String destinationName;
        if (topic) {
            destinationName = "topic://" + getTestName();
        } else {
            destinationName = "queue://" + getTestName();
        }

        int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(destinationName);

        sendMessages(getTestName(), MSG_COUNT, topic);

        final DestinationViewMBean destinationView;
        if (topic) {
            destinationView = getProxyToTopic(getTestName());
        } else {
            destinationView = getProxyToQueue(getTestName());
        }

        assertEquals(MSG_COUNT, destinationView.getEnqueueCount());
        assertEquals(0, destinationView.getDispatchCount());

        receiver.drain(MSG_COUNT);
        for (int i = 0; i < MSG_COUNT; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("Failed to read message: " + (i + 1), message);
            LOG.info("Read message: {}", message.getMessageId());
            message.accept();
        }
        receiver.close();

        assertEquals(MSG_COUNT, destinationView.getDequeueCount());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testPullWithNoMessageGetDrainedQueue() throws Exception {
        doTestPullWithNoMessageGetDrained(false);
    }

    @Test(timeout = 60000)
    public void testPullWithNoMessageGetDrainedTopic() throws Exception {
        doTestPullWithNoMessageGetDrained(true);
    }

    private void doTestPullWithNoMessageGetDrained(boolean topic) throws Exception {

        final String destinationName;
        if (topic) {
            destinationName = "topic://" + getTestName();
        } else {
            destinationName = "queue://" + getTestName();
        }

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(destinationName);

        receiver.flow(10);

        final DestinationViewMBean destinationView;
        if (topic) {
            destinationView = getProxyToTopic(getTestName());
        } else {
            destinationView = getProxyToQueue(getTestName());
        }

        assertEquals(0, destinationView.getEnqueueCount());
        assertEquals(0, destinationView.getDispatchCount());

        assertEquals(10, receiver.getReceiver().getRemoteCredit());

        assertNull(receiver.pull(1, TimeUnit.SECONDS));

        assertEquals(0, receiver.getReceiver().getRemoteCredit());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testPullOneFromRemoteQueue() throws Exception {
        doTestPullOneFromRemote(false);
    }

    @Test(timeout = 60000)
    public void testPullOneFromRemoteTopic() throws Exception {
        doTestPullOneFromRemote(true);
    }

    private void doTestPullOneFromRemote(boolean topic) throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final String destinationName;
        if (topic) {
            destinationName = "topic://" + getTestName();
        } else {
            destinationName = "queue://" + getTestName();
        }

        AmqpReceiver receiver = session.createReceiver(destinationName);

        int MSG_COUNT = 20;
        sendMessages(getTestName(), MSG_COUNT, topic);

        final DestinationViewMBean destinationView;
        if (topic) {
            destinationView = getProxyToTopic(getTestName());
        } else {
            destinationView = getProxyToQueue(getTestName());
        }

        assertEquals(MSG_COUNT, destinationView.getEnqueueCount());
        assertEquals(0, destinationView.getDispatchCount());

        assertEquals(0, receiver.getReceiver().getRemoteCredit());

        AmqpMessage message = receiver.pull(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();

        assertEquals(0, receiver.getReceiver().getRemoteCredit());

        receiver.close();

        assertEquals(MSG_COUNT - 1, destinationView.getEnqueueCount() - destinationView.getDequeueCount());
        assertEquals(1, destinationView.getDispatchCount());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testMultipleZeroResultPullsQueue() throws Exception {
        doTestMultipleZeroResultPulls(false);
    }

    @Test(timeout = 60000)
    public void testMultipleZeroResultPullsTopic() throws Exception {
        doTestMultipleZeroResultPulls(true);
    }

    private void doTestMultipleZeroResultPulls(boolean topic) throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final String destinationName;
        if (topic) {
            destinationName = "topic://" + getTestName();
        } else {
            destinationName = "queue://" + getTestName();
        }

        AmqpReceiver receiver = session.createReceiver(destinationName);

        receiver.flow(10);

        if (topic) {
            TopicViewMBean topicView = getProxyToTopic(getTestName());
            assertEquals(0, topicView.getEnqueueCount());
            assertEquals(0, topicView.getDispatchCount());
        } else {
            QueueViewMBean queueView = getProxyToQueue(getTestName());
            assertEquals(0, queueView.getQueueSize());
            assertEquals(0, queueView.getDispatchCount());
        }

        assertEquals(10, receiver.getReceiver().getRemoteCredit());

        assertNull(receiver.pull(1, TimeUnit.SECONDS));

        assertEquals(0, receiver.getReceiver().getRemoteCredit());

        assertNull(receiver.pull(1, TimeUnit.SECONDS));
        assertNull(receiver.pull(1, TimeUnit.SECONDS));

        assertEquals(0, receiver.getReceiver().getRemoteCredit());

        connection.close();
    }

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }
}
