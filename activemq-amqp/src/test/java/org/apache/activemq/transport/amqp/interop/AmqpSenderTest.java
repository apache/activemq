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

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.util.Wait;
import org.junit.Test;

/**
 * Test broker behavior when creating AMQP senders
 */
public class AmqpSenderTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testCreateQueueSender() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpSender sender = session.createSender("queue://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);
        sender.close();
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicSender() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getTopics().length);

        AmqpSender sender = session.createSender("topic://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getTopics().length);
        assertNotNull(getProxyToTopic(getTestName()));
        assertEquals(1, brokerService.getAdminView().getTopicProducers().length);
        sender.close();
        assertEquals(0, brokerService.getAdminView().getTopicProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueue() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();

        message.setText("Test-Message");

        sender.send(message);

        QueueViewMBean queue = getProxyToQueue(getTestName());

        assertEquals(1, queue.getQueueSize());

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMultipleMessagesToQueue() throws Exception {
        final int MSG_COUNT = 100;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);
        }

        QueueViewMBean queue = getProxyToQueue(getTestName());

        assertEquals(MSG_COUNT, queue.getQueueSize());

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testUnsettledSender() throws Exception {
        final int MSG_COUNT = 1000;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("topic://" + getTestName(), false);

        for (int i = 1; i <= MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);

            if (i % 1000 == 0) {
                LOG.info("Sent message: {}", i);
            }
        }

        final TopicViewMBean topic = getProxyToTopic(getTestName());
        assertTrue("All messages should arrive", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return topic.getEnqueueCount() == MSG_COUNT;
            }
        }));

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testPresettledSender() throws Exception {
        final int MSG_COUNT = 1000;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("topic://" + getTestName(), true);

        for (int i = 1; i <= MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);

            if (i % 1000 == 0) {
                LOG.info("Sent message: {}", i);
            }
        }

        final TopicViewMBean topic = getProxyToTopic(getTestName());
        assertTrue("All messages should arrive", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return topic.getEnqueueCount() == MSG_COUNT;
            }
        }));

        sender.close();
        connection.close();
    }
}