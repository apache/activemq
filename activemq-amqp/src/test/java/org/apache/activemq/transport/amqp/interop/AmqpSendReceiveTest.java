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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
 * Test basic send and receive scenarios using only AMQP sender and receiver links.
 */
public class AmqpSendReceiveTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testCloseBusyReceiver() throws Exception {
        final int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();

            message.setMessageId("msg" + i);
            message.setMessageAnnotation("serialNo", i);
            message.setText("Test-Message");

            sender.send(message);
        }

        sender.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(20, queue.getQueueSize());

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());
        receiver1.flow(MSG_COUNT);
        AmqpMessage received = receiver1.receive(5, TimeUnit.SECONDS);
        assertEquals("msg0", received.getMessageId());
        receiver1.close();

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());
        receiver2.flow(200);
        for (int i = 0; i < MSG_COUNT; ++i) {
            received = receiver2.receive(5, TimeUnit.SECONDS);
            assertEquals("msg" + i, received.getMessageId());
        }

        receiver2.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveWithJMSSelectorFilter() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpMessage message1 = new AmqpMessage();
        message1.setGroupId("abcdefg");
        message1.setApplicationProperty("sn", 100);

        AmqpMessage message2 = new AmqpMessage();
        message2.setGroupId("hijklm");
        message2.setApplicationProperty("sn", 200);

        AmqpSender sender = session.createSender("queue://" + getTestName());
        sender.send(message1);
        sender.send(message2);
        sender.close();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName(), "sn = 100");
        receiver.flow(2);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        assertEquals(100, received.getApplicationProperty("sn"));
        assertEquals("abcdefg", received.getGroupId());
        received.accept();

        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        receiver.close();
    }

    @Test(timeout = 30000)
    public void testAdvancedLinkFlowControl() throws Exception {
        final int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();

            message.setMessageId("msg" + i);
            message.setMessageAnnotation("serialNo", i);
            message.setText("Test-Message");

            sender.send(message);
        }

        sender.close();

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());
        receiver1.flow(2);
        AmqpMessage message1 = receiver1.receive(5, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver1.receive(5, TimeUnit.SECONDS);
        assertEquals("msg0", message1.getMessageId());
        assertEquals("msg1", message2.getMessageId());
        message1.accept();
        message2.accept();

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());
        receiver2.flow(2);
        AmqpMessage message3 = receiver2.receive(5, TimeUnit.SECONDS);
        AmqpMessage message4 = receiver2.receive(5, TimeUnit.SECONDS);
        assertEquals("msg2", message3.getMessageId());
        assertEquals("msg3", message4.getMessageId());
        message3.accept();
        message4.accept();

        receiver1.flow(MSG_COUNT - 4);
        for (int i = 4; i < MSG_COUNT - 4; i++) {
            AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
            assertEquals("msg" + i, message.getMessageId());
            message.accept();
        }

        receiver1.close();
        receiver2.close();
    }

    @Test(timeout = 60000)
    public void testMessageDurabliltyFollowsSpec() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = client.connect();
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        QueueViewMBean queue = getProxyToQueue(getTestName());

        // Create default message that should be sent as non-durable
        AmqpMessage message1 = new AmqpMessage();
        message1.setText("Test-Message -> non-durable");
        message1.setDurable(false);
        message1.setMessageId("ID:Message:1");
        sender.send(message1);

        assertEquals(1, queue.getQueueSize());
        receiver1.flow(1);
        message1 = receiver1.receive(50, TimeUnit.SECONDS);
        assertFalse("First message sent should not be durable", message1.isDurable());
        message1.accept();

        // Create default message that should be sent as non-durable
        AmqpMessage message2 = new AmqpMessage();
        message2.setText("Test-Message -> durable");
        message2.setDurable(true);
        message2.setMessageId("ID:Message:2");
        sender.send(message2);

        assertEquals(1, queue.getQueueSize());
        receiver1.flow(1);
        message2 = receiver1.receive(50, TimeUnit.SECONDS);
        assertTrue("Second message sent should be durable", message2.isDurable());
        message2.accept();

        sender.close();
        connection.close();
    }
}
