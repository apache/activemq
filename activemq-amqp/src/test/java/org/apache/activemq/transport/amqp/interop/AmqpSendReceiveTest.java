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
}
