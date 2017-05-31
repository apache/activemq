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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

/**
 * Test for support of Anonymous sender links.
 */
public class AmqpAnonymousSenderTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testSendMessageOnAnonymousRelayLinkUsingMessageTo() throws Exception {

        AmqpClient client = createAmqpClient();
        client.setTraceFrames(false);

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender();

        assertEquals(1, getProxyToBroker().getDynamicDestinationProducers().length);

        AmqpMessage message = new AmqpMessage();

        message.setAddress("queue://" + getTestName());
        message.setMessageId("msg" + 1);
        message.setMessageAnnotation("serialNo", 1);
        message.setText("Test-Message");

        sender.send(message);
        sender.close();

        LOG.info("Attempting to read message with receiver");
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(1);
        AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message", received);
        assertEquals("msg1", received.getMessageId());
        received.accept();

        receiver.close();

        connection.close();
    }
}
