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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.JMSInteroperabilityTest;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that the AMQP CorrelationId value and type are preserved.
 */
@RunWith(Parameterized.class)
public class AmqpCorrelationIdPreservationTest extends AmqpClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSInteroperabilityTest.class);

    private final String transformer;

    @Parameters(name="Transformer->{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"jms"},
                {"native"},
                {"raw"},
            });
    }

    public AmqpCorrelationIdPreservationTest(String transformer) {
        this.transformer = transformer;
    }

    @Override
    protected String getAmqpTransformer() {
        return transformer;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Test(timeout = 60000)
    public void testStringCorrelationIdIsPreserved() throws Exception {
        doTestCorrelationIdPreservation("msg-id-string:1");
    }

    @Test(timeout = 60000)
    public void testStringCorrelationIdIsPreservedAfterRestart() throws Exception {
        doTestCorrelationIdPreservationOnBrokerRestart("msg-id-string:1");
    }

    @Test(timeout = 60000)
    public void testUUIDCorrelationIdIsPreserved() throws Exception {
        doTestCorrelationIdPreservation(UUID.randomUUID());
    }

    @Test(timeout = 60000)
    public void testUUIDCorrelationIdIsPreservedAfterRestart() throws Exception {
        doTestCorrelationIdPreservationOnBrokerRestart(UUID.randomUUID());
    }

    @Test(timeout = 60000)
    public void testUnsignedLongCorrelationIdIsPreserved() throws Exception {
        doTestCorrelationIdPreservation(new UnsignedLong(255l));
    }

    @Test(timeout = 60000)
    public void testUnsignedLongCorrelationIdIsPreservedAfterRestart() throws Exception {
        doTestCorrelationIdPreservationOnBrokerRestart(new UnsignedLong(255l));
    }

    @Test(timeout = 60000)
    public void testBinaryLongCorrelationIdIsPreserved() throws Exception {
        byte[] payload = new byte[32];
        for (int i = 0; i < 32; ++i) {
            payload[i] = (byte) ('a' + i);
        }

        doTestCorrelationIdPreservation(new Binary(payload));
    }

    @Test(timeout = 60000)
    public void testBinaryLongCorrelationIdIsPreservedAfterRestart() throws Exception {
        byte[] payload = new byte[32];
        for (int i = 0; i < 32; ++i) {
            payload[i] = (byte) ('a' + i);
        }

        doTestCorrelationIdPreservationOnBrokerRestart(new Binary(payload));
    }

    @Test(timeout = 60000)
    public void testStringCorrelationIdPrefixIsPreserved() throws Exception {
        doTestCorrelationIdPreservation("ID:msg-id-string:1");
    }

    public void doTestCorrelationIdPreservation(Object messageId) throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        AmqpMessage message = new AmqpMessage();

        message.setRawCorrelationId(messageId);
        message.setText("Test-Message");

        sender.send(message);

        sender.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(1, queue.getQueueSize());

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("Should have got a message", received);
        assertEquals(received.getRawCorrelationId().getClass(), messageId.getClass());
        assertEquals(messageId, received.getRawCorrelationId());
        receiver.close();
        connection.close();
    }

    public void doTestCorrelationIdPreservationOnBrokerRestart(Object messageId) throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        AmqpMessage message = new AmqpMessage();

        message.setRawCorrelationId(messageId);
        message.setText("Test-Message");
        message.setDurable(true);

        sender.send(message);

        sender.close();
        connection.close();

        restartBroker();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(1, queue.getQueueSize());

        connection = client.connect();
        session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("Should have got a message", received);
        assertEquals(received.getRawCorrelationId().getClass(), messageId.getClass());
        assertEquals(messageId, received.getRawCorrelationId());
        receiver.close();
        connection.close();
    }
}
