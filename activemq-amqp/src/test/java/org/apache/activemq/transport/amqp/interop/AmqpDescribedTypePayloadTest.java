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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpNoLocalFilter;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that the broker can pass through an AMQP message with a described type
 * in the message body regardless of transformer in use.
 */
@RunWith(Parameterized.class)
public class AmqpDescribedTypePayloadTest extends AmqpClientTestSupport {

    private final String transformer;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"jms"},
            {"native"},
            {"raw"}
        });
    }

    public AmqpDescribedTypePayloadTest(String transformer) {
        this.transformer = transformer;
    }

    @Override
    protected String getAmqpTransformer() {
        return transformer;
    }

    @Test(timeout = 60000)
    public void testSendMessageWithDescribedTypeInBody() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();
        message.setDescribedType(new AmqpNoLocalFilter());
        sender.send(message);
        sender.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(1, queue.getQueueSize());

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        assertNotNull(received.getDescribedType());
        receiver.close();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageWithDescribedTypeInBodyReceiveOverOpenWire() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();
        message.setDescribedType(new AmqpNoLocalFilter());
        sender.send(message);
        sender.close();
        connection.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(1, queue.getQueueSize());

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        Connection jmsConnection = factory.createConnection();
        Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(getTestName());
        MessageConsumer jmsConsumer = jmsSession.createConsumer(destination);
        jmsConnection.start();

        Message received = jmsConsumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof BytesMessage);
        jmsConnection.close();
    }

    @Test(timeout = 60000)
    public void testDescribedTypeMessageRoundTrips() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        // Send with AMQP client.
        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();
        message.setDescribedType(new AmqpNoLocalFilter());
        sender.send(message);
        sender.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(1, queue.getQueueSize());

        // Receive and resend with OpenWire JMS client
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        Connection jmsConnection = factory.createConnection();
        Session jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = jmsSession.createQueue(getTestName());
        MessageConsumer jmsConsumer = jmsSession.createConsumer(destination);
        jmsConnection.start();

        Message received = jmsConsumer.receive(5000);
        assertNotNull(received);
        assertTrue(received instanceof BytesMessage);

        MessageProducer jmsProducer = jmsSession.createProducer(destination);
        jmsProducer.send(received);
        jmsConnection.close();

        assertEquals(1, queue.getQueueSize());

        // Now lets receive it with AMQP and see that we get back what we expected.
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(1);
        AmqpMessage returned = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(returned);
        assertNotNull(returned.getDescribedType());
        receiver.close();
        connection.close();
    }
}
