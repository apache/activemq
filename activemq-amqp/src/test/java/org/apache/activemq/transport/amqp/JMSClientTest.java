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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.objectweb.jtests.jms.framework.TestConfig;

public class JMSClientTest extends AmqpTestSupport {

    @Rule public TestName name = new TestName();

    @SuppressWarnings("rawtypes")
    @Test
    public void testProducerConsume() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://" + name);

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
            }

            MessageConsumer consumer = session.createConsumer(queue);
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
        }
        connection.close();
    }

    @Test
    public void testTransactedConsumer() throws Exception {
        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://" + name);
        final int msgCount = 1;

        Connection connection = createConnection();
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);

        Message msg = consumer.receive(TestConfig.TIMEOUT);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);

        LOG.info("Queue size before session commit is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        session.commit();

        LOG.info("Queue size after session commit is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test
    public void testRollbackRececeivedMessage() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://" + name);
        final int msgCount = 1;

        Connection connection = createConnection();
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);

        // Receive and roll back, first receive should not show redelivered.
        Message msg = consumer.receive(TestConfig.TIMEOUT);
        LOG.info("Test received msg: {}", msg);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals(false, msg.getJMSRedelivered());

        session.rollback();

        // Receive and roll back, first receive should not show redelivered.
        msg = consumer.receive(TestConfig.TIMEOUT);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        assertEquals(true, msg.getJMSRedelivered());

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        session.commit();

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());

        session.close();
        connection.close();
    }

    @Test
    public void testTXConsumerAndLargeNumberOfMessages() throws Exception {

        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://" + name);
        final int msgCount = 500;

        Connection connection = createConnection();
        sendMessages(connection, queue, msgCount);

        QueueViewMBean queueView = getProxyToQueue(name.toString());
        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(msgCount, queueView.getQueueSize());

        // Consumer all in TX and commit.
        {
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < msgCount; ++i) {
                if ((i % 100) == 0) {
                    LOG.info("Attempting receive of Message #{}", i);
                }
                Message msg = consumer.receive(TestConfig.TIMEOUT);
                assertNotNull("Should receive message: " + i, msg);
                assertTrue(msg instanceof TextMessage);
            }

            session.commit();
            consumer.close();
            session.close();
        }

        connection.close();

        LOG.info("Queue size after produce is: {}", queueView.getQueueSize());
        assertEquals(0, queueView.getQueueSize());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSelectors() throws Exception{
        ActiveMQAdmin.enableJMSFrameTracing();
        QueueImpl queue = new QueueImpl("queue://" + name);

        Connection connection = createConnection();
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer p = session.createProducer(queue);

            TextMessage message = session.createTextMessage();
            message.setText("hello");
            p.send(message, DeliveryMode.PERSISTENT, 5, 0);

            message = session.createTextMessage();
            message.setText("hello + 9");
            p.send(message, DeliveryMode.PERSISTENT, 9, 0);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            int count = 0;
            while (enumeration.hasMoreElements()) {
                Message m = (Message) enumeration.nextElement();
                assertTrue(m instanceof TextMessage);
                count ++;
            }

            assertEquals(2, count);

            MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
            Message msg = consumer.receive(TestConfig.TIMEOUT);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("hello + 9", ((TextMessage) msg).getText());
        }
        connection.close();
    }

    private Connection createConnection() throws JMSException {
        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
        final Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });
        connection.start();
        return connection;
    }
}
