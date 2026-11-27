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
package org.apache.activemq;

import static org.junit.Assert.*;

import java.util.Queue;

import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SharedConsumerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SharedTopicSessionTest {

    private Connection connection;
    private SharedTopicSession session;
    private StubTransport transport;

    @Before
    public void setUp() throws Exception {
        transport = new StubTransport();
        SharedTopicConnectionFactory factory =
                new SharedTopicConnectionFactory("tcp://localhost:61616") {
                    @Override
                    protected org.apache.activemq.transport.Transport createTransport() {
                        return transport;
                    }
                };
        connection = factory.createConnection();
        session = (SharedTopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testIsInstanceOfActiveMQSession() {
        assertTrue(session instanceof ActiveMQSession);
    }

    @Test
    public void testCreateSharedConsumer() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        MessageConsumer consumer = session.createSharedConsumer(topic, "mySub");
        assertNotNull(consumer);

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull("Should have sent SharedConsumerInfo to broker", sent);
        assertTrue(sent.isShared());
        assertFalse("Non-durable shared consumer", sent.isDurable());
        assertEquals("mySub", sent.getSubscriptionName());
    }

    @Test
    public void testCreateSharedConsumerWithSelector() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        MessageConsumer consumer = session.createSharedConsumer(topic, "mySub", "color = 'blue'");
        assertNotNull(consumer);

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertTrue(sent.isShared());
        assertFalse(sent.isDurable());
        assertEquals("color = 'blue'", sent.getSelector());
    }

    @Test(expected = InvalidDestinationException.class)
    public void testCreateSharedConsumerNullTopic() throws Exception {
        session.createSharedConsumer(null, "mySub");
    }

    @Test(expected = JMSException.class)
    public void testCreateSharedConsumerNullName() throws Exception {
        session.createSharedConsumer(new ActiveMQTopic("t"), null);
    }

    @Test(expected = JMSException.class)
    public void testCreateSharedConsumerEmptyName() throws Exception {
        session.createSharedConsumer(new ActiveMQTopic("t"), "");
    }

    @Test
    public void testCreateSharedDurableConsumer() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "durSub");
        assertNotNull(consumer);

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull("Should have sent SharedConsumerInfo to broker", sent);
        assertTrue(sent.isShared());
        assertTrue("Should be durable", sent.isDurable());
        assertEquals("durSub", sent.getSubscriptionName());
    }

    @Test
    public void testCreateSharedDurableConsumerWithSelector() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        MessageConsumer consumer = session.createSharedDurableConsumer(topic, "durSub", "price > 10");
        assertNotNull(consumer);

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertTrue(sent.isShared());
        assertTrue(sent.isDurable());
        assertEquals("price > 10", sent.getSelector());
    }

    @Test(expected = InvalidDestinationException.class)
    public void testCreateSharedDurableConsumerNullTopic() throws Exception {
        session.createSharedDurableConsumer(null, "durSub");
    }

    @Test(expected = JMSException.class)
    public void testCreateSharedDurableConsumerNullName() throws Exception {
        session.createSharedDurableConsumer(new ActiveMQTopic("t"), null);
    }

    @Test
    public void testNonSharedConsumerInfoPassedThrough() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        session.createConsumer(topic);

        SharedConsumerInfo shared = findSharedConsumerInfo();
        assertNull("Regular createConsumer should NOT produce SharedConsumerInfo", shared);

        ConsumerInfo regular = findConsumerInfo();
        assertNotNull("Regular ConsumerInfo should be sent", regular);
    }

    @Test
    public void testToSharedConsumerInfoCopiesFields() {
        ConsumerInfo original = new ConsumerInfo();
        original.setSubscriptionName("sub1");
        original.setPrefetchSize(50);
        original.setSelector("x = 1");

        SharedConsumerInfo result = SharedTopicSession.toSharedConsumerInfo(original, true);
        assertTrue(result.isShared());
        assertTrue(result.isDurable());
        assertEquals("sub1", result.getSubscriptionName());
        assertEquals(50, result.getPrefetchSize());
        assertEquals("x = 1", result.getSelector());
    }

    @Test
    public void testToSharedConsumerInfoNonDurable() {
        ConsumerInfo original = new ConsumerInfo();
        SharedConsumerInfo result = SharedTopicSession.toSharedConsumerInfo(original, false);
        assertTrue(result.isShared());
        assertFalse(result.isDurable());
    }

    private SharedConsumerInfo findSharedConsumerInfo() {
        for (Object cmd : transport.getSent()) {
            if (cmd instanceof SharedConsumerInfo) {
                return (SharedConsumerInfo) cmd;
            }
        }
        return null;
    }

    private ConsumerInfo findConsumerInfo() {
        for (Object cmd : transport.getSent()) {
            if (cmd instanceof ConsumerInfo && !(cmd instanceof SharedConsumerInfo)) {
                return (ConsumerInfo) cmd;
            }
        }
        return null;
    }
}
