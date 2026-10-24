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

import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Session;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.SharedConsumerInfo;
import org.apache.activemq.transport.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link SharedTopicConnectionFactory} produces a
 * {@link SharedJMSContext} whose JMS 3.1 simplified-API shared-consumer methods
 * route through to the underlying {@link SharedTopicSession}, emitting a
 * {@link SharedConsumerInfo} to the broker rather than throwing.
 */
public class SharedJMSContextTest {

    private StubTransport transport;
    private SharedTopicConnectionFactory factory;
    private JMSContext context;

    @Before
    public void setUp() {
        transport = new StubTransport();
        factory = new SharedTopicConnectionFactory("tcp://localhost:61616") {
            @Override
            protected Transport createTransport() {
                return transport;
            }
        };
    }

    @After
    public void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void testFactoryCreatesSharedJMSContext() {
        context = factory.createContext();
        assertTrue("Factory should create a SharedJMSContext",
                context instanceof SharedJMSContext);
    }

    @Test
    public void testFactoryCreatesSharedJMSContextForAllOverloads() {
        assertTrue(factory.createContext() instanceof SharedJMSContext);
        assertTrue(factory.createContext(Session.AUTO_ACKNOWLEDGE) instanceof SharedJMSContext);
        // Credential overloads take the same routing path.
        assertTrue(factory.createContext("u", "p") instanceof SharedJMSContext);
        assertTrue(factory.createContext("u", "p", Session.AUTO_ACKNOWLEDGE) instanceof SharedJMSContext);
    }

    @Test
    public void testChildContextStaysShared() {
        context = factory.createContext();
        JMSContext child = context.createContext(Session.AUTO_ACKNOWLEDGE);
        try {
            assertTrue("createContext(int) must return a SharedJMSContext, not a plain one",
                    child instanceof SharedJMSContext);
        } finally {
            child.close();
        }
    }

    @Test
    public void testCreateSharedConsumerEmitsSharedConsumerInfo() {
        context = factory.createContext();
        JMSConsumer consumer = context.createSharedConsumer(new ActiveMQTopic("test.topic"), "mySub");
        assertNotNull(consumer);

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull("createSharedConsumer must reach the broker as a SharedConsumerInfo", sent);
        assertTrue(sent.isShared());
        assertFalse("Non-durable shared consumer", sent.isDurable());
        assertEquals("mySub", sent.getSubscriptionName());
    }

    @Test
    public void testCreateSharedConsumerWithSelector() {
        context = factory.createContext();
        context.createSharedConsumer(new ActiveMQTopic("test.topic"), "mySub", "color = 'red'");

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull(sent);
        assertTrue(sent.isShared());
        assertEquals("color = 'red'", sent.getSelector());
    }

    @Test
    public void testCreateSharedDurableConsumerEmitsDurableSharedConsumerInfo() {
        context = factory.createContext();
        context.setClientID("cts");
        context.createSharedDurableConsumer(new ActiveMQTopic("test.topic"), "myDurableSub");

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull("createSharedDurableConsumer must reach the broker as a SharedConsumerInfo", sent);
        assertTrue(sent.isShared());
        assertTrue("Durable shared consumer", sent.isDurable());
        assertEquals("myDurableSub", sent.getSubscriptionName());
    }

    @Test
    public void testCreateSharedDurableConsumerWithSelector() {
        context = factory.createContext();
        context.setClientID("cts");
        context.createSharedDurableConsumer(new ActiveMQTopic("test.topic"), "myDurableSub", "n > 1");

        SharedConsumerInfo sent = findSharedConsumerInfo();
        assertNotNull(sent);
        assertTrue(sent.isShared());
        assertTrue(sent.isDurable());
        assertEquals("n > 1", sent.getSelector());
    }

    @Test(expected = InvalidDestinationRuntimeException.class)
    public void testCreateSharedConsumerNullTopicThrows() {
        context = factory.createContext();
        context.createSharedConsumer(null, "mySub");
    }

    @Test(expected = InvalidDestinationRuntimeException.class)
    public void testCreateSharedDurableConsumerNullTopicThrows() {
        context = factory.createContext();
        context.createSharedDurableConsumer(null, "mySub");
    }

    private SharedConsumerInfo findSharedConsumerInfo() {
        for (Object cmd : transport.getSent()) {
            if (cmd instanceof SharedConsumerInfo) {
                return (SharedConsumerInfo) cmd;
            }
        }
        return null;
    }
}
