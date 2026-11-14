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

import static org.junit.Assert.assertThrows;

import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.transport.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The shared consumer methods are not supported, but a bad argument is still
 * reported as such before the method says so.
 */
public class ActiveMQSessionSharedConsumerArgumentTest {

    private static final String BAD_SELECTOR = "color = = 'red'";

    private Connection connection;
    private Session session;
    private Topic topic;

    @Before
    public void setUp() throws Exception {
        final var transport = new StubTransport();
        final var factory = new ActiveMQConnectionFactory("tcp://localhost:61616") {
            @Override
            protected Transport createTransport() {
                return transport;
            }
        };
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic("TEST.TOPIC");
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testNullTopicIsInvalidDestination() {
        assertThrows(InvalidDestinationException.class, () -> session.createSharedConsumer(null, "sub"));
        assertThrows(InvalidDestinationException.class, () -> session.createSharedConsumer(null, "sub", null));
        assertThrows(InvalidDestinationException.class, () -> session.createSharedDurableConsumer(null, "sub"));
        assertThrows(InvalidDestinationException.class, () -> session.createSharedDurableConsumer(null, "sub", null));
    }

    @Test
    public void testBadSelectorIsInvalidSelector() {
        assertThrows(InvalidSelectorException.class,
            () -> session.createSharedConsumer(topic, "sub", BAD_SELECTOR));
        assertThrows(InvalidSelectorException.class,
            () -> session.createSharedDurableConsumer(topic, "sub", BAD_SELECTOR));
    }

    @Test
    public void testValidArgumentsAreUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> session.createSharedConsumer(topic, "sub"));
        assertThrows(UnsupportedOperationException.class, () -> session.createSharedConsumer(topic, "sub", "color = 'red'"));
        assertThrows(UnsupportedOperationException.class, () -> session.createSharedDurableConsumer(topic, "sub"));
        assertThrows(UnsupportedOperationException.class,
            () -> session.createSharedDurableConsumer(topic, "sub", "color = 'red'"));
    }
}
