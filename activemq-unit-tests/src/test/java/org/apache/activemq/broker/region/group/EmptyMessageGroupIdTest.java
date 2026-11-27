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
package org.apache.activemq.broker.region.group;

import static org.junit.Assert.assertNotNull;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * An empty JMSXGroupID ("") should behave like no group at all.
 */
@Category(ParallelTest.class)
public class EmptyMessageGroupIdTest {

    private static final String QUEUE_NAME = "TEST.EMPTY.GROUP";

    private BrokerService broker;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
            "vm://localhost?create=false&jms.prefetchPolicy.queuePrefetch=1");
        connection = factory.createConnection();
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testEmptyGroupIdDoesNotPinMessagesToOneConsumer() throws Exception {
        // Client ack and prefetch=1: each consumer holds one un-acked message,
        // so the second message can only go to consumer2 unless it is pinned.
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQQueue queue = new ActiveMQQueue(QUEUE_NAME);
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageConsumer consumer2 = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < 2; i++) {
            TextMessage message = session.createTextMessage("message " + i);
            message.setStringProperty("JMSXGroupID", "");
            producer.send(message);
        }

        assertNotNull("consumer1 should get a message", consumer1.receive(5000));
        assertNotNull("consumer2 should get a message, messages with an empty group id were pinned to consumer1",
            consumer2.receive(5000));
    }
}
