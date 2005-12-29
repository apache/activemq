/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import org.apache.activemq.JmsMultipleClientsTestSupport;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;

public class QueueSubscriptionTest extends JmsMultipleClientsTestSupport {
    protected int messageCount  = 1000; // 1000 Messages per producer
    protected int prefetchCount = 10;

    protected void setUp() throws Exception {
        super.setUp();
        durable = false;
        topic = false;
    }

    public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount  = 1000;
        prefetchCount = 1;
        messageSize   = 1024; // 1 Kb

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount  = 1000;
        prefetchCount = messageCount * 2;
        messageSize   = 1024; // 1 Kb

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount  = 10;
        prefetchCount = 1;
        messageSize   = 1024 * 1024 * 1; // 2 MB

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount  = 10;
        prefetchCount = messageCount * 2;
        messageSize   = 1024 * 1024 * 1; // 2 MB

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testOneProducerManyConsumersFewMessages() throws Exception {
        consumerCount = 50;
        producerCount = 1;
        messageCount  = 10;
        messageSize   = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testOneProducerManyConsumersManyMessages() throws Exception {
        consumerCount = 50;
        producerCount = 1;
        messageCount  = 1000;
        messageSize   = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testManyProducersOneConsumer() throws Exception {
        consumerCount = 1;
        producerCount = 50;
        messageCount  = 100;
        messageSize   = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void testManyProducersManyConsumers() throws Exception {
        consumerCount = 50;
        producerCount = 50;
        messageCount  = 100;
        messageSize   = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
    }

    public void doMultipleClientsTest() throws Exception {
        // Create destination
        final ActiveMQDestination dest = createDestination();

        // Create consumers
        ActiveMQConnectionFactory consumerFactory = (ActiveMQConnectionFactory)createConnectionFactory();
        consumerFactory.getPrefetchPolicy().setAll(prefetchCount);

        startConsumers(consumerFactory, dest);

        // Wait for consumers to setup
        Thread.sleep(500);

        startProducers(dest, messageCount);

        // Wait for messages to be received. Make it proportional to the messages delivered.
        waitForAllMessagesToBeReceived((producerCount * messageCount) / 2000);
    }
}
