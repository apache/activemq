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
package org.apache.activemq.broker;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleClientsTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class QueueSubscriptionTest extends JmsMultipleClientsTestSupport {
    protected int messageCount = 1000; // 1000 Messages per producer
    protected int prefetchCount = 10;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        durable = false;
        topic = false;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }


    @Test(timeout = 60 * 1000)
    public void testManyProducersOneConsumer() throws Exception {
        consumerCount = 1;
        producerCount = 10;
        messageCount = 100;
        messageSize = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersSmallMessagesOnePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount = 1000;
        messageSize = 1024; // 1 Kb
        configurePrefetchOfOne();

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersSmallMessagesLargePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount = 1000;
        prefetchCount = messageCount * 2;
        messageSize = 1024; // 1 Kb

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testOneProducerTwoConsumersLargeMessagesOnePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount = 10;
        messageSize = 1024 * 1024 * 1; // 2 MB
        configurePrefetchOfOne();

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerTwoConsumersLargeMessagesLargePrefetch() throws Exception {
        consumerCount = 2;
        producerCount = 1;
        messageCount = 10;
        prefetchCount = messageCount * 2;
        messageSize = 1024 * 1024 * 1; // 2 MB

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerManyConsumersFewMessages() throws Exception {
        consumerCount = 50;
        producerCount = 1;
        messageCount = 10;
        messageSize = 1; // 1 byte
        prefetchCount = 10;

        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 60 * 1000)
    public void testOneProducerManyConsumersManyMessages() throws Exception {
        consumerCount = 50;
        producerCount = 1;
        messageCount = 1000;
        messageSize = 1; // 1 byte
        prefetchCount = messageCount / consumerCount;
        allMessagesList.setMaximumDuration(allMessagesList.getMaximumDuration() * 20);
        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    @Test(timeout = 2 * 60 * 1000)
    public void testManyProducersManyConsumers() throws Exception {
        consumerCount = 200;
        producerCount = 50;
        messageCount = 100;
        messageSize = 1; // 1 byte
        prefetchCount = 100;
        allMessagesList.setMaximumDuration(allMessagesList.getMaximumDuration() * 20);
        doMultipleClientsTest();

        assertTotalMessagesReceived(messageCount * producerCount);
        assertDestinationMemoryUsageGoesToZero();
    }

    protected void configurePrefetchOfOne() {
        prefetchCount = 1;

        // this is gonna be a bit slow what with the low prefetch so bump up the
        // wait time
        allMessagesList.setMaximumDuration(allMessagesList.getMaximumDuration() * 20);
    }

    public void doMultipleClientsTest() throws Exception {
        // Create destination
        final ActiveMQDestination dest = createDestination();

        // Create consumers
        ActiveMQConnectionFactory consumerFactory = (ActiveMQConnectionFactory)createConnectionFactory();
        consumerFactory.getPrefetchPolicy().setAll(prefetchCount);

        startConsumers(consumerFactory, dest);

        startProducers(dest, messageCount);

        // Wait for messages to be received. Make it proportional to the
        // messages delivered.
        int totalMessageCount = messageCount * producerCount;
        if (dest.isTopic()) {
            totalMessageCount *= consumerCount;
        }
        waitForAllMessagesToBeReceived(totalMessageCount);
    }
}
