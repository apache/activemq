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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizedAckScheduledAckStallTest {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizedAckScheduledAckStallTest.class);

    private static final int PREFETCH = 1000;
    private static final long SCHEDULED_ACK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long SYNC_ACK_PROCESSING_DELAY_MS = 500;
    private static final long RUN_TIME_MS = SCHEDULED_ACK_INTERVAL_MS * 3;

    private BrokerService broker;
    private ActiveMQConnection consumerConnection;
    private ActiveMQConnection producerConnection;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setPlugins(new BrokerPlugin[] { b -> new BrokerPluginSupport() {
            {
                setNext(b);
            }

            @Override
            public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                if (ack.isResponseRequired()) {
                    Thread.sleep(SYNC_ACK_PROCESSING_DELAY_MS);
                }
                super.acknowledge(consumerExchange, ack);
            }
        } });
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        running.set(false);
        if (producerConnection != null) {
            producerConnection.close();
        }
        if (consumerConnection != null) {
            consumerConnection.close();
        }
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testScheduledAckDoesNotAckMessageStillInListener() throws Exception {
        String uri = broker.getTransportConnectors().get(0).getPublishableConnectString();
        long scheduledAckIntervalMs = 1000;

        ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory(uri);
        consumerFactory.setOptimizeAcknowledge(true);
        consumerFactory.setOptimizedAckScheduledAckInterval(scheduledAckIntervalMs);
        consumerFactory.getPrefetchPolicy().setTopicPrefetch(PREFETCH);
        consumerConnection = (ActiveMQConnection) consumerFactory.createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = consumerSession.createTopic("optimized.ack.in.listener");
        MessageConsumer consumer = consumerSession.createConsumer(topic);

        CountDownLatch inListener = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        consumer.setMessageListener(message -> {
            inListener.countDown();
            try {
                releaseListener.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        producerConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(uri).createConnection();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(producerSession.createTextMessage("payload"));

        assertTrue(inListener.await(10, TimeUnit.SECONDS));
        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        try {
            Thread.sleep(scheduledAckIntervalMs * 3);
            assertEquals("message acked while its listener was still running",
                    1, regionBroker.getDestinationStatistics().getInflight().getCount());
        } finally {
            releaseListener.countDown();
        }
        assertTrue("message acked after its listener returned",
                Wait.waitFor(() -> regionBroker.getDestinationStatistics().getInflight().getCount() == 0));
    }

    @Test(timeout = 120000)
    public void testConsumerDoesNotStallWhileScheduledAckIsInFlight() throws Exception {
        String uri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory(uri);
        consumerFactory.setOptimizeAcknowledge(true);
        consumerFactory.setOptimizedAckScheduledAckInterval(SCHEDULED_ACK_INTERVAL_MS);
        consumerFactory.getPrefetchPolicy().setTopicPrefetch(PREFETCH);
        consumerConnection = (ActiveMQConnection) consumerFactory.createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = consumerSession.createTopic("optimized.ack.stall");
        MessageConsumer consumer = consumerSession.createConsumer(topic);

        AtomicLong received = new AtomicLong();
        AtomicLong lastReceivedAt = new AtomicLong(System.currentTimeMillis());
        consumer.setMessageListener(message -> {
            received.incrementAndGet();
            lastReceivedAt.set(System.currentTimeMillis());
        });

        producerConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(uri).createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        Thread producerThread = new Thread(() -> {
            try {
                while (running.get()) {
                    producer.send(producerSession.createTextMessage("payload"));
                }
            } catch (Exception e) {
                if (running.get()) {
                    LOG.error("producer failed", e);
                }
            }
        }, "optimized-ack-stall-producer");
        producerThread.setDaemon(true);
        producerThread.start();

        long maxGapMs = 0;
        long end = System.currentTimeMillis() + RUN_TIME_MS;
        while (System.currentTimeMillis() < end) {
            Thread.sleep(50);
            maxGapMs = Math.max(maxGapMs, System.currentTimeMillis() - lastReceivedAt.get());
        }
        running.set(false);
        producerThread.join(TimeUnit.SECONDS.toMillis(10));

        LOG.info("received {} messages, longest gap between messages {} ms", received.get(), maxGapMs);
        assertTrue("consumer stalled for " + maxGapMs + " ms with a scheduled ack interval of " + SCHEDULED_ACK_INTERVAL_MS + " ms",
                maxGapMs < SCHEDULED_ACK_INTERVAL_MS / 2);
    }
}
