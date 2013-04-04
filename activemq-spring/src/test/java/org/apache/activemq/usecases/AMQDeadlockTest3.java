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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class AMQDeadlockTest3 extends org.apache.activemq.test.TestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQDeadlockTest3.class);

    private static final String URL1 = "tcp://localhost:61616";

    private static final String URL2 = "tcp://localhost:61617";

    private static final String QUEUE1_NAME = "test.queue.1";

    private static final String QUEUE2_NAME = "test.queue.2";

    private static final int MAX_CONSUMERS = 1;

    private static final int MAX_PRODUCERS = 1;

    private static final int NUM_MESSAGE_TO_SEND = 10;

    private final AtomicInteger messageCount = new AtomicInteger();
    private CountDownLatch doneLatch;

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    // This should fail with incubator-activemq-fuse-4.1.0.5
    public void testQueueLimitsWithOneBrokerSameConnection() throws Exception {

        BrokerService brokerService1 = null;
        ActiveMQConnectionFactory acf = null;
        PooledConnectionFactory pcf = null;
        DefaultMessageListenerContainer container1 = null;

        try {
            brokerService1 = createBrokerService("broker1", URL1, null);
            brokerService1.start();

            acf = createConnectionFactory(URL1);
            pcf = new PooledConnectionFactory(acf);

            // Only listen on the first queue.. let the 2nd queue fill up.
            doneLatch = new CountDownLatch(NUM_MESSAGE_TO_SEND);
            container1 = createDefaultMessageListenerContainer(acf, new TestMessageListener1(500), QUEUE1_NAME);
            container1.afterPropertiesSet();

            Thread.sleep(2000);

            final ExecutorService executor = Executors.newCachedThreadPool();
            for (int i = 0; i < MAX_PRODUCERS; i++) {
                executor.submit(new PooledProducerTask(pcf, QUEUE2_NAME));
                Thread.sleep(1000);
                executor.submit(new PooledProducerTask(pcf, QUEUE1_NAME));
            }

            // Wait for all message to arrive.
            assertTrue(doneLatch.await(20, TimeUnit.SECONDS));
            executor.shutdownNow();

            assertEquals(NUM_MESSAGE_TO_SEND, messageCount.get());

        } finally {
            container1.stop();
            container1.destroy();
            container1 = null;
            brokerService1.stop();
            brokerService1 = null;
        }
    }

    // This should fail with incubator-activemq-fuse-4.1.0.5
    public void testQueueLimitsWithTwoBrokerProduceandConsumeonDifferentBrokersWithOneConnectionForProducing() throws Exception {

        BrokerService brokerService1 = null;
        BrokerService brokerService2 = null;
        ActiveMQConnectionFactory acf1 = null;
        ActiveMQConnectionFactory acf2 = null;
        PooledConnectionFactory pcf = null;
        DefaultMessageListenerContainer container1 = null;

        try {
            brokerService1 = createBrokerService("broker1", URL1, URL2);
            brokerService1.start();
            brokerService2 = createBrokerService("broker2", URL2, URL1);
            brokerService2.start();

            acf1 = createConnectionFactory(URL1);
            acf2 = createConnectionFactory(URL2);

            pcf = new PooledConnectionFactory(acf1);

            Thread.sleep(1000);

            doneLatch = new CountDownLatch(MAX_PRODUCERS * NUM_MESSAGE_TO_SEND);
            container1 = createDefaultMessageListenerContainer(acf2, new TestMessageListener1(500), QUEUE1_NAME);
            container1.afterPropertiesSet();

            final ExecutorService executor = Executors.newCachedThreadPool();
            for (int i = 0; i < MAX_PRODUCERS; i++) {
                executor.submit(new PooledProducerTask(pcf, QUEUE2_NAME));
                Thread.sleep(1000);
                executor.submit(new PooledProducerTask(pcf, QUEUE1_NAME));
            }

            assertTrue(doneLatch.await(20, TimeUnit.SECONDS));
            executor.shutdownNow();

            assertEquals(MAX_PRODUCERS * NUM_MESSAGE_TO_SEND, messageCount.get());
        } finally {

            container1.stop();
            container1.destroy();
            container1 = null;

            brokerService1.stop();
            brokerService1 = null;
            brokerService2.stop();
            brokerService2 = null;
        }
    }

    // This should fail with incubator-activemq-fuse-4.1.0.5
    public void testQueueLimitsWithTwoBrokerProduceandConsumeonDifferentBrokersWithSeperateConnectionsForProducing() throws Exception {

        BrokerService brokerService1 = null;
        BrokerService brokerService2 = null;
        ActiveMQConnectionFactory acf1 = null;
        ActiveMQConnectionFactory acf2 = null;
        DefaultMessageListenerContainer container1 = null;
        DefaultMessageListenerContainer container2 = null;

        try {
            brokerService1 = createBrokerService("broker1", URL1, URL2);
            brokerService1.start();
            brokerService2 = createBrokerService("broker2", URL2, URL1);
            brokerService2.start();

            acf1 = createConnectionFactory(URL1);
            acf2 = createConnectionFactory(URL2);

            Thread.sleep(1000);

            doneLatch = new CountDownLatch(NUM_MESSAGE_TO_SEND * MAX_PRODUCERS);

            container1 = createDefaultMessageListenerContainer(acf2, new TestMessageListener1(500), QUEUE1_NAME);
            container1.afterPropertiesSet();
            container2 = createDefaultMessageListenerContainer(acf2, new TestMessageListener1(30000), QUEUE2_NAME);
            container2.afterPropertiesSet();

            final ExecutorService executor = Executors.newCachedThreadPool();
            for (int i = 0; i < MAX_PRODUCERS; i++) {
                executor.submit(new NonPooledProducerTask(acf1, QUEUE2_NAME));
                Thread.sleep(1000);
                executor.submit(new NonPooledProducerTask(acf1, QUEUE1_NAME));
            }

            assertTrue(doneLatch.await(20, TimeUnit.SECONDS));
            executor.shutdownNow();

            assertEquals(MAX_PRODUCERS * NUM_MESSAGE_TO_SEND, messageCount.get());
        } finally {
            container1.stop();
            container1.destroy();
            container1 = null;

            container2.stop();
            container2.destroy();
            container2 = null;

            brokerService1.stop();
            brokerService1 = null;
            brokerService2.stop();
            brokerService2 = null;
        }
    }

    private BrokerService createBrokerService(final String brokerName, final String uri1, final String uri2) throws Exception {
        final BrokerService brokerService = new BrokerService();

        brokerService.setBrokerName(brokerName);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);

        final SystemUsage memoryManager = new SystemUsage();
        memoryManager.getMemoryUsage().setLimit(5000000);
        brokerService.setSystemUsage(memoryManager);

        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();

        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        // entry.setQueue(QUEUE1_NAME);
        entry.setMemoryLimit(1000);
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        brokerService.setDestinationPolicy(policyMap);

        final TransportConnector tConnector = new TransportConnector();
        tConnector.setUri(new URI(uri1));
        tConnector.setName(brokerName + ".transportConnector");
        brokerService.addConnector(tConnector);

        if (uri2 != null) {
            final NetworkConnector nc = new DiscoveryNetworkConnector(new URI("static:" + uri2));
            nc.setBridgeTempDestinations(true);
            nc.setBrokerName(brokerName);
            brokerService.addNetworkConnector(nc);
        }

        return brokerService;
    }

    public DefaultMessageListenerContainer createDefaultMessageListenerContainer(final ConnectionFactory acf, final MessageListener listener, final String queue) {
        final DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(acf);
        container.setDestinationName(queue);
        container.setMessageListener(listener);
        container.setSessionTransacted(false);
        container.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        container.setConcurrentConsumers(MAX_CONSUMERS);
        return container;
    }

    public ActiveMQConnectionFactory createConnectionFactory(final String url) {
        final ActiveMQConnectionFactory acf = new ActiveMQConnectionFactory(url);
        acf.setCopyMessageOnSend(false);
        acf.setUseAsyncSend(false);
        acf.setDispatchAsync(true);
        acf.setUseCompression(false);
        acf.setOptimizeAcknowledge(false);
        acf.setOptimizedMessageDispatch(true);
        acf.setAlwaysSyncSend(true);
        return acf;
    }

    private class TestMessageListener1 implements MessageListener {

        private final long waitTime;

        public TestMessageListener1(long waitTime) {
            this.waitTime = waitTime;
        }

        @Override
        public void onMessage(Message msg) {

            try {
                LOG.info("Listener1 Consumed message " + msg.getIntProperty("count"));
                messageCount.incrementAndGet();
                doneLatch.countDown();
                Thread.sleep(waitTime);
            } catch (JMSException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class PooledProducerTask implements Runnable {

        private final String queueName;

        private final PooledConnectionFactory pcf;

        public PooledProducerTask(final PooledConnectionFactory pcf, final String queueName) {
            this.pcf = pcf;
            this.queueName = queueName;
        }

        @Override
        public void run() {

            try {

                final JmsTemplate jmsTemplate = new JmsTemplate(pcf);
                jmsTemplate.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                jmsTemplate.setExplicitQosEnabled(true);
                jmsTemplate.setMessageIdEnabled(false);
                jmsTemplate.setMessageTimestampEnabled(false);
                jmsTemplate.afterPropertiesSet();

                final byte[] bytes = new byte[2048];
                final Random r = new Random();
                r.nextBytes(bytes);

                Thread.sleep(2000);

                final AtomicInteger count = new AtomicInteger();
                for (int i = 0; i < NUM_MESSAGE_TO_SEND; i++) {
                    jmsTemplate.send(queueName, new MessageCreator() {

                        @Override
                        public Message createMessage(Session session) throws JMSException {

                            final BytesMessage message = session.createBytesMessage();

                            message.writeBytes(bytes);
                            message.setIntProperty("count", count.incrementAndGet());
                            message.setStringProperty("producer", "pooled");
                            return message;
                        }
                    });

                    LOG.info("PooledProducer sent message: " + count.get());
                    // Thread.sleep(1000);
                }

            } catch (final Throwable e) {
                LOG.error("Producer 1 is exiting", e);
            }
        }
    }

    private static class NonPooledProducerTask implements Runnable {

        private final String queueName;

        private final ConnectionFactory cf;

        public NonPooledProducerTask(final ConnectionFactory cf, final String queueName) {
            this.cf = cf;
            this.queueName = queueName;
        }

        @Override
        public void run() {

            try {

                final JmsTemplate jmsTemplate = new JmsTemplate(cf);
                jmsTemplate.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                jmsTemplate.setExplicitQosEnabled(true);
                jmsTemplate.setMessageIdEnabled(false);
                jmsTemplate.setMessageTimestampEnabled(false);
                jmsTemplate.afterPropertiesSet();

                final byte[] bytes = new byte[2048];
                final Random r = new Random();
                r.nextBytes(bytes);

                Thread.sleep(2000);

                final AtomicInteger count = new AtomicInteger();
                for (int i = 0; i < NUM_MESSAGE_TO_SEND; i++) {
                    jmsTemplate.send(queueName, new MessageCreator() {

                        @Override
                        public Message createMessage(Session session) throws JMSException {

                            final BytesMessage message = session.createBytesMessage();

                            message.writeBytes(bytes);
                            message.setIntProperty("count", count.incrementAndGet());
                            message.setStringProperty("producer", "non-pooled");
                            return message;
                        }
                    });

                    LOG.info("Non-PooledProducer sent message: " + count.get());

                    // Thread.sleep(1000);
                }

            } catch (final Throwable e) {
                LOG.error("Producer 1 is exiting", e);
            }
        }
    }
}
