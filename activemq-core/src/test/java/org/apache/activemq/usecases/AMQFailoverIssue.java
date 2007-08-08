/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.usecases;

import java.net.URI;
import java.util.ArrayList;
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
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

public class AMQFailoverIssue extends TestCase {

    private static final String URL1 = "tcp://localhost:61616";
    private static final String QUEUE1_NAME = "test.queue.1";
    private static final int MAX_CONSUMERS = 10;
    private static final int MAX_PRODUCERS = 5;
    private static final int NUM_MESSAGE_TO_SEND = 10000;
    private static final int TOTAL_MESSAGES = MAX_PRODUCERS * NUM_MESSAGE_TO_SEND;
    private static final boolean USE_FAILOVER = true;
    private AtomicInteger messageCount = new AtomicInteger();
    private CountDownLatch doneLatch;

    @Override
    public void setUp() throws Exception {
    }

    @Override
    public void tearDown() throws Exception {
    }

    // This should fail with incubator-activemq-fuse-4.1.0.5
    public void testFailoverIssue() throws Exception {
        BrokerService brokerService1 = null;
        ActiveMQConnectionFactory acf;
        PooledConnectionFactory pcf;
        DefaultMessageListenerContainer container1 = null;
        try {
            brokerService1 = createBrokerService("broker1", URL1, null);
            brokerService1.start();
            acf = createConnectionFactory(URL1, USE_FAILOVER);
            pcf = new PooledConnectionFactory(acf);
            // Only listen on the first queue.. let the 2nd queue fill up.
            doneLatch = new CountDownLatch(TOTAL_MESSAGES);
            container1 = createDefaultMessageListenerContainer(acf, new TestMessageListener1(0), QUEUE1_NAME);
            container1.afterPropertiesSet();
            Thread.sleep(5000);
            final ExecutorService executor = Executors.newCachedThreadPool();
            for (int i = 0; i < MAX_PRODUCERS; i++) {
                executor.submit(new PooledProducerTask(pcf, QUEUE1_NAME));
            }
            // Wait for all message to arrive.
            assertTrue(doneLatch.await(45, TimeUnit.SECONDS));
            executor.shutdown();
            // Thread.sleep(30000);
            Assert.assertEquals(TOTAL_MESSAGES, messageCount.get());
        } finally {
            container1.stop();
            container1.destroy();
            container1 = null;
            brokerService1.stop();
            brokerService1 = null;
        }
    }

    private BrokerService createBrokerService(final String brokerName, final String uri1, final String uri2) throws Exception {
        final BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName(brokerName);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        final UsageManager memoryManager = new UsageManager();
        memoryManager.setLimit(5000000);
        brokerService.setMemoryManager(memoryManager);
        final ArrayList<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        // entry.setQueue(QUEUE1_NAME);
        entry.setMemoryLimit(1);
        policyEntries.add(entry);
        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        brokerService.setDestinationPolicy(policyMap);
        final TransportConnector tConnector = new TransportConnector();
        tConnector.setUri(new URI(uri1));
        tConnector.setBrokerName(brokerName);
        tConnector.setName(brokerName + ".transportConnector");
        brokerService.addConnector(tConnector);
        if (uri2 != null) {
            final NetworkConnector nc = new DiscoveryNetworkConnector(new URI("static:" + uri2));
            nc.setBridgeTempDestinations(true);
            nc.setBrokerName(brokerName);
            nc.setPrefetchSize(1);
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

    public ActiveMQConnectionFactory createConnectionFactory(final String url, final boolean useFailover) {
        final String failoverUrl = "failover:(" + url + ")";
        final ActiveMQConnectionFactory acf = new ActiveMQConnectionFactory(useFailover ? failoverUrl : url);
        acf.setCopyMessageOnSend(false);
        acf.setUseAsyncSend(false);
        acf.setDispatchAsync(true);
        acf.setUseCompression(false);
        acf.setOptimizeAcknowledge(false);
        acf.setOptimizedMessageDispatch(true);
        acf.setUseAsyncSend(false);
        return acf;
    }

    private class TestMessageListener1 implements MessageListener {

        private final long waitTime;

        public TestMessageListener1(long waitTime) {
            this.waitTime = waitTime;
        }

        public void onMessage(Message msg) {
            try {
                messageCount.incrementAndGet();
                doneLatch.countDown();
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
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

                        public Message createMessage(Session session) throws JMSException {
                            final BytesMessage message = session.createBytesMessage();
                            message.writeBytes(bytes);
                            message.setIntProperty("count", count.incrementAndGet());
                            message.setStringProperty("producer", "pooled");
                            return message;
                        }
                    });
                }
            } catch (final Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
