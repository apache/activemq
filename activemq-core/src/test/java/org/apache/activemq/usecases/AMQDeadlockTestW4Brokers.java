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
package org.apache.activemq.usecases;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AMQDeadlockTestW4Brokers extends TestCase {
    private static final transient Log log = LogFactory.getLog(AMQDeadlockTestW4Brokers.class);
    private static final String BROKER_URL1 = "tcp://localhost:61616";
    private static final String BROKER_URL2 = "tcp://localhost:61617";
    private static final String BROKER_URL3 = "tcp://localhost:61618";
    private static final String BROKER_URL4 = "tcp://localhost:61619";
    private static final String URL1 = "tcp://localhost:61616?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false&wireFormat.maxInactivityDuration=30000&wireFormat.tcpNoDelayEnabled=false";
    private static final String URL2 = "tcp://localhost:61617?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false&wireFormat.maxInactivityDuration=30000&wireFormat.tcpNoDelayEnabled=false";
    private static final String URL3 = "tcp://localhost:61618?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false&wireFormat.maxInactivityDuration=30000&wireFormat.tcpNoDelayEnabled=false";
    private static final String URL4 = "tcp://localhost:61619?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false&wireFormat.maxInactivityDuration=30000&wireFormat.tcpNoDelayEnabled=false";
    private static final String QUEUE1_NAME = "test.queue.1";
    private static final int MAX_CONSUMERS = 5;
    private static final int NUM_MESSAGE_TO_SEND = 10000;
    private static final CountDownLatch latch = new CountDownLatch(MAX_CONSUMERS * NUM_MESSAGE_TO_SEND);

    @Override
    public void setUp() throws Exception {

    }

    @Override
    public void tearDown() throws Exception {

    }

    public void test4BrokerWithOutLingo() throws Exception {

        BrokerService brokerService1 = null;
        BrokerService brokerService2 = null;
        BrokerService brokerService3 = null;
        BrokerService brokerService4 = null;
        ActiveMQConnectionFactory acf1 = null;
        ActiveMQConnectionFactory acf2 = null;
        PooledConnectionFactory pcf1 = null;
        PooledConnectionFactory pcf2 = null;
        ActiveMQConnectionFactory acf3 = null;
        ActiveMQConnectionFactory acf4 = null;
        PooledConnectionFactory pcf3 = null;
        PooledConnectionFactory pcf4 = null;
        DefaultMessageListenerContainer container1 = null;

        try {

            //Test with and without queue limits.
            brokerService1 = createBrokerService("broker1", BROKER_URL1,
                    BROKER_URL2, BROKER_URL3, BROKER_URL4, 0 /* 10000000 */);
            brokerService1.start();
            brokerService2 = createBrokerService("broker2", BROKER_URL2,
                    BROKER_URL1, BROKER_URL3, BROKER_URL4, 0/* 40000000 */);
            brokerService2.start();
            brokerService3 = createBrokerService("broker3", BROKER_URL3,
                    BROKER_URL2, BROKER_URL1, BROKER_URL4, 0/* 10000000 */);
            brokerService3.start();
            brokerService4 = createBrokerService("broker4", BROKER_URL4,
                    BROKER_URL1, BROKER_URL3, BROKER_URL2, 0/* 10000000 */);
            brokerService4.start();

            final String failover1 = "failover:("
                    + URL1
                    + ")?initialReconnectDelay=10&maxReconnectDelay=30000&useExponentialBackOff=true&backOffMultiplier=2&maxReconnectAttempts=0&randomize=false";
            final String failover2 = "failover:("
                    + URL2
                    + ")?initialReconnectDelay=10&maxReconnectDelay=30000&useExponentialBackOff=true&backOffMultiplier=2&maxReconnectAttempts=0&randomize=false";

            final String failover3 = "failover:("
                    + URL3
                    + ")?initialReconnectDelay=10&maxReconnectDelay=30000&useExponentialBackOff=true&backOffMultiplier=2&maxReconnectAttempts=0&randomize=false";

            final String failover4 = "failover:("
                    + URL4
                    + ")?initialReconnectDelay=10&maxReconnectDelay=30000&useExponentialBackOff=true&backOffMultiplier=2&maxReconnectAttempts=0&randomize=false";

            acf1 = createConnectionFactory(failover1);
            acf2 = createConnectionFactory(failover2);
            acf3 = createConnectionFactory(failover3);
            acf4 = createConnectionFactory(failover4);

            pcf1 = new PooledConnectionFactory(acf1);
            pcf2 = new PooledConnectionFactory(acf2);
            pcf3 = new PooledConnectionFactory(acf3);
            pcf4 = new PooledConnectionFactory(acf4);

            container1 = createDefaultMessageListenerContainer(acf2,
                    new TestMessageListener1(0), QUEUE1_NAME);
            container1.afterPropertiesSet();

            final PooledProducerTask[] task = new PooledProducerTask[4];
            task[0] = new PooledProducerTask(pcf1, QUEUE1_NAME, "producer1");
            task[1] = new PooledProducerTask(pcf2, QUEUE1_NAME, "producer2");
            task[2] = new PooledProducerTask(pcf3, QUEUE1_NAME, "producer3");
            task[3] = new PooledProducerTask(pcf4, QUEUE1_NAME, "producer4");

            final ExecutorService executor = Executors.newCachedThreadPool();

            for (int i = 0; i < 4; i++) {
                executor.submit(task[i]);
            }

            latch.await(15, TimeUnit.SECONDS);
            assertTrue(latch.getCount() == MAX_CONSUMERS * NUM_MESSAGE_TO_SEND);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

            container1.stop();
            container1.destroy();
            container1 = null;

            brokerService1.stop();
            brokerService1 = null;
            brokerService2.stop();
            brokerService2 = null;
            brokerService3.stop();
            brokerService3 = null;
            brokerService4.stop();
            brokerService4 = null;
        }
    }

    private BrokerService createBrokerService(final String brokerName,
            final String uri1, final String uri2, final String uri3,
            final String uri4, final int queueLimit) throws Exception {
        final BrokerService brokerService = new BrokerService();

        brokerService.setBrokerName(brokerName);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);

        final UsageManager memoryManager = new UsageManager();
        memoryManager.setLimit(100000000);
        brokerService.setMemoryManager(memoryManager);

        final ArrayList<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();

        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(queueLimit);
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
            final NetworkConnector nc = new DiscoveryNetworkConnector(new URI(
                    "static:" + uri2 + "," + uri3 + "," + uri4));
            nc.setBridgeTempDestinations(true);
            nc.setBrokerName(brokerName);

            // When using queue limits set this to 1
            nc.setPrefetchSize(1000);
            nc.setNetworkTTL(1);
            brokerService.addNetworkConnector(nc);
        }

        return brokerService;
    }

    public DefaultMessageListenerContainer createDefaultMessageListenerContainer(
            final ConnectionFactory acf, final MessageListener listener,
            final String queue) {
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
        acf.setUseAsyncSend(false);

        return acf;
    }

    private class TestMessageListener1 implements MessageListener {
        private final long waitTime;
        final AtomicInteger count = new AtomicInteger(0);

        public TestMessageListener1(long waitTime) {
            this.waitTime = waitTime;
        }

        public void onMessage(Message msg) {

            try {
                /*log.info("Listener1 Consumed message "
                            + msg.getIntProperty("count") + " from "
                            + msg.getStringProperty("producerName"));*/
                int value = count.incrementAndGet();
                if (value % 1000 == 0) {
                    log.info("Consumed message: " + value);
                }

                Thread.sleep(waitTime);
                latch.countDown();
                /*} catch (JMSException e) {
                    e.printStackTrace();*/
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class PooledProducerTask implements Runnable {
        private final String queueName;
        private final PooledConnectionFactory pcf;
        private final String producerName;

        public PooledProducerTask(final PooledConnectionFactory pcf,
                final String queueName, final String producerName) {
            this.pcf = pcf;
            this.queueName = queueName;
            this.producerName = producerName;
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

                for (int i = 0; i < NUM_MESSAGE_TO_SEND; i++) {
                    final int count = i;
                    jmsTemplate.send(queueName, new MessageCreator() {
                        public Message createMessage(Session session)
                                throws JMSException {

                            final BytesMessage message = session
                                    .createBytesMessage();

                            message.writeBytes(bytes);
                            message.setIntProperty("count", count);
                            message.setStringProperty("producerName",
                                    producerName);
                            return message;
                        }
                    });

                    //	log.info("PooledProducer " + producerName + " sent message: " + count);

                    // Thread.sleep(1000);
                }
            }
            catch (final Throwable e) {
                System.err.println("Producer 1 is exiting.");
                e.printStackTrace();
            }
        }
    }
}
