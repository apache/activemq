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
package org.apache.activemq.broker.region;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuePurgeTest extends CombinationTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(QueuePurgeTest.class);
    private static final int NUM_TO_SEND = 20000;
    private final String MESSAGE_TEXT = new String(new byte[1024]);
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    MessageConsumer consumer;

    @Override
    protected void setUp() throws Exception {
        setMaxTestTime(10*60*1000); // 10 mins
        setAutoFail(true);
        super.setUp();
        broker = new BrokerService();

        File testDataDir = new File("target/activemq-data/QueuePurgeTest");
        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024l*1024*64);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File(testDataDir, "kahadb"));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (consumer != null) {
            consumer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    public void testPurgeLargeQueue() throws Exception {
        testPurgeLargeQueue(false);
    }

    public void testPurgeLargeQueuePrioritizedMessages() throws Exception {
        testPurgeLargeQueue(true);
    }

    private void testPurgeLargeQueue(boolean prioritizedMessages) throws Exception {
        applyBrokerSpoolingPolicy(prioritizedMessages);
        createProducerAndSendMessages(NUM_TO_SEND);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        LOG.info("purging..");

        org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(org.apache.activemq.broker.jmx.QueueView.class);
        final AtomicBoolean gotPurgeLogMessage = new AtomicBoolean(false);

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getMessage() instanceof String) {
                    String message = (String) event.getMessage();
                    if (message.contains("purge of " + NUM_TO_SEND +" messages")) {
                        LOG.info("Received a log message: {} ", event.getMessage());
                        gotPurgeLogMessage.set(true);
                    }
                }
            }
        };

        Level level = log4jLogger.getLevel();
        log4jLogger.setLevel(Level.INFO);
        log4jLogger.addAppender(appender);
        try {

            proxy.purge();

        } finally {
            log4jLogger.setLevel(level);
            log4jLogger.removeAppender(appender);
        }

        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
        assertTrue("cache is disabled, temp store being used", !proxy.isCacheEnabled());
        assertTrue("got expected info purge log message", gotPurgeLogMessage.get());
        assertEquals("Found messages when browsing", 0, proxy.browseMessages().size());
    }

    public void testRepeatedExpiryProcessingOfLargeQueue() throws Exception {
        applyBrokerSpoolingPolicy(false);
        final int expiryPeriod = 500;
        applyExpiryDuration(expiryPeriod);
        createProducerAndSendMessages(NUM_TO_SEND);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        LOG.info("waiting for expiry to kick in a bunch of times to verify it does not blow mem");
        Thread.sleep(5000);
        assertEquals("Queue size is has not changed " + proxy.getQueueSize(), NUM_TO_SEND,
                proxy.getQueueSize());
    }


    private void applyExpiryDuration(int i) {
        broker.getDestinationPolicy().getDefaultEntry().setExpireMessagesPeriod(i);
    }

    private void applyBrokerSpoolingPolicy(boolean prioritizedMessages) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setPrioritizedMessages(prioritizedMessages);
        defaultEntry.setProducerFlowControl(false);
        PendingQueueMessageStoragePolicy pendingQueuePolicy = new FilePendingQueueMessageStoragePolicy();
        defaultEntry.setPendingQueuePolicy(pendingQueuePolicy);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
    }


    public void testPurgeLargeQueueWithConsumer() throws Exception {
        testPurgeLargeQueueWithConsumer(false);
    }

    public void testPurgeLargeQueueWithConsumerPrioritizedMessages() throws Exception {
        testPurgeLargeQueueWithConsumer(true);
    }

    public void testConcurrentPurgeAndSend() throws Exception {
        testConcurrentPurgeAndSend(false);
    }

    public void testConcurrentPurgeAndSendPrioritizedMessages() throws Exception {
        testConcurrentPurgeAndSend(true);
    }

    private void testConcurrentPurgeAndSend(boolean prioritizedMessages) throws Exception {
        applyBrokerSpoolingPolicy(false);
        createProducerAndSendMessages(NUM_TO_SEND / 2);
        final QueueViewMBean proxy = getProxyToQueueViewMBean();
        createConsumer();
        final long start = System.currentTimeMillis();
        ExecutorService service = Executors.newFixedThreadPool(1);
        try {
            LOG.info("purging..");
            service.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        proxy.purge();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                    LOG.info("purge done: " + (System.currentTimeMillis() - start) + "ms");
                }
            });

            //send should get blocked while purge is running
            //which should ensure the metrics are correct
            createProducerAndSendMessages(NUM_TO_SEND / 2);

            Message msg;
            do {
                msg = consumer.receive(1000);
                if (msg != null) {
                    msg.acknowledge();
                }
            } while (msg != null);
            assertEquals("Queue size not valid", 0, proxy.getQueueSize());
            assertEquals("Found messages when browsing", 0, proxy.browseMessages().size());
        } finally {
            service.shutdownNow();
        }
    }

    private void testPurgeLargeQueueWithConsumer(boolean prioritizedMessages) throws Exception {
        applyBrokerSpoolingPolicy(prioritizedMessages);
        createProducerAndSendMessages(NUM_TO_SEND);
        QueueViewMBean proxy = getProxyToQueueViewMBean();
        createConsumer();
        long start = System.currentTimeMillis();
        LOG.info("purging..");
        proxy.purge();
        LOG.info("purge done: " + (System.currentTimeMillis() - start) + "ms");
        assertEquals("Queue size is not zero, it's " + proxy.getQueueSize(), 0,
                proxy.getQueueSize());
        assertEquals("usage goes to duck", 0, proxy.getMemoryPercentUsage());
        Message msg;
        do {
            msg = consumer.receive(1000);
            if (msg != null) {
                msg.acknowledge();
            }
        } while (msg != null);
        assertEquals("Queue size not valid", 0, proxy.getQueueSize());
        assertEquals("Found messages when browsing", 0, proxy.browseMessages().size());
    }

    private QueueViewMBean getProxyToQueueViewMBean()
            throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName =
                new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="
                + queue.getQueueName());
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName,
                        QueueViewMBean.class, true);
        return proxy;
    }

    private void createProducerAndSendMessages(int numToSend) throws Exception {
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        queue = session.createQueue("test1");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < numToSend; i++) {
            TextMessage message = session.createTextMessage(MESSAGE_TEXT + i);
            if (i  != 0 && i % 10000 == 0) {
                LOG.info("sent: " + i);
            }
            producer.send(message);
        }
        producer.close();
    }

    private void createConsumer() throws Exception {
        consumer = session.createConsumer(queue);
        // wait for buffer fill out
        Thread.sleep(5 * 1000);
        for (int i = 0; i < 500; ++i) {
            Message message = consumer.receive();
            message.acknowledge();
        }
    }
}
