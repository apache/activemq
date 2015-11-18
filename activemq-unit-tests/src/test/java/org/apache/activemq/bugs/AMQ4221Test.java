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
package org.apache.activemq.bugs;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4221Test extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4221Test.class);
    public int PAYLOAD_SIZE_BYTES = 4 * 1024;
    public int NUM_TO_SEND = 60000;
    public int NUM_CONCURRENT_PRODUCERS = 20;
    public int QUEUE_COUNT = 1;
    public int TMP_JOURNAL_MAX_FILE_SIZE = 10 * 1024 * 1024;

    public int DLQ_PURGE_INTERVAL = 30000;

    public int MESSAGE_TIME_TO_LIVE = 20000;
    public int EXPIRE_SWEEP_PERIOD = 200;
    public int TMP_JOURNAL_GC_PERIOD = 50;
    public int RECEIVE_POLL_PERIOD = 4000;
    private int RECEIVE_BATCH = 5000;

    final byte[] payload = new byte[PAYLOAD_SIZE_BYTES];
    final AtomicInteger counter = new AtomicInteger(0);
    final HashSet<Throwable> exceptions = new HashSet<Throwable>();
    BrokerService brokerService;
    private String brokerUrlString;
    ExecutorService executorService = Executors.newCachedThreadPool();
    final AtomicBoolean done = new AtomicBoolean(false);
    final LinkedList<String> errorsInLog = new LinkedList<String>();

    public static Test suite() {
        return suite(AMQ4221Test.class);
    }

    @Override
    public void setUp() throws Exception {

        LogManager.getRootLogger().addAppender(new DefaultTestAppender() {

            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().isGreaterOrEqual(Level.ERROR)) {
                    System.err.println("Fail on error in log: " + event.getMessage());
                    done.set(true);
                    errorsInLog.add(event.getRenderedMessage());
                }
            }
        });

        done.set(false);
        errorsInLog.clear();
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setDestinations(new ActiveMQDestination[]{new ActiveMQQueue("ActiveMQ.DLQ")});

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setPendingQueuePolicy(new FilePendingQueueMessageStoragePolicy());
        defaultPolicy.setExpireMessagesPeriod(EXPIRE_SWEEP_PERIOD);
        defaultPolicy.setProducerFlowControl(false);
        defaultPolicy.setMemoryLimit(50 * 1024 * 1024);

        brokerService.getSystemUsage().getMemoryUsage().setLimit(50 * 1024 * 1024);


        PolicyMap destinationPolicyMap = new PolicyMap();
        destinationPolicyMap.setDefaultEntry(defaultPolicy);
        brokerService.setDestinationPolicy(destinationPolicyMap);


        PListStoreImpl tempDataStore = new PListStoreImpl();
        tempDataStore.setDirectory(brokerService.getTmpDataDirectory());
        tempDataStore.setJournalMaxFileLength(TMP_JOURNAL_MAX_FILE_SIZE);
        tempDataStore.setCleanupInterval(TMP_JOURNAL_GC_PERIOD);
        tempDataStore.setIndexPageSize(200);
        tempDataStore.setIndexEnablePageCaching(false);

        brokerService.setTempDataStore(tempDataStore);
        brokerService.setAdvisorySupport(false);
        TransportConnector tcp = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        brokerUrlString = tcp.getPublishableConnectString();
    }

    @Override
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
        executorService.shutdownNow();
    }

    public void testProduceConsumeExpireHalf() throws Exception {

        final org.apache.activemq.broker.region.Queue dlq =
                (org.apache.activemq.broker.region.Queue) getDestination(brokerService, new ActiveMQQueue("ActiveMQ.DLQ"));

        if (DLQ_PURGE_INTERVAL > 0) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    while (!done.get()) {
                        try {
                            Thread.sleep(DLQ_PURGE_INTERVAL);
                            LOG.info("Purge DLQ, current size: " + dlq.getDestinationStatistics().getMessages().getCount());
                            dlq.purge();
                        } catch (InterruptedException allDone) {
                        } catch (Throwable e) {
                            e.printStackTrace();
                            exceptions.add(e);
                        }
                    }
                }
            });

        }

        final CountDownLatch latch = new CountDownLatch(QUEUE_COUNT);
        for (int i = 0; i < QUEUE_COUNT; i++) {
            final int id = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        doProduceConsumeExpireHalf(id, latch);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        exceptions.add(e);
                    }
                }
            });
        }

        while (!done.get()) {
            done.set(latch.await(5, TimeUnit.SECONDS));
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);

        assertTrue("no exceptions:" + exceptions, exceptions.isEmpty());
        assertTrue("No ERROR in log:" + errorsInLog, errorsInLog.isEmpty());

    }

    public void doProduceConsumeExpireHalf(int id, CountDownLatch latch) throws Exception {

        final ActiveMQQueue queue = new ActiveMQQueue("Q" + id);

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrlString);
        ActiveMQPrefetchPolicy prefecthPolicy = new ActiveMQPrefetchPolicy();
        prefecthPolicy.setAll(0);
        factory.setPrefetchPolicy(prefecthPolicy);
        Connection connection = factory.createConnection();
        connection.start();
        final MessageConsumer consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(queue, "on = 'true'");

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!done.get()) {
                        Thread.sleep(RECEIVE_POLL_PERIOD);
                        for (int i = 0; i < RECEIVE_BATCH && !done.get(); i++) {

                            Message message = consumer.receive(1000);
                            if (message != null) {
                                counter.incrementAndGet();
                                if (counter.get() > 0 && counter.get() % 500 == 0) {
                                    LOG.info("received: " + counter.get() + ", " + message.getJMSDestination().toString());
                                }
                            }
                        }
                    }
                } catch (JMSException ignored) {

                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions.add(e);
                }
            }
        });

        final AtomicInteger accumulator = new AtomicInteger(0);
        final CountDownLatch producersDone = new CountDownLatch(NUM_CONCURRENT_PRODUCERS);

        for (int i = 0; i < NUM_CONCURRENT_PRODUCERS; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection sendConnection = factory.createConnection();
                        sendConnection.start();
                        Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        MessageProducer producer = sendSession.createProducer(queue);
                        producer.setTimeToLive(MESSAGE_TIME_TO_LIVE);
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                        while (accumulator.incrementAndGet() < NUM_TO_SEND && !done.get()) {
                            BytesMessage message = sendSession.createBytesMessage();
                            message.writeBytes(payload);
                            message.setStringProperty("on", String.valueOf(accumulator.get() % 2 == 0));
                            producer.send(message);

                        }
                        producersDone.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptions.add(e);
                    }
                }
            });
        }

        producersDone.await(10, TimeUnit.MINUTES);

        final DestinationStatistics view = getDestinationStatistics(brokerService, queue);
        LOG.info("total expired so far " + view.getExpired().getCount() + ", " + queue.getQueueName());
        latch.countDown();
    }
}
