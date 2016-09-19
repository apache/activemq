/*
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
package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test scheduler store GC cleanup with redelivery plugin and rollbacks.
 */
public class JobSchedulerRedliveryPluginDLQStoreCleanupTest {

    static final Logger LOG = LoggerFactory.getLogger(JobSchedulerStoreCheckpointTest.class);

    private JobSchedulerStoreImpl store;
    private BrokerService brokerService;
    private ByteSequence payload;
    private String connectionURI;
    private ActiveMQConnectionFactory cf;

    @Before
    public void setUp() throws Exception {

        // investigate gc issue - store usage not getting released
        org.apache.log4j.Logger.getLogger(JobSchedulerStoreImpl.class).setLevel(Level.TRACE);

        File directory = new File("target/test/ScheduledJobsDB");
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        createSchedulerStore(directory);

        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setJobSchedulerStore(store);
        brokerService.setSchedulerSupport(true);
        brokerService.setAdvisorySupport(false);
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:0");
        RedeliveryPlugin plugin = createRedeliveryPlugin();
        brokerService.setPlugins(new BrokerPlugin[] { plugin });

        PolicyEntry policy = new PolicyEntry();
        IndividualDeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessExpired(true);
        strategy.setProcessNonPersistent(false);
        strategy.setUseQueueForQueueMessages(true);
        strategy.setQueuePrefix("DLQ.");
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        brokerService.setDestinationPolicy(pMap);
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = connector.getPublishableConnectString();

        byte[] data = new byte[8192];
        for (int i = 0; i < data.length; ++i) {
            data[i] = (byte) (i % 256);
        }

        payload = new ByteSequence(data);

        cf = new ActiveMQConnectionFactory(connectionURI);
        cf.getRedeliveryPolicy().setMaximumRedeliveries(0);
    }

    protected void createSchedulerStore(File directory) throws Exception {
        store = new JobSchedulerStoreImpl();
        store.setDirectory(directory);
        store.setCheckpointInterval(5000);
        store.setCleanupInterval(10000);
        store.setJournalMaxFileLength(10 * 1024);
    }

    protected RedeliveryPlugin createRedeliveryPlugin() {
        RedeliveryPlugin plugin = new RedeliveryPlugin();

        RedeliveryPolicy queueEntry = new RedeliveryPolicy();
        queueEntry.setMaximumRedeliveries(3);
        queueEntry.setDestination(new ActiveMQQueue("FOO.BAR"));

        RedeliveryPolicy defaultEntry = new RedeliveryPolicy();
        defaultEntry.setInitialRedeliveryDelay(5000);
        defaultEntry.setMaximumRedeliveries(0);

        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        redeliveryPolicyMap.setDefaultEntry(defaultEntry);
        redeliveryPolicyMap.setRedeliveryPolicyEntries(Arrays.asList(queueEntry));

        plugin.setRedeliveryPolicyMap(redeliveryPolicyMap);

        return plugin;
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
    public void testProducerAndRollback() throws Exception {
        final Connection connection = cf.createConnection();
        final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        final Queue queue = producerSession.createQueue("FOO.BAR");
        final MessageProducer producer = producerSession.createProducer(queue);
        final MessageConsumer consumer = consumerSession.createConsumer(queue);
        final CountDownLatch sentAll = new CountDownLatch(8);

        connection.start();
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    LOG.info("Rolling back incoming message");
                    consumerSession.rollback();
                } catch (JMSException e) {
                    LOG.warn("Failed to Rollback on incoming message");
                }
            }
        });

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    BytesMessage message = producerSession.createBytesMessage();
                    message.writeBytes(payload.data, payload.offset, payload.length);
                    producer.send(message);
                    LOG.info("Send next Message to Queue");
                    sentAll.countDown();
                } catch (JMSException e) {
                    LOG.warn("Send of message did not complete.");
                }
            }
        }, 0, 5, TimeUnit.SECONDS);

        assertTrue("Should have sent all messages", sentAll.await(2, TimeUnit.MINUTES));

        executor.shutdownNow();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue("Should clean out the scheduler store", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getNumJournalFiles() == 1;
            }
        }));
    }

    private int getNumJournalFiles() throws IOException {
        return store.getJournal().getFileMap().size();
    }
}
