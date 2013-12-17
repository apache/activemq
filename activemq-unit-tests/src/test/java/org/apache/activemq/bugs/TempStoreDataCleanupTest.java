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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempStoreDataCleanupTest {

    private static final Logger LOG = LoggerFactory.getLogger(TempStoreDataCleanupTest.class);
    private static final String QUEUE_NAME = TempStoreDataCleanupTest.class.getName() + "Queue";

    private final String str = new String(
        "QAa0bcLdUK2eHfJgTP8XhiFj61DOklNm9nBoI5pGqYVrs3CtSuMZvwWx4yE7zR");

    private BrokerService broker;
    private String connectionUri;
    private ExecutorService pool;
    private String queueName;
    private Random r = new Random();

    @Before
    public void setUp() throws Exception {

        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setDedicatedTaskRunner(false);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        SharedDeadLetterStrategy strategy = new SharedDeadLetterStrategy();
        strategy.setProcessExpired(false);
        strategy.setProcessNonPersistent(false);

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setQueue(">");
        defaultPolicy.setOptimizedDispatch(true);
        defaultPolicy.setDeadLetterStrategy(strategy);
        defaultPolicy.setMemoryLimit(9000000);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);

        broker.setDestinationPolicy(policyMap);

        broker.getSystemUsage().getMemoryUsage().setLimit(300000000L);

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
        pool = Executors.newFixedThreadPool(10);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }

        if (pool != null) {
            pool.shutdown();
        }
    }

    @Test
    public void testIt() throws Exception {

        int startPercentage = broker.getAdminView().getMemoryPercentUsage();
        LOG.info("MemoryUsage at test start = " + startPercentage);

        for (int i = 0; i < 2; i++) {
            LOG.info("Started the test iteration: " + i + " using queueName = " + queueName);
            queueName = QUEUE_NAME + i;
            final CountDownLatch latch = new CountDownLatch(11);

            pool.execute(new Runnable() {
                @Override
                public void run() {
                    receiveAndDiscard100messages(latch);
                }
            });

            for (int j = 0; j < 10; j++) {
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        send10000messages(latch);
                    }
                });
            }

            LOG.info("Waiting on the send / receive latch");
            latch.await(5, TimeUnit.MINUTES);
            LOG.info("Resumed");

            destroyQueue();
            TimeUnit.SECONDS.sleep(2);
        }

        LOG.info("MemoryUsage before awaiting temp store cleanup = " + broker.getAdminView().getMemoryPercentUsage());

        final PListStoreImpl pa = (PListStoreImpl) broker.getTempDataStore();
        assertTrue("only one journal file should be left: " + pa.getJournal().getFileMap().size(),
            Wait.waitFor(new Wait.Condition() {

                @Override
                public boolean isSatisified() throws Exception {
                    return pa.getJournal().getFileMap().size() == 1;
                }
            }, TimeUnit.MINUTES.toMillis(3))
        );

        int endPercentage = broker.getAdminView().getMemoryPercentUsage();
        LOG.info("MemoryUseage at test end = " + endPercentage);

        assertEquals(startPercentage, endPercentage);
    }

    public void destroyQueue() {
        try {
            Broker broker = this.broker.getBroker();
            if (!broker.isStopped()) {
                LOG.info("Removing: " + queueName);
                broker.removeDestination(this.broker.getAdminConnectionContext(), new ActiveMQQueue(queueName), 10);
            }
        } catch (Exception e) {
            LOG.warn("Got an error while removing the test queue", e);
        }
    }

    private void send10000messages(CountDownLatch latch) {
        ActiveMQConnection activeMQConnection = null;
        try {
            activeMQConnection = createConnection(null);
            Session session = activeMQConnection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session
                    .createQueue(queueName));
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            activeMQConnection.start();
            for (int i = 0; i < 10000; i++) {
                TextMessage textMessage = session.createTextMessage();
                textMessage.setText(generateBody(1000));
                textMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
                producer.send(textMessage);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }
            producer.close();
        } catch (JMSException e) {
            LOG.warn("Got an error while sending the messages", e);
        } finally {
            if (activeMQConnection != null) {
                try {
                    activeMQConnection.close();
                } catch (JMSException e) {
                }
            }
        }
        latch.countDown();
    }

    private void receiveAndDiscard100messages(CountDownLatch latch) {
        ActiveMQConnection activeMQConnection = null;
        try {
            activeMQConnection = createConnection(null);
            Session session = activeMQConnection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(
                    session.createQueue(queueName));
            activeMQConnection.start();
            for (int i = 0; i < 100; i++) {
                messageConsumer.receive();
            }
            messageConsumer.close();
            LOG.info("Created and disconnected");
        } catch (JMSException e) {
            LOG.warn("Got an error while receiving the messages", e);
        } finally {
            if (activeMQConnection != null) {
                try {
                    activeMQConnection.close();
                } catch (JMSException e) {
                }
            }
        }
        latch.countDown();
    }

    private ActiveMQConnection createConnection(String id) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        if (id != null) {
            factory.setClientID(id);
        }

        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        return connection;
    }

    private String generateBody(int length) {

        StringBuilder sb = new StringBuilder();
        int te = 0;
        for (int i = 1; i <= length; i++) {
            te = r.nextInt(62);
            sb.append(str.charAt(te));
        }
        return sb.toString();
    }
}
