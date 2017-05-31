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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test loss of message on index rebuild when presistJMSRedelivered is on.
 */
public class AMQ6133PersistJMSRedeliveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6133PersistJMSRedeliveryTest.class);
    private static final String QUEUE_NAME = "test.queue";

    private BrokerService broker;

    @Test
    public void testPersistJMSRedeliveredMessageLossOnIndexRebuild() throws Exception {
        sendMessages();
        LOG.info("#### Finished sending messages, test starting. ####");

        long msgCount = getProxyToQueue(QUEUE_NAME).getQueueSize();

        final int ITERATIONS = 3;

        // Force some updates
        for (int i = 0; i < ITERATIONS; ++i) {
            LOG.info("Consumer and Rollback iteration: {}", i);
            consumerAndRollback(i);
        }

        // Allow GC to run at least once.
        TimeUnit.SECONDS.sleep(20);

        restart();

        assertEquals(msgCount, getProxyToQueue(QUEUE_NAME).getQueueSize());

        restartWithRecovery(getPersistentDir());

        assertEquals(msgCount, getProxyToQueue(QUEUE_NAME).getQueueSize());
    }

    @Before
    public void setup() throws Exception {

        // Investigate loss of messages on message update in store.
        org.apache.log4j.Logger.getLogger(MessageDatabase.class).setLevel(Level.TRACE);

        createBroker(true);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    private void restart() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        createBroker(false);
    }

    private void restartWithRecovery(File persistenceDir) throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        // delete the index so that it needs to be rebuilt from replay
        for (File index : FileUtils.listFiles(persistenceDir, new WildcardFileFilter("db.*"), TrueFileFilter.INSTANCE)) {
            FileUtils.deleteQuietly(index);
        }

        createBroker(false);
    }

    private void sendMessages() throws Exception {
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(QUEUE_NAME);
        Destination retainQueue = session.createQueue(QUEUE_NAME + "-retain");
        MessageProducer producer = session.createProducer(null);

        final byte[] payload = new byte[1000];
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(payload);

        // Build up a set of messages that will be redelivered and updated later.
        while (getLogFileCount() < 3) {
            producer.send(queue, message);
        }

        // Now create some space for files that are retained during the test.
        while (getLogFileCount() < 6) {
            producer.send(retainQueue, message);
        }

        connection.close();
    }

    private void consumerAndRollback(int iteration) throws Exception {
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE_NAME);
        MessageConsumer consumer = session.createConsumer(queue);

        long msgCount = getProxyToQueue(queue.getQueueName()).getQueueSize();

        for (int i = 0; i < msgCount; ++i) {
            Message message = consumer.receive(50000);
            assertNotNull(message);
            if (iteration > 0) {
                assertTrue(message.getJMSRedelivered());
            }
        }

        connection.close();
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=0");
        factory.setAlwaysSyncSend(true);
        Connection connection = factory.createConnection();
        connection.start();

        return connection;
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        PolicyEntry entry = new PolicyEntry();
        entry.setPersistJMSRedelivered(true);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.setPersistent(true);
        broker.setDestinationPolicy(policyMap);

        KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        kahaDB.setJournalMaxFileLength(128 * 1024);
        kahaDB.setCleanupInterval(8*1000);

        broker.setPersistenceAdapter(kahaDB);
        broker.getSystemUsage().getStoreUsage().setLimit(7*1024*1024);
        broker.start();
        broker.waitUntilStarted();
    }

    private int getLogFileCount() throws Exception {
        return new ArrayList<File>(
                FileUtils.listFiles(getPersistentDir(),
                    new WildcardFileFilter("*.log"), TrueFileFilter.INSTANCE)).size();
    }

    private File getPersistentDir() throws IOException {
        return broker.getPersistenceAdapter().getDirectory();
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
