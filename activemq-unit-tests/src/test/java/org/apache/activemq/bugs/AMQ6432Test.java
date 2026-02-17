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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.activemq.util.Wait;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import javax.management.ObjectName;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class AMQ6432Test {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ6432Test.class);

    private static final String QUEUE_NAME = "test.queue";
    private BrokerService broker;

    @Before
    public void setup() throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);

        KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        kahaDB.setJournalMaxFileLength(256 * 1024);
        kahaDB.setCleanupInterval(500);
        kahaDB.setCompactAcksAfterNoGC(1);
        kahaDB.setCompactAcksIgnoresStoreGrowth(true);
        broker.setPersistenceAdapter(kahaDB);


        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testTransactedStoreUsageSuspendResume() throws Exception {

        final AtomicBoolean failed = new AtomicBoolean(false);

        final File journalDataDir = ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getDirectory();

        
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(MessageDatabase.class));
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getMessage().getFormattedMessage().startsWith("Failed to load next journal")) {
                    LOG.info("received unexpected log message: " + event.getMessage().getFormattedMessage());
                    failed.set(true);
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        try {

            ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
            sendExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendReceive(10000);
                    } catch (Exception ignored) {
                    }
                }
            });

            sendExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendLargeAndPurge(5000);
                    } catch (Exception ignored) {
                    }
                }
            });

            sendExecutor.shutdown();
            sendExecutor.awaitTermination(10, TimeUnit.MINUTES);


            // need to let a few gc cycles to complete then there will be 2 files in the mix and acks will move
            TimeUnit.SECONDS.sleep(2);

            assertTrue("gc worked ok", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().size() < 3;
                }
            }));

        } finally {
            logger.removeAppender(appender);
        }
        assertFalse("failed on unexpected log event", failed.get());

        sendReceive(500);

        assertTrue("gc worked ok", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().size() < 2;
            }
        }));

        // file actually gone!
        LOG.info("Files: " + Arrays.asList(journalDataDir.listFiles()));
        assertTrue("Minimum data files in the mix", journalDataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("db-");
            }
        }).length == 1);


    }

    private void sendReceive(int max) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setAlwaysSyncSend(true);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(QUEUE_NAME+max);
        MessageProducer producer = session.createProducer(null);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(new byte[10]);

        MessageConsumer consumer = session.createConsumer(queue);

        for (int i=0; i<max; i++) {
            producer.send(queue, message);
            consumer.receive(4000);
        }
        connection.close();
    }

    private void sendLargeAndPurge(int max) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setAlwaysSyncSend(true);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue toPurge = new ActiveMQQueue(QUEUE_NAME + "-to-purge-" + max);
        MessageProducer producer = session.createProducer(null);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(new byte[1024]);

        for (int i = 0; i < max; i++) {
            producer.send(toPurge, message);
        }

        connection.close();

        TimeUnit.SECONDS.sleep(1);

        ObjectName queueViewMBeanName =
                new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="
                        + toPurge.getQueueName());
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName,
                        QueueViewMBean.class, true);

        proxy.purge();
    }
}
