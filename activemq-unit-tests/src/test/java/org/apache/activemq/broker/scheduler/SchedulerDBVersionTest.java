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
package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerDBVersionTest {
    static String basedir;
    static {
        try {
            ProtectionDomain protectionDomain = SchedulerDBVersionTest.class.getProtectionDomain();
            basedir = new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalPath();
        } catch (IOException e) {
            basedir = ".";
        }
    }

    static final Logger LOG = LoggerFactory.getLogger(SchedulerDBVersionTest.class);
    final static File VERSION_LEGACY_JMS =
        new File(basedir + "/src/test/resources/org/apache/activemq/store/schedulerDB/legacy");

    private BrokerService broker = null;

    protected BrokerService createBroker(JobSchedulerStoreImpl scheduler) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setStoreOpenWireVersion(OpenWireFormat.DEFAULT_LEGACY_VERSION);
        answer.setJobSchedulerStore(scheduler);
        answer.setPersistent(true);
        answer.setDataDirectory("target/SchedulerDBVersionTest/");
        answer.setSchedulerSupport(true);
        answer.setUseJmx(false);
        return answer;
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Ignore("Used only when a new version of the store needs to archive it's test data.")
    @Test
    public void testCreateStore() throws Exception {
        JobSchedulerStoreImpl scheduler = new JobSchedulerStoreImpl();
        File dir = new File("src/test/resources/org/apache/activemq/store/schedulerDB/legacy");
        IOHelper.deleteFile(dir);
        scheduler.setDirectory(dir);
        scheduler.setJournalMaxFileLength(1024 * 1024);
        broker = createBroker(scheduler);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.start();
        scheduleRepeating(connection);
        connection.close();
        broker.stop();
    }

    private void scheduleRepeating(Connection connection) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        TextMessage message = session.createTextMessage("test msg");
        long time = 1000;
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 500);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, -1);
        producer.send(message);
        producer.close();
    }

    @Test
    public void testLegacyStoreConversion() throws Exception {
        doTestScheduleRepeated(VERSION_LEGACY_JMS);
    }

    public void doTestScheduleRepeated(File existingStore) throws Exception {
        File testDir = new File("target/SchedulerDBVersionTest/store/scheduler/versionDB");
        IOHelper.deleteFile(testDir);
        IOHelper.copyFile(existingStore, testDir);

        final int NUMBER = 1;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");

        for (int i = 0; i < 1; ++i) {
            JobSchedulerStoreImpl scheduler = new JobSchedulerStoreImpl();
            scheduler.setDirectory(testDir);
            scheduler.setJournalMaxFileLength(1024 * 1024);
            BrokerService broker = createBroker(scheduler);
            broker.start();
            broker.waitUntilStarted();

            final AtomicInteger count = new AtomicInteger();
            Connection connection = cf.createConnection();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("test.queue");

            MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch latch = new CountDownLatch(NUMBER);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    LOG.info("Received scheduled message: {}", message);
                    latch.countDown();
                    count.incrementAndGet();
                }
            });

            connection.start();
            assertEquals(latch.getCount(), NUMBER);
            latch.await(30, TimeUnit.SECONDS);

            connection.close();
            broker.stop();
            broker.waitUntilStopped();

            assertEquals(0, latch.getCount());
        }
    }
}
