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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBSchedulerIndexRebuildTest {

    static final Logger LOG = LoggerFactory.getLogger(KahaDBSchedulerIndexRebuildTest.class);

    @Rule
    public TestName name = new TestName();

    private BrokerService broker = null;
    private final int NUM_JOBS = 50;

    static String basedir;
    static {
        try {
            ProtectionDomain protectionDomain = SchedulerDBVersionTest.class.getProtectionDomain();
            basedir = new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../.").getCanonicalPath();
        } catch (IOException e) {
            basedir = ".";
        }
    }

    private File schedulerStoreDir;
    private final File storeDir = new File(basedir, "activemq-data/store/");

    @Before
    public void setUp() throws Exception {
        schedulerStoreDir = new File(basedir, "activemq-data/store/scheduler/" + name.getMethodName());
        LOG.info("Test Dir = {}", schedulerStoreDir);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    public void testIndexRebuilds() throws Exception {
        IOHelper.deleteFile(schedulerStoreDir);

        JobSchedulerStoreImpl schedulerStore = createScheduler();
        broker = createBroker(schedulerStore);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.start();
        for (int i = 0; i < NUM_JOBS; ++i) {
            scheduleRepeating(connection);
        }
        connection.close();

        JobScheduler scheduler = schedulerStore.getJobScheduler("JMS");
        assertNotNull(scheduler);
        assertEquals(NUM_JOBS, scheduler.getAllJobs().size());

        broker.stop();

        IOHelper.delete(new File(schedulerStoreDir, "scheduleDB.data"));

        schedulerStore = createScheduler();
        broker = createBroker(schedulerStore);
        broker.start();

        scheduler = schedulerStore.getJobScheduler("JMS");
        assertNotNull(scheduler);
        assertEquals(NUM_JOBS, scheduler.getAllJobs().size());
    }

    @Test
    public void testIndexRebuildsAfterSomeJobsExpire() throws Exception {
        IOHelper.deleteFile(schedulerStoreDir);

        JobSchedulerStoreImpl schedulerStore = createScheduler();
        broker = createBroker(schedulerStore);
        broker.start();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.start();
        for (int i = 0; i < NUM_JOBS; ++i) {
            scheduleRepeating(connection);
            scheduleOneShot(connection);
        }
        connection.close();

        JobScheduler scheduler = schedulerStore.getJobScheduler("JMS");
        assertNotNull(scheduler);
        assertEquals(NUM_JOBS * 2, scheduler.getAllJobs().size());

        final JobScheduler awaitingOneShotTimeout = scheduler;
        assertTrue("One shot jobs should time out", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return awaitingOneShotTimeout.getAllJobs().size() == NUM_JOBS;
            }
        }, TimeUnit.MINUTES.toMillis(2)));

        broker.stop();

        IOHelper.delete(new File(schedulerStoreDir, "scheduleDB.data"));

        schedulerStore = createScheduler();
        broker = createBroker(schedulerStore);
        broker.start();

        scheduler = schedulerStore.getJobScheduler("JMS");
        assertNotNull(scheduler);
        assertEquals(NUM_JOBS, scheduler.getAllJobs().size());
    }

    private void scheduleRepeating(Connection connection) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        TextMessage message = session.createTextMessage("test msg");
        long time = 360 * 1000;
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 500);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 0);
        producer.send(message);
        producer.close();
    }

    private void scheduleOneShot(Connection connection) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);

        TextMessage message = session.createTextMessage("test msg");
        long time = TimeUnit.SECONDS.toMillis(30);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 0);
        producer.send(message);
        producer.close();
    }

    protected JobSchedulerStoreImpl createScheduler() {
        JobSchedulerStoreImpl scheduler = new JobSchedulerStoreImpl();
        scheduler.setDirectory(schedulerStoreDir);
        scheduler.setJournalMaxFileLength(10 * 1024);
        return scheduler;
    }

    protected BrokerService createBroker(JobSchedulerStoreImpl scheduler) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setJobSchedulerStore(scheduler);
        answer.setPersistent(true);
        answer.setDataDirectory(storeDir.getAbsolutePath());
        answer.setSchedulerSupport(true);
        answer.setUseJmx(false);
        return answer;
    }
}
