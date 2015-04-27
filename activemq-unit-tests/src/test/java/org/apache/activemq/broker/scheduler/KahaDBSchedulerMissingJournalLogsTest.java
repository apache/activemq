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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *Test that the store recovers even if some log files are missing.
 */
public class KahaDBSchedulerMissingJournalLogsTest {

    static final Logger LOG = LoggerFactory.getLogger(KahaDBSchedulerIndexRebuildTest.class);

    private BrokerService broker = null;
    private JobSchedulerStoreImpl schedulerStore = null;

    private final int NUM_LOGS = 6;

    static String basedir;
    static {
        try {
            ProtectionDomain protectionDomain = SchedulerDBVersionTest.class.getProtectionDomain();
            basedir = new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../.").getCanonicalPath();
        } catch (IOException e) {
            basedir = ".";
        }
    }

    private final File schedulerStoreDir = new File(basedir, "activemq-data/store/scheduler");
    private final File storeDir = new File(basedir, "activemq-data/store/");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        IOHelper.deleteFile(schedulerStoreDir);
        LOG.info("Test Dir = {}", schedulerStoreDir);

        createBroker();
        broker.start();
        broker.waitUntilStarted();

        schedulerStore = (JobSchedulerStoreImpl) broker.getJobSchedulerStore();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout=120 * 1000)
    public void testMissingLogsCausesBrokerToFail() throws Exception {
        fillUpSomeLogFiles();

        int jobCount = schedulerStore.getJobScheduler("JMS").getAllJobs().size();
        LOG.info("There are {} jobs in the store.", jobCount);

        List<File> toDelete = new ArrayList<File>();
        Map<Integer, DataFile> files = schedulerStore.getJournal().getFileMap();
        for (int i = files.size(); i > files.size() / 2; i--) {
            toDelete.add(files.get(i).getFile());
        }

        broker.stop();
        broker.waitUntilStopped();

        for (File file : toDelete) {
            LOG.info("File to delete: {}", file);
            IOHelper.delete(file);
        }

        try {
            createBroker();
            broker.start();
            fail("Should not start when logs are missing.");
        } catch (Exception e) {
        }
    }

    @Test(timeout=120 * 1000)
    public void testRecoverWhenSomeLogsAreMissing() throws Exception {
        fillUpSomeLogFiles();

        int jobCount = schedulerStore.getJobScheduler("JMS").getAllJobs().size();
        LOG.info("There are {} jobs in the store.", jobCount);

        List<File> toDelete = new ArrayList<File>();
        Map<Integer, DataFile> files = schedulerStore.getJournal().getFileMap();
        for (int i = files.size() - 1; i > files.size() / 2; i--) {
            toDelete.add(files.get(i).getFile());
        }

        broker.stop();
        broker.waitUntilStopped();

        for (File file : toDelete) {
            LOG.info("File to delete: {}", file);
            IOHelper.delete(file);
        }

        schedulerStore = createScheduler();
        schedulerStore.setIgnoreMissingJournalfiles(true);

        createBroker(schedulerStore);
        broker.start();
        broker.waitUntilStarted();

        int postRecoverJobCount = schedulerStore.getJobScheduler("JMS").getAllJobs().size();
        assertTrue(postRecoverJobCount > 0);
        assertTrue(postRecoverJobCount < jobCount);
    }

    private void fillUpSomeLogFiles() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test.queue");
        MessageProducer producer = session.createProducer(queue);
        connection.start();
        while (true) {
            scheduleRepeating(session, producer);
            if (schedulerStore.getJournal().getFileMap().size() == NUM_LOGS) {
                break;
            }
        }
        connection.close();
    }

    private void scheduleRepeating(Session session, MessageProducer producer) throws Exception {
        TextMessage message = session.createTextMessage("test msg");
        long time = 360 * 1000;
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 500);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, -1);
        producer.send(message);
    }

    protected JobSchedulerStoreImpl createScheduler() {
        JobSchedulerStoreImpl scheduler = new JobSchedulerStoreImpl();
        scheduler.setDirectory(schedulerStoreDir);
        scheduler.setJournalMaxFileLength(10 * 1024);
        return scheduler;
    }

    protected void createBroker() throws Exception {
        createBroker(createScheduler());
    }

    protected void createBroker(JobSchedulerStoreImpl scheduler) throws Exception {
        broker = new BrokerService();
        broker.setJobSchedulerStore(scheduler);
        broker.setPersistent(true);
        broker.setDataDirectory(storeDir.getAbsolutePath());
        broker.setSchedulerSupport(true);
        broker.setUseJmx(false);
    }
}
