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
package org.apache.activemq.store.kahadb.scheduler;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.IOHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AMQ7086Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ7086Test.class);

    BrokerService brokerService;
    JobSchedulerStoreImpl jobSchedulerStore;
    KahaDBPersistenceAdapter kahaDBPersistenceAdapter;

    @Test(timeout = 120000)
    public void testGcDoneAtStop() throws Exception {

        brokerService = createBroker(true);
        brokerService.start();

        produceWithScheduledDelayAndConsume();

        LOG.info("job store: " + jobSchedulerStore);
        int numSchedulerFiles = jobSchedulerStore.getJournal().getFileMap().size();
        LOG.info("kahadb store: " + kahaDBPersistenceAdapter);
        int numKahadbFiles = kahaDBPersistenceAdapter.getStore().getJournal().getFileMap().size();

        LOG.info("Num files, job store: {}, message store: {}", numKahadbFiles, numKahadbFiles);

        // pull the dirs before we stop
        File jobDir = jobSchedulerStore.getJournal().getDirectory();
        File kahaDir = kahaDBPersistenceAdapter.getStore().getJournal().getDirectory();

        brokerService.stop();

        while (verifyFilesOnDisk(jobDir) < 1) {
            Thread.sleep(100);
        }
        assertTrue("Expected job store data files at least 1", verifyFilesOnDisk(jobDir) >= 1);
        while (verifyFilesOnDisk(kahaDir) < 1) {
            Thread.sleep(100);
        }
        assertTrue("Expected kahadb data files at least 1", verifyFilesOnDisk(kahaDir) >= 1);
    }

    @Test
    public void testNoGcAtStop() throws Exception {

        brokerService = createBroker(false);
        brokerService.start();

        produceWithScheduledDelayAndConsume();

        LOG.info("job store: " + jobSchedulerStore);
        int numSchedulerFiles = jobSchedulerStore.getJournal().getFileMap().size();
        LOG.info("kahadb store: " + kahaDBPersistenceAdapter);
        int numKahadbFiles = kahaDBPersistenceAdapter.getStore().getJournal().getFileMap().size();

        LOG.info("Num files, job store: {}, message store: {}", numKahadbFiles, numKahadbFiles);

        // pull the dirs before we stop
        File jobDir = jobSchedulerStore.getJournal().getDirectory();
        File kahaDir = kahaDBPersistenceAdapter.getStore().getJournal().getDirectory();

        brokerService.stop();

        assertEquals("Expected job store data files", numSchedulerFiles, verifyFilesOnDisk(jobDir));
        assertEquals("Expected kahadb data files", numKahadbFiles, verifyFilesOnDisk(kahaDir));
    }

    private int verifyFilesOnDisk(File directory) {

        LOG.info("Broker: " + brokerService);
        LOG.info("dir: " + directory);
        int result = 0;

        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(Journal.DEFAULT_FILE_PREFIX) && n.endsWith(Journal.DEFAULT_FILE_SUFFIX);
            }
        });

        LOG.info("File count: " + (files != null ? files.length : " empty!"));

        if (files != null) {
            result = files.length;
        }
        for (File file : files) {
            LOG.info("name :" + file.getAbsolutePath());
        }
        return result;
    }

    protected BrokerService createBroker(boolean doCleanupOnStop) throws Exception {
        File schedulerDirectory = new File("target/scheduler");
        File kahadbDir = new File("target/kahadb");

        for (File directory: new File[]{schedulerDirectory, kahadbDir}) {
            IOHelper.mkdirs(directory);
            IOHelper.deleteChildren(directory);
        }

        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setSchedulerSupport(true);


        jobSchedulerStore = new JobSchedulerStoreImpl();
        jobSchedulerStore.setDirectory(schedulerDirectory);
        jobSchedulerStore.setJournalMaxFileLength(16*1024);

        jobSchedulerStore.setCheckpointInterval(0);
        jobSchedulerStore.setCleanupOnStop(doCleanupOnStop);

        broker.setJobSchedulerStore(jobSchedulerStore);


        kahaDBPersistenceAdapter = new KahaDBPersistenceAdapter();
        kahaDBPersistenceAdapter.setDirectory(kahadbDir);
        kahaDBPersistenceAdapter.setJournalMaxFileLength(16*1024);

        kahaDBPersistenceAdapter.setCleanupInterval(0);
        kahaDBPersistenceAdapter.setCleanupOnStop(doCleanupOnStop);

        broker.setPersistenceAdapter(kahaDBPersistenceAdapter);

        return broker;
    }

    public void produceWithScheduledDelayAndConsume() throws Exception {
        Connection connection = new ActiveMQConnectionFactory("vm://localhost").createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        connection.start();
        final ActiveMQQueue destination = new ActiveMQQueue("QQ");
        final int numMessages = 50;
        final long time = 1000l;
        final byte[] payload = new byte[1024];
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < numMessages; i++) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload);
            bytesMessage.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
            producer.send(bytesMessage);
        }

        MessageConsumer messageConsumer = session.createConsumer(destination);
        for (int i = 0; i < numMessages; i++) {
            assertNotNull(messageConsumer.receive(5000));
        }
        connection.close();

        // let last ack settle
        TimeUnit.SECONDS.sleep(1);

    }
}
