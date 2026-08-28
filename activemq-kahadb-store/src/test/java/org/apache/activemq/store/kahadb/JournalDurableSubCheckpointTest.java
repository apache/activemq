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
package org.apache.activemq.store.kahadb;

import static org.apache.activemq.store.kahadb.JournalCorruptionEofIndexRecoveryTest.drain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import java.io.IOException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JournalDurableSubCheckpointTest {

    private static final Logger LOG = LoggerFactory.getLogger(JournalDurableSubCheckpointTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String payload = new String(new byte[1024]);
    private BrokerService broker = null;
    private KahaDBPersistenceAdapter adapter;
    private KahaDBStore store;
    private ActiveMQConnectionFactory cf;

    protected void startBroker() throws Exception {
        doStartBroker(true, false);
    }

    protected void restartBroker(boolean recoverIndex) throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        doStartBroker(false, recoverIndex);
    }

    private void doStartBroker(boolean delete, boolean recoverIndex) throws Exception {
        doCreateBroker(delete, recoverIndex);
        LOG.info("Starting broker..");
        broker.start();
        store = adapter.getStore();
        cf = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
    }

    private void doCreateBroker(boolean delete, boolean recoverIndex) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setDataDirectory(temporaryFolder.getRoot().getAbsolutePath());
        configurePersistence(broker, recoverIndex);
    }

    protected void configurePersistence(BrokerService brokerService, boolean recoverIndex) throws Exception {
        adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);
        adapter.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.PERIODIC.name());
        adapter.setForceRecoverIndex(recoverIndex);

        // manual cleanup
        adapter.setCheckpointInterval(0);
        adapter.setCleanupInterval(0);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testEmptyDurableSubsRewrite() throws Exception {
        startBroker();

        // send a single message to a queue to block the first journal file from GC
        sendMessages(new ActiveMQQueue("test.queue.1"), 1);

        ActiveMQQueue testQueue2 = new ActiveMQQueue("test.queue.2");
        int numSend = 50;
        int numTopics = 4;
        int durablesCreated = 0;

        // Send messages to a second destination while creating new empty durables
        // every 5 messages so durables are scattered in all files
        for (int i = 0; i < numTopics; i++) {
            durablesCreated += mixMessagesWithDurables(testQueue2, new ActiveMQTopic("test.topic." + i), "test" + i, numSend);
        }

        int numFilesAfterSend = getNumberOfJournalFiles();
        LOG.info("Num journal files: {} ", numFilesAfterSend);

        // verify over 25 files were created
        assertTrue("more than 20 files", numFilesAfterSend > 25);

        // consumer all the messages on the second destination, but not the first
        // this will leave a single message in file 1 but all the durables are empty
        int received = tryConsume(testQueue2, numSend * numTopics);
        assertEquals("all message received", numSend * numTopics, received);

        int numFilesAfterReceive = getNumberOfJournalFiles();
        LOG.info("Num journal files before gc: {}", numFilesAfterReceive);

        // force gc
        store.checkpoint(true);

        // We should have cleaned up the majority of files as the improved durable GC algorithm
        // will check all files to advance empty durables
        int numFilesAfterGc = getNumberOfJournalFiles();
        LOG.info("Num journal files after gc: {}", numFilesAfterGc);
        assertTrue("less than 5 files", numFilesAfterGc < 5);

        // restart the broker and rebuild the index by rescanning the remaining journal file
        restartBroker(true);

        // Verify all the subscriptions come back which prove they were moved correctly
        RegionBroker regionBroker = (RegionBroker) broker.getRegionBroker();
        TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        assertEquals(durablesCreated, topicRegion.getDurableSubscriptions().size());
    }

    @Test
    public void testNonEmptyDurableSubsRewrite() throws Exception {
        startBroker();

        ActiveMQQueue testQueue = new ActiveMQQueue("test.queue");
        int numSend = 50;
        int numTopics = 4;

        // create durables mixed with messages so it creates lots of files and scatters
        // the durables
        for (int i = 0; i < numTopics; i++) {
            var topic =  new ActiveMQTopic("test.topic." + i);
            mixMessagesWithDurables(testQueue, topic, "test" + i, numSend);
            // publish a single message for each durable to prevent GC
            sendMessages(topic, 1);
        }

        // Make sure we generated a bunch of files
        int numFilesAfterSend = getNumberOfJournalFiles();
        assertTrue("more than 25 files", numFilesAfterSend > 25);

        // drain the original queue so only the 4 messages are left on the topics
        int received = tryConsume(testQueue, numSend * numTopics);
        assertEquals("all message received", numSend * numTopics, received);

        int numFilesAfterReceive = getNumberOfJournalFiles();
        LOG.info("Num journal files before gc: {}", numFilesAfterReceive);

        // force gc
        store.checkpoint(true);

        // The exact number can vary a bit sometimes depending on when things like the ack map
        // or producer audit map get checkpointed which can impact if a certain file can be removed
        // but we should still have more than 25
        int numFilesAfterGc = getNumberOfJournalFiles();
        LOG.info("Num journal files after gc: {}", numFilesAfterGc);
        assertTrue("more than 25 files", numFilesAfterGc > 25);
    }

    private int getNumberOfJournalFiles() throws IOException {
        return Math.toIntExact(store.getJournal().getFileMap().size());
    }

    private void sendMessages(Destination messageDest, int numToSend) throws Exception {
        try (Connection connection = cf.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(messageDest);
            for (int i = 0; i < numToSend; i++) {
                producer.send(createMessage(session, i));
            }
        }
    }

    private int mixMessagesWithDurables(Destination messageDest, ActiveMQTopic durablesTopic, String clientId, int numToSend) throws Exception {
        int numDurables = 0;
        try (Connection connection = cf.createConnection()) {
            connection.setClientID(clientId);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // always just publish to the same queue because the message dest doesn't matter
            MessageProducer producer = session.createProducer(messageDest);
            for (int i = 0; i < numToSend; i++) {
                producer.send(createMessage(session, i));
                if (i % 5 == 0) {
                    session.createDurableSubscriber(durablesTopic, "sub" + i);
                    numDurables++;
                    LOG.info("Created durable sub: {} on {}", clientId + ":sub" + i, durablesTopic);
                }
            }
        }
        return numDurables;
    }

    private int tryConsume(Destination destination, int numToGet) throws Exception {
        return drain(cf, destination, numToGet);
    }

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }

}
