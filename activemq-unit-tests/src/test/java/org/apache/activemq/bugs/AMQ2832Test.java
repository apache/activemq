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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2832Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2832Test.class);

    BrokerService broker = null;
    private ActiveMQConnectionFactory cf;
    private final Destination destination = new ActiveMQQueue("AMQ2832Test");
    private String connectionUri;

    protected void startBroker() throws Exception {
        doStartBroker(true, false);
    }

    protected void restartBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        doStartBroker(false, false);
    }

    protected void recoverBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        doStartBroker(false, true);
    }

    private void doStartBroker(boolean delete, boolean recover) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:0");

        configurePersistence(broker, recover);

        connectionUri = "vm://localhost?create=false";
        cf = new ActiveMQConnectionFactory(connectionUri);

        broker.start();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService, boolean recover) throws Exception {
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(5000);
        adapter.setCleanupInterval(5000);
        adapter.setPreallocationScope(Journal.PreallocationScope.ENTIRE_JOURNAL.name());

        if (recover) {
            adapter.setForceRecoverIndex(true);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

   /**
    * Scenario:
    * db-1.log has an unacknowledged message,
    * db-2.log contains acks for the messages from db-1.log,
    * db-3.log contains acks for the messages from db-2.log
    *
    * Expected behavior: since db-1.log is blocked, db-2.log and db-3.log should not be removed during the cleanup.
    * Current situation on 5.10.0, 5.10.1 is that db-3.log is removed causing all messages from db-2.log, whose acks were in db-3.log, to be replayed.
    *
    * @throws Exception
    */
    @Test
    public void testAckChain() throws Exception {
        startBroker();

        makeAckChain();

        broker.stop();
        broker.waitUntilStopped();

        recoverBroker();

        StagedConsumer consumer = new StagedConsumer();
        Message message = consumer.receive(1);
        assertNotNull("One message stays unacked from db-1.log", message);
        message.acknowledge();
        message = consumer.receive(1);
        assertNull("There should not be any unconsumed messages any more", message);
        consumer.close();
    }

    private void makeAckChain() throws Exception {
        StagedConsumer consumer = new StagedConsumer();
        // file #1
        produceMessagesToConsumeMultipleDataFiles(5);
        // acknowledge first 2 messages and leave the 3rd one unacknowledged blocking db-1.log
        consumer.receive(3);

        // send messages by consuming and acknowledging every message right after sent in order to get KahadbAdd and Remove command to be saved together
        // this is necessary in order to get KahaAddMessageCommand to be saved in one db file and the corresponding KahaRemoveMessageCommand in the next one
        produceAndConsumeImmediately(20, consumer);
        consumer.receive(2).acknowledge(); // consume and ack the last 2 unconsumed

        // now we have 3 files written and started with #4
        consumer.close();
    }

    @Test
    public void testNoRestartOnMissingAckDataFile() throws Exception {
        startBroker();

        // reuse scenario from previous test
        makeAckChain();

        File dataDir = broker.getPersistenceAdapter().getDirectory();
        broker.stop();
        broker.waitUntilStopped();

        File secondLastDataFile = new File(dataDir, "db-3.log");
        LOG.info("Whacking data file with acks: " + secondLastDataFile);
        secondLastDataFile.delete();

        try {
            doStartBroker(false, false);
            fail("Expect failure to start with corrupt journal");
        } catch (IOException expected) {
        }
    }


   private void produceAndConsumeImmediately(int numOfMsgs, StagedConsumer consumer) throws Exception {
      for (int i = 0; i < numOfMsgs; i++) {
         produceMessagesToConsumeMultipleDataFiles(1);
         consumer.receive(1).acknowledge();
      }
   }

   @Test
    public void testAckRemovedMessageReplayedAfterRecovery() throws Exception {

        startBroker();

        StagedConsumer consumer = new StagedConsumer();
        int numMessagesAvailable = produceMessagesToConsumeMultipleDataFiles(20);
        // this will block the reclaiming of one data file
        Message firstUnacked = consumer.receive(10);
        LOG.info("first unacked: " + firstUnacked.getJMSMessageID());
        Message secondUnacked = consumer.receive(1);
        LOG.info("second unacked: " + secondUnacked.getJMSMessageID());
        numMessagesAvailable -= 11;

        numMessagesAvailable += produceMessagesToConsumeMultipleDataFiles(10);
        // ensure ack is another data file
        LOG.info("Acking firstUnacked: " + firstUnacked.getJMSMessageID());
        firstUnacked.acknowledge();

        numMessagesAvailable += produceMessagesToConsumeMultipleDataFiles(10);

        consumer.receive(numMessagesAvailable).acknowledge();

        // second unacked should keep first data file available but journal with the first ack
        // may get whacked
        consumer.close();

        broker.stop();
        broker.waitUntilStopped();

        recoverBroker();

        consumer = new StagedConsumer();
        // need to force recovery?

        Message msg = consumer.receive(1, 5);
        assertNotNull("One messages left after recovery", msg);
        msg.acknowledge();

        // should be no more messages
        msg = consumer.receive(1, 5);
        assertEquals("Only one messages left after recovery: " + msg, null, msg);
        consumer.close();
    }

    @Test
    public void testAlternateLossScenario() throws Exception {

        startBroker();
        PersistenceAdapter pa  = broker.getPersistenceAdapter();
        if (pa instanceof LevelDBStore) {
            return;
        }

        ActiveMQQueue queue = new ActiveMQQueue("MyQueue");
        ActiveMQQueue disposable = new ActiveMQQueue("MyDisposableQueue");
        ActiveMQTopic topic = new ActiveMQTopic("MyDurableTopic");

        // This ensure that data file 1 never goes away.
        createInactiveDurableSub(topic);
        assertEquals(1, getNumberOfJournalFiles());

        // One Queue Message that will be acked in another data file.
        produceMessages(queue, 1);
        assertEquals(1, getNumberOfJournalFiles());

        // Add some messages to consume space
        produceMessages(disposable, 50);

        int dataFilesCount = getNumberOfJournalFiles();
        assertTrue(dataFilesCount > 1);

        // Create an ack for the single message on this queue
        drainQueue(queue);

        // Add some more messages to consume space beyond tha data file with the ack
        produceMessages(disposable, 50);

        assertTrue(dataFilesCount < getNumberOfJournalFiles());
        dataFilesCount = getNumberOfJournalFiles();

        restartBroker();

        // Clear out all queue data
        broker.getAdminView().removeQueue(disposable.getQueueName());

        // Once this becomes true our ack could be lost.
        assertTrue("Less than three journal file expected, was " + getNumberOfJournalFiles(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getNumberOfJournalFiles() <= 3;
            }
        }, TimeUnit.MINUTES.toMillis(3)));

        // Recover and the Message should not be replayed but if the old MessageAck is lost
        // then it could be.
        recoverBroker();

        assertTrue(drainQueue(queue) == 0);
    }

    private int getNumberOfJournalFiles() throws IOException {

        Collection<DataFile> files =
            ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
        LOG.info("Data files: " + files);
        int reality = 0;
        for (DataFile file : files) {
            if (file != null) {
                reality++;
            }
        }

        return reality;
    }

    private void createInactiveDurableSub(Topic topic) throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID("Inactive");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "Inactive");
        consumer.close();
        connection.close();
        produceMessages(topic, 1);
    }

    private int drainQueue(Queue queue) throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID("Inactive");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);
        int count = 0;
        while (consumer.receive(5000) != null) {
            count++;
        }
        consumer.close();
        connection.close();
        return count;
    }

    private int produceMessages(Destination destination, int numToSend) throws Exception {
        int sent = 0;
        Connection connection = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri()).createConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < numToSend; i++) {
                producer.send(createMessage(session, i));
                sent++;
            }
        } finally {
            connection.close();
        }

        return sent;
    }

    private int produceMessagesToConsumeMultipleDataFiles(int numToSend) throws Exception {
        return produceMessages(destination, numToSend);
    }

    final String payload = new String(new byte[1024]);

    private Message createMessage(Session session, int i) throws Exception {
        return session.createTextMessage(payload + "::" + i);
    }

    private class StagedConsumer {
        Connection connection;
        MessageConsumer consumer;

        StagedConsumer() throws Exception {
            connection = new ActiveMQConnectionFactory("failover://" +
                    broker.getTransportConnectors().get(0).getConnectUri().toString()).createConnection();
            connection.start();
            consumer = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE).createConsumer(destination);
        }

        public Message receive(int numToReceive) throws Exception {
            return receive(numToReceive, 2);
        }

        public Message receive(int numToReceive, int timeoutInSeconds) throws Exception {
            Message msg = null;
            for (; numToReceive > 0; numToReceive--) {

                do  {
                    msg = consumer.receive(1*1000);
                } while (msg == null && --timeoutInSeconds > 0);

                if (numToReceive > 1) {
                    msg.acknowledge();
                }

                if (msg != null) {
                    LOG.debug("received: " + msg.getJMSMessageID());
                }
            }
            // last message, unacked
            return msg;
        }

        void close() throws JMSException {
            consumer.close();
            connection.close();
        }
    }
}
