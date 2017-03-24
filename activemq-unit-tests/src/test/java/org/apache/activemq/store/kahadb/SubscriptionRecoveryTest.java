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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionRecoveryTest.class);

    private BrokerService service;
    private String connectionUri;
    private ActiveMQConnectionFactory cf;

    private final int MSG_COUNT = 256;

    @Before
    public void setUp() throws IOException, Exception {
        createBroker(true, false);
    }

    public void createBroker(boolean deleteAllMessages, boolean recover) throws Exception {
        service = new BrokerService();
        service.setBrokerName("InactiveSubTest");
        service.setDeleteAllMessagesOnStartup(deleteAllMessages);
        service.setPersistent(true);

        KahaDBPersistenceAdapter pa=new KahaDBPersistenceAdapter();
        File dataFile=new File("KahaDB");
        pa.setDirectory(dataFile);
        pa.setJournalMaxFileLength(10*1024);
        pa.setPreallocationScope(Journal.PreallocationScope.ENTIRE_JOURNAL.name());
        pa.setCheckpointInterval(TimeUnit.SECONDS.toMillis(5));
        pa.setCleanupInterval(TimeUnit.SECONDS.toMillis(5));
        //Delete the index files on recovery
        if (recover) {
            for (File index : FileUtils.listFiles(dataFile, new WildcardFileFilter("*.data"), TrueFileFilter.INSTANCE)) {
                LOG.info("deleting: " + index);
                FileUtils.deleteQuietly(index);
            }
        }

        service.setPersistenceAdapter(pa);
        service.start();
        service.waitUntilStarted();

        connectionUri = "vm://InactiveSubTest?create=false";
        cf = new ActiveMQConnectionFactory(connectionUri);
    }

    private void restartBroker() throws Exception {
        stopBroker();
        createBroker(false, false);
    }

    private void recoverBroker() throws Exception {
        stopBroker();
        createBroker(false, true);
    }

    @After
    public void stopBroker() throws Exception {
        if (service != null) {
            service.stop();
            service.waitUntilStopped();
            service = null;
        }
    }

    @Test
    public void testDurableSubPrefetchRecovered() throws Exception {

        ActiveMQQueue queue = new ActiveMQQueue("MyQueue");
        ActiveMQTopic topic = new ActiveMQTopic("MyDurableTopic");

        // Send to a Queue to create some journal files
        sendMessages(queue);

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());

        createInactiveDurableSub(topic);

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));

        // Now send some more to the queue to create even more files.
        sendMessages(queue);

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());
        assertTrue(getNumberOfJournalFiles() > 1);

        LOG.info("Restarting the broker.");
        restartBroker();
        LOG.info("Restarted the broker.");

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());
        assertTrue(getNumberOfJournalFiles() > 1);

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));

        // Clear out all queue data
        service.getAdminView().removeQueue(queue.getQueueName());

        assertTrue("Less than two journal files expected, was " + getNumberOfJournalFiles(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getNumberOfJournalFiles() <= 2;
            }
        }, TimeUnit.MINUTES.toMillis(2)));

        LOG.info("Sending {} Messages to the Topic.", MSG_COUNT);
        // Send some messages to the inactive destination
        sendMessages(topic);

        LOG.info("Attempt to consume {} messages from the Topic.", MSG_COUNT);
        assertEquals(MSG_COUNT, consumeFromInactiveDurableSub(topic));

        LOG.info("Recovering the broker.");
        recoverBroker();
        LOG.info("Recovering the broker.");

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));
    }

    @Test
    public void testDurableAcksNotDropped() throws Exception {

        ActiveMQQueue queue = new ActiveMQQueue("MyQueue");
        ActiveMQTopic topic = new ActiveMQTopic("MyDurableTopic");

        // Create durable sub in first data file.
        createInactiveDurableSub(topic);

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));

        // Send to a Topic
        sendMessages(topic, 1);

        // Send to a Queue to create some journal files
        sendMessages(queue);

        LOG.info("Before consume there are currently [{}] journal log files.", getNumberOfJournalFiles());

        // Consume all the Messages leaving acks behind.
        consumeDurableMessages(topic, 1);

        LOG.info("After consume there are currently [{}] journal log files.", getNumberOfJournalFiles());

        // Now send some more to the queue to create even more files.
        sendMessages(queue);

        LOG.info("More Queued. There are currently [{}] journal log files.", getNumberOfJournalFiles());
        assertTrue(getNumberOfJournalFiles() > 1);

        LOG.info("Restarting the broker.");
        restartBroker();
        LOG.info("Restarted the broker.");

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());
        assertTrue(getNumberOfJournalFiles() > 1);

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));

        // Clear out all queue data
        service.getAdminView().removeQueue(queue.getQueueName());

        assertTrue("Less than three journal file expected, was " + getNumberOfJournalFiles(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getNumberOfJournalFiles() <= 3;
            }
        }, TimeUnit.MINUTES.toMillis(3)));

        // See if we receive any message they should all be acked.
        tryConsumeExpectNone(topic);

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());

        LOG.info("Recovering the broker.");
        recoverBroker();
        LOG.info("Recovering the broker.");

        LOG.info("There are currently [{}] journal log files.", getNumberOfJournalFiles());

        assertTrue("Should have an inactive durable sub", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                ObjectName[] subs = service.getAdminView().getInactiveDurableTopicSubscribers();
                return subs != null && subs.length == 1 ? true : false;
            }
        }));

        // See if we receive any message they should all be acked.
        tryConsumeExpectNone(topic);

        assertTrue("Less than three journal file expected, was " + getNumberOfJournalFiles(), Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getNumberOfJournalFiles() == 1;
            }
        }, TimeUnit.MINUTES.toMillis(1)));
    }

    private int getNumberOfJournalFiles() throws IOException {
        Collection<DataFile> files =
            ((KahaDBPersistenceAdapter) service.getPersistenceAdapter()).getStore().getJournal().getFileMap().values();
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
    }

    private void consumeDurableMessages(Topic topic, int count) throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID("Inactive");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "Inactive");
        connection.start();
        for (int i = 0; i < count; ++i) {
           if (consumer.receive(TimeUnit.SECONDS.toMillis(10)) == null) {
               fail("should have received a message");
           }
        }
        consumer.close();
        connection.close();
    }

    private void tryConsumeExpectNone(Topic topic) throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID("Inactive");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "Inactive");
        connection.start();
        if (consumer.receive(TimeUnit.SECONDS.toMillis(10)) != null) {
            fail("Should be no messages for this durable.");
        }
        consumer.close();
        connection.close();
    }

    private int consumeFromInactiveDurableSub(Topic topic) throws Exception {
        Connection connection = cf.createConnection();
        connection.setClientID("Inactive");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "Inactive");

        int count = 0;

        while (consumer.receive(10000) != null) {
            count++;
        }

        consumer.close();
        connection.close();

        return count;
    }

    private void sendMessages(Destination destination) throws Exception {
        sendMessages(destination, MSG_COUNT);
    }

    private void sendMessages(Destination destination, int count) throws Exception {
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < count; ++i) {
            TextMessage message = session.createTextMessage("Message #" + i + " for destination: " + destination);
            producer.send(message);
        }
        connection.close();
    }

}