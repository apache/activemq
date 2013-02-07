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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.store.kahadb.disk.journal.FileAppender;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBFastEnqueueTest {
    private static final Logger LOG = LoggerFactory.getLogger(KahaDBFastEnqueueTest.class);
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    KahaDBPersistenceAdapter kahaDBPersistenceAdapter;
    private final Destination destination = new ActiveMQQueue("Test");
    private final String payloadString = new String(new byte[6*1024]);
    private final boolean useBytesMessage= true;
    private final int parallelProducer = 20;
    private final Vector<Exception> exceptions = new Vector<Exception>();
    long toSend = 10000;

    // use with:
    // -Xmx4g -Dorg.apache.kahadb.journal.appender.WRITE_STAT_WINDOW=10000 -Dorg.apache.kahadb.journal.CALLER_BUFFER_APPENDER=true
    @Test
    public void testPublishNoConsumer() throws Exception {

        startBroker(true, 10);

        final AtomicLong sharedCount = new AtomicLong(toSend);
        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0; i< parallelProducer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        publishMessages(sharedCount, 0);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MINUTES);
        assertTrue("Producers done in time", executorService.isTerminated());
        assertTrue("No exceptions: " + exceptions, exceptions.isEmpty());
        long totalSent  = toSend * payloadString.length();

        double duration =  System.currentTimeMillis() - start;
        stopBroker();
        LOG.info("Duration:                " + duration + "ms");
        LOG.info("Rate:                       " + (toSend * 1000/duration) + "m/s");
        LOG.info("Total send:             " + totalSent);
        LOG.info("Total journal write: " + kahaDBPersistenceAdapter.getStore().getJournal().length());
        LOG.info("Total index size " + kahaDBPersistenceAdapter.getStore().getPageFile().getDiskSize());
        LOG.info("Total store size: " + kahaDBPersistenceAdapter.size());
        LOG.info("Journal writes %:    " + kahaDBPersistenceAdapter.getStore().getJournal().length() / (double)totalSent * 100 + "%");

        restartBroker(0, 1200000);
        consumeMessages(toSend);
    }

    @Test
    public void testPublishNoConsumerNoCheckpoint() throws Exception {

        toSend = 100;
        startBroker(true, 0);

        final AtomicLong sharedCount = new AtomicLong(toSend);
        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0; i< parallelProducer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        publishMessages(sharedCount, 0);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MINUTES);
        assertTrue("Producers done in time", executorService.isTerminated());
        assertTrue("No exceptions: " + exceptions, exceptions.isEmpty());
        long totalSent  = toSend * payloadString.length();

        broker.getAdminView().gc();


        double duration =  System.currentTimeMillis() - start;
        stopBroker();
        LOG.info("Duration:                " + duration + "ms");
        LOG.info("Rate:                       " + (toSend * 1000/duration) + "m/s");
        LOG.info("Total send:             " + totalSent);
        LOG.info("Total journal write: " + kahaDBPersistenceAdapter.getStore().getJournal().length());
        LOG.info("Total index size " + kahaDBPersistenceAdapter.getStore().getPageFile().getDiskSize());
        LOG.info("Total store size: " + kahaDBPersistenceAdapter.size());
        LOG.info("Journal writes %:    " + kahaDBPersistenceAdapter.getStore().getJournal().length() / (double)totalSent * 100 + "%");

        restartBroker(0, 0);
        consumeMessages(toSend);
    }

    private void consumeMessages(long count) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setWatchTopicAdvisories(false);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        for (int i=0; i<count; i++) {
            assertNotNull("got message "+ i, consumer.receive(10000));
        }
        assertNull("none left over", consumer.receive(2000));
    }

    private void restartBroker(int restartDelay, int checkpoint) throws Exception {
        stopBroker();
        TimeUnit.MILLISECONDS.sleep(restartDelay);
        startBroker(false, checkpoint);
    }

    @Before
    public void setProps() {
        System.setProperty(Journal.CALLER_BUFFER_APPENDER, Boolean.toString(true));
        System.setProperty(FileAppender.PROPERTY_LOG_WRITE_STAT_WINDOW, "10000");
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        System.clearProperty(Journal.CALLER_BUFFER_APPENDER);
        System.clearProperty(FileAppender.PROPERTY_LOG_WRITE_STAT_WINDOW);
    }

    final double sampleRate = 100000;
    private void publishMessages(AtomicLong count, int expiry) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setWatchTopicAdvisories(false);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        Long start = System.currentTimeMillis();
        long i = 0l;
        while ( (i=count.getAndDecrement()) > 0) {
            Message message = null;
            if (useBytesMessage) {
                message = session.createBytesMessage();
                ((BytesMessage) message).writeBytes(payloadString.getBytes());
            } else {
                message = session.createTextMessage(payloadString);
            }
            producer.send(message, DeliveryMode.PERSISTENT, 5, expiry);
            if (i != toSend && i%sampleRate == 0) {
                long now = System.currentTimeMillis();
                LOG.info("Remainder: " + i + ", rate: " + sampleRate * 1000 / (now - start) + "m/s" );
                start = now;
            }
        }
        connection.syncSendPacket(new ConnectionControl());
        connection.close();
    }

    public void startBroker(boolean deleteAllMessages, int checkPointPeriod) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setEnableJournalDiskSyncs(false);
        // defer checkpoints which require a sync
        kahaDBPersistenceAdapter.setCleanupInterval(checkPointPeriod);
        kahaDBPersistenceAdapter.setCheckpointInterval(checkPointPeriod);

        // optimise for disk best batch rate
        kahaDBPersistenceAdapter.setJournalMaxWriteBatchSize(24*1024*1024); //4mb default
        kahaDBPersistenceAdapter.setJournalMaxFileLength(128*1024*1024); // 32mb default
        // keep index in memory
        kahaDBPersistenceAdapter.setIndexCacheSize(500000);
        kahaDBPersistenceAdapter.setIndexWriteBatchSize(500000);
        kahaDBPersistenceAdapter.setEnableIndexRecoveryFile(false);
        kahaDBPersistenceAdapter.setEnableIndexDiskSyncs(false);

        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();

        String options = "?jms.watchTopicAdvisories=false&jms.useAsyncSend=true&jms.alwaysSessionAsync=false&jms.dispatchAsync=false&socketBufferSize=131072&ioBufferSize=16384&wireFormat.tightEncodingEnabled=false&wireFormat.cacheSize=8192";
        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri() + options);
    }

    @Test
    public void testRollover() throws Exception {
        byte flip = 0x1;
        for (long i=0; i<Short.MAX_VALUE; i++) {
            assertEquals("0 @:" + i, 0, flip ^= 1);
            assertEquals("1 @:" + i, 1, flip ^= 1);
        }
    }
}