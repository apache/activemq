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
import org.junit.After;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static junit.framework.Assert.*;

public class KahaDBFastEnqueueTest {
    private static final Logger LOG = LoggerFactory.getLogger(KahaDBFastEnqueueTest.class);
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    KahaDBPersistenceAdapter kahaDBPersistenceAdapter;
    private Destination destination = new ActiveMQQueue("Test");
    private String payloadString = new String(new byte[6*1024]);
    private boolean useBytesMessage= true;
    private final int parallelProducer = 2;
    private Vector<Exception> exceptions = new Vector<Exception>();
    final long toSend = 500000;

    @Ignore("not ready yet, exploring getting broker disk bound")
    public void testPublishNoConsumer() throws Exception {

        startBroker(true);

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

        //System.out.println("Pre shutdown: Index totalWritten:       " + kahaDBPersistenceAdapter.getStore().getPageFile().totalWritten);

        double duration =  System.currentTimeMillis() - start;
        stopBroker();
        System.out.println("Duration:                " + duration + "ms");
        System.out.println("Rate:                       " + (toSend * 1000/duration) + "m/s");
        System.out.println("Total send:             " + totalSent);
        System.out.println("Total journal write: " + kahaDBPersistenceAdapter.getStore().getJournal().length());
        //System.out.println("Total index write:   " + kahaDBPersistenceAdapter.getStore().getPageFile().totalWritten);
        System.out.println("Journal writes %:    " + kahaDBPersistenceAdapter.getStore().getJournal().length() / (double)totalSent * 100 + "%");
        //System.out.println("Index writes %:       " + kahaDBPersistenceAdapter.getStore().getPageFile().totalWritten / (double)totalSent * 100 + "%");

        //restartBroker(0);
        //consumeMessages(toSend);
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

    private void restartBroker(int restartDelay) throws Exception {
        stopBroker();
        TimeUnit.MILLISECONDS.sleep(restartDelay);
        startBroker(false);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
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

    public void startBroker(boolean deleteAllMessages) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setEnableJournalDiskSyncs(false);
        // defer checkpoints which require a sync
        kahaDBPersistenceAdapter.setCleanupInterval(20 * 60 * 1000);
        kahaDBPersistenceAdapter.setCheckpointInterval(20 * 60 * 1000);

        // optimise for disk best batch rate
        //kahaDBPersistenceAdapter.setJournalMaxWriteBatchSize(128*1024); //4mb default
        kahaDBPersistenceAdapter.setJournalMaxFileLength(1024*1024*1024); // 32mb default
        // keep index in memory
        kahaDBPersistenceAdapter.setIndexCacheSize(500000);
        kahaDBPersistenceAdapter.setIndexWriteBatchSize(500000);

        broker.setUseJmx(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();

        String options = "?jms.watchTopicAdvisories=false&jms.useAsyncSend=true&jms.alwaysSessionAsync=false&jms.dispatchAsync=false&socketBufferSize=131072&ioBufferSize=16384&wireFormat.tightEncodingEnabled=false&wireFormat.cacheSize=8192";
        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri() + options);
    }
}