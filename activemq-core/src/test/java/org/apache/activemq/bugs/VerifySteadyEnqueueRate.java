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

import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VerifySteadyEnqueueRate extends TestCase {

    private static final Log LOG = LogFactory
            .getLog(VerifySteadyEnqueueRate.class);

    private static int max_messages = 1000000;
    private String destinationName = getName() + "_Queue";
    private BrokerService broker;
    final boolean useTopic = false;

    private boolean useAMQPStore = false;
    protected static final String payload = new String(new byte[24]);

    public void setUp() throws Exception {
        startBroker();
    }

    public void tearDown() throws Exception {
        broker.stop();
    }

    public void testForDataFileNotDeleted() throws Exception {
        if (true) {
            return;
        }
        doTestEnqueue(false);
    }

    private void doTestEnqueue(final boolean transacted) throws Exception {
        final long min = 100;
        final AtomicLong total = new AtomicLong(0);
        final AtomicLong slaViolations = new AtomicLong(0);
        final AtomicLong max = new AtomicLong(0);
        long reportTime = 0;

        Runnable runner = new Runnable() {

            public void run() {
                try {
                    MessageSender producer = new MessageSender(destinationName,
                            createConnection(), transacted, useTopic);

                    for (int i = 0; i < max_messages; i++) {
                        long startT = System.currentTimeMillis();
                        producer.send(payload);
                        long endT = System.currentTimeMillis();
                        long duration = endT - startT;

                        total.incrementAndGet();
                        
                        if (duration > max.get()) {
                            max.set(duration);
                        }

                        if (duration > min) {
                        	slaViolations.incrementAndGet();
                            System.err.println("SLA violation @ "+Thread.currentThread().getName()
                                    + " "
                                    + DateFormat.getTimeInstance().format(
                                            new Date(startT)) + " at message "
                                    + i + " send time=" + duration
                                    + " - Total SLA violations: "+slaViolations.get()+"/"+total.get()+" ("+String.format("%.6f", 100.0*slaViolations.get()/total.get())+"%)");
                        }
                    }

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("max = " + max);
            }
        };
        ExecutorService executor = Executors.newCachedThreadPool();
        int numThreads = 6;
        for (int i = 0; i < numThreads; i++) {
            executor.execute(runner);
        }
        
        executor.shutdown();
        while(!executor.isTerminated()) {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri());
        return factory.createConnection();
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);

        if (useAMQPStore) {
            AMQPersistenceAdapterFactory factory = (AMQPersistenceAdapterFactory) broker
                    .getPersistenceFactory();
            // ensure there are a bunch of data files but multiple entries in
            // each
            // factory.setMaxFileLength(1024 * 20);
            // speed up the test case, checkpoint an cleanup early and often
            // factory.setCheckpointInterval(500);
            factory.setCleanupInterval(1000 * 60 * 30);
            factory.setSyncOnWrite(false);

            // int indexBinSize=262144; // good for 6M
            int indexBinSize = 1024;
            factory.setIndexMaxBinSize(indexBinSize * 2);
            factory.setIndexBinSize(indexBinSize);
            factory.setIndexPageSize(192 * 20);
        } else {
            KahaDBStore kaha = new KahaDBStore();
            kaha.setDirectory(new File("target/activemq-data/kahadb"));
            // The setEnableJournalDiskSyncs(false) setting is a little dangerous right now, as I have not verified 
            // what happens if the index is updated but a journal update is lost.
            // Index is going to be in consistent, but can it be repaired?
            kaha.setEnableJournalDiskSyncs(false);
            // Using a bigger journal file size makes he take fewer spikes as it is not switching files as often.
            kaha.getJournal().setMaxFileLength(1024*1024*100);
            kaha.getPageFile().setWriteBatchSize(100);
            kaha.getPageFile().setEnableWriteThread(true);
            broker.setPersistenceAdapter(kaha);
        }

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();
        LOG.info("Starting broker..");
    }
}
