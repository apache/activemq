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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class VerifySteadyEnqueueRate extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(VerifySteadyEnqueueRate.class);

    private static int max_messages = 1000000;
    private final String destinationName = getName() + "_Queue";
    private BrokerService broker;
    final boolean useTopic = false;

    protected static final String payload = new String(new byte[24]);

    @Override
    public void setUp() throws Exception {
        startBroker();
    }

    @Override
    public void tearDown() throws Exception {
        broker.stop();
    }

    @SuppressWarnings("unused")
    public void testEnqueueRateCanMeetSLA() throws Exception {
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
        final int numThreads = 6;

        Runnable runner = new Runnable() {

            @Override
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
                System.out.println("Max Violation = " + max + " - Total SLA violations: "+slaViolations.get()+"/"+total.get()+" ("+String.format("%.6f", 100.0*slaViolations.get()/total.get())+"%)");
            }
        };
        ExecutorService executor = Executors.newCachedThreadPool();

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
        //broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        kaha.setJournalDiskSyncStrategy(JournalDiskSyncStrategy.NEVER.name());
        // Using a bigger journal file size makes he take fewer spikes as it is not switching files as often.
        kaha.setJournalMaxFileLength(1024*1024*100);

        // small batch means more frequent and smaller writes
        kaha.setIndexWriteBatchSize(100);
        // do the index write in a separate thread
        kaha.setEnableIndexWriteAsync(true);

        broker.setPersistenceAdapter(kaha);

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();
        LOG.info("Starting broker..");
    }
}
