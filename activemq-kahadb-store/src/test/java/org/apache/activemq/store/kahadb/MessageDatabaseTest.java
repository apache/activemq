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

import org.apache.activemq.ActiveMQMessageAuditNoSync;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.ByteSequence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq.store.kahadb.disk.journal.Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
import static org.junit.Assert.*;

public class MessageDatabaseTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCheckPointCleanupErrorBubblesUp() throws Exception {

        CountDownLatch traceCommandComplete = new CountDownLatch(1);
        KahaDBStore kaha = new KahaDBStore() {
            public Journal createJournal() {
                Journal journal = new Journal() {
                    public boolean isChecksum() {
                        // allow trace command on start

                        if (traceCommandComplete.getCount() > 0) {
                            traceCommandComplete.countDown();
                            return false;
                        }

                        // called from processQ, we can throw here to error out the async write
                        throw new RuntimeException("Fail with error on processQ");
                    }
                };
                journal.setDirectory(directory);
                return journal;
            }
        };
        kaha.setDirectory(new File(temporaryFolder.getRoot(), "kaha"));
        kaha.setCheckpointInterval(0l); // disable periodic checkpoint
        kaha.setBrokerService(new BrokerService() {
            public void handleIOException(IOException exception) {
                exception.printStackTrace();
            }
        });
        kaha.start();

        assertTrue(traceCommandComplete.await(5, TimeUnit.SECONDS));

        try {
            kaha.checkpoint(false);
            fail("expect error on first store from checkpoint");
        } catch (Exception expected) {
        }

        assertNull("audit location should be null", kaha.getMetadata().producerSequenceIdTrackerLocation);
    }

    @Test
    public void testRecoverCheckOnBatchBoundary() throws Exception {

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File(temporaryFolder.getRoot(), "kaha2"));
        kaha.setCheckpointInterval(0l); // disable periodic checkpoint
        kaha.setCheckForCorruptJournalFiles(true);
        kaha.setChecksumJournalFiles(true);
        kaha.setMaxFailoverProducersToTrack(10);
        kaha.setBrokerService(new BrokerService() {
            public void handleIOException(IOException exception) {
                exception.printStackTrace();
            }
        });
        kaha.start();

        // track original metadata reference to ensure it is read from the journal on recovery
        ActiveMQMessageAuditNoSync auditToVerify = kaha.getMetadata().producerSequenceIdTracker;
        final String messsageId = "1:1:1:1";
        auditToVerify.isDuplicate(messsageId);

        ByteSequence byteSequence = new ByteSequence(new byte[DEFAULT_MAX_WRITE_BATCH_SIZE - 110]);
        kaha.getJournal().write(byteSequence, false);
        kaha.getJournal().write(byteSequence, false);

        kaha.stop();
        try {
            kaha.start();
            assertTrue("Value from journal recovered ok", kaha.getMetadata().producerSequenceIdTracker.isDuplicate(messsageId));
        } finally {
            kaha.stop();
        }
    }
}