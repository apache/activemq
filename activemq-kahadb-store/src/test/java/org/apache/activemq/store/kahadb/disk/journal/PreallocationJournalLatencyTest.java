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
package org.apache.activemq.store.kahadb.disk.journal;

import org.apache.activemq.management.TimeStatisticImpl;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class PreallocationJournalLatencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(PreallocationJournalLatencyTest.class);
    final Random rand = new Random();

    @Test
    public void preallocationLatency() throws Exception {

        TimeStatisticImpl sparse = executeTest(Journal.PreallocationStrategy.SPARSE_FILE.name());
        TimeStatisticImpl chunked_zeros = executeTest(Journal.PreallocationStrategy.CHUNKED_ZEROS.name());
        //TimeStatisticImpl zeros = executeTest(Journal.PreallocationStrategy.ZEROS.name());
        TimeStatisticImpl kernel = executeTest(Journal.PreallocationStrategy.OS_KERNEL_COPY.name());

        LOG.info("  sparse: " + sparse);
        LOG.info(" chunked: " + chunked_zeros);
        //LOG.info("   zeros: " + zeros);
        LOG.info("  kernel: " + kernel);

    }

    private TimeStatisticImpl executeTest(String preallocationStrategy)throws Exception {
        int randInt = rand.nextInt(100);
        File dataDirectory = new File("./target/activemq-data/kahadb" + randInt);

        final KahaDBStore store = new KahaDBStore();
        store.setCheckpointInterval(5000);
        store.setJournalMaxFileLength(32*1204*1024);
        store.deleteAllMessages();
        store.setDirectory(dataDirectory);
        store.setPreallocationStrategy(preallocationStrategy);
        store.setPreallocationScope(Journal.PreallocationScope.ENTIRE_JOURNAL_ASYNC.name());
        store.start();

        final File journalLog = new File(dataDirectory, "db-1.log");
        assertTrue("file exists", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return journalLog.exists();
            }
        }));

        final Journal journal = store.getJournal();
        ByteSequence byteSequence = new ByteSequence(new byte[16*1024]);

        TimeStatisticImpl timeStatistic = new TimeStatisticImpl("append", "duration");
        for (int i=0;i<5000; i++) {
            final long start = System.currentTimeMillis();
            journal.write(byteSequence, true);
            timeStatistic.addTime(System.currentTimeMillis() - start);
        }
        LOG.info("current journal dataFile id: " + journal.getCurrentDataFileId());
        store.stop();
        return timeStatistic;
    }

}
