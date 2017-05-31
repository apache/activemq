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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.concurrent.TimeUnit;


import static org.junit.Assert.assertTrue;

public class KahaDBDeleteLockTest {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBDeleteLockTest.class);

    protected BrokerService master;

    protected KahaDBPersistenceAdapter masterPersistenceAdapter = new KahaDBPersistenceAdapter();

    private final File testDataDir = new File("target/activemq-data/KahaDBDeleteLockTest");
    private final File kahaDataDir = new File(testDataDir, "kahadb");

    @Before
    public void createMaster() throws Exception{
        master = new BrokerService();
        master.setDeleteAllMessagesOnStartup(true);
        master.setBrokerName("Master");
        master.setDataDirectoryFile(testDataDir);

        masterPersistenceAdapter.setDirectory(kahaDataDir);
        masterPersistenceAdapter.setLockKeepAlivePeriod(500);

        // ensure broker creates the file
        File lockFile = new File(kahaDataDir, "lock");
        if (lockFile.exists()) {
            lockFile.delete();
        }

        master.setPersistenceAdapter(masterPersistenceAdapter);
        master.start();
        master.waitUntilStarted();
    }

    @After
    public void stopBrokerJustInCase() throws Exception {
        if (master != null) {
            master.stop();
            master.waitUntilStopped();
        }
    }

    /**
     * Deletes the lock file and makes sure that the broken stops.
     * @throws Exception
     */
    @Test
    public void testLockFileDelete() throws Exception {
        assertTrue(master.isStarted());

        //Delete the lock file
        File lockFile = new File(kahaDataDir, "lock");

        if(lockFile.exists()) {
            lockFile.delete();
        }

        assertTrue("Master stops on lock file delete", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return master.isStopped();
            }
        }));
    }

    /**
     * Modifies the lock file so that the last modified date is not the same when the broker obtained the lock.
     * This should force the broker to stop.
     * @throws Exception
     */
    @Test
    public void testModifyLockFile() throws Exception {
        assertTrue(master.isStarted());

        final File lockFile = new File(kahaDataDir, "lock");
        assertTrue("lock file exists via modification time", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Lock file " + lockFile.getAbsolutePath() + ", last mod at: " + new Date(lockFile.lastModified()));
                return lockFile.lastModified() > 0;
            }
        }));

        // ensure modification will be seen, second granularity on some nix
        TimeUnit.SECONDS.sleep(2);
        RandomAccessFile file = new RandomAccessFile(lockFile, "rw");
        file.write(4);
        file.getChannel().force(true);
        file.close();

        assertTrue("Master stops on lock file modification", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return master.isStopped();
            }
        }, 10000));

    }
}
