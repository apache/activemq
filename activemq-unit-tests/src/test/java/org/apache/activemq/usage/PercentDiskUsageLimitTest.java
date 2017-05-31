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
package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


import java.io.File;

import org.apache.activemq.ConfigurationException;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.StoreUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This test is for AMQ-5393  and will check that schedulePeriodForDiskLimitCheck
 * properly schedules a task that will update disk limits if the amount of usable disk space drops
 * because another process uses up disk space.
 *
 */
public class PercentDiskUsageLimitTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(PercentDiskUsageLimitTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private BrokerService broker;
    private PersistenceAdapter adapter;
    private TempUsage tempUsage;
    private StoreUsage storeUsage;
    private File storeDir;

    @Before
    public void setUpBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        broker.setDeleteAllMessagesOnStartup(true);
        adapter = broker.getPersistenceAdapter();

        FileUtils.forceMkdir(adapter.getDirectory());
        FileUtils.forceMkdir(broker.getTempDataStore().getDirectory());
        storeDir = StoreUtil.findParentDirectory(adapter.getDirectory());

        final SystemUsage systemUsage = broker.getSystemUsage();
        tempUsage = systemUsage.getTempUsage();
        storeUsage = systemUsage.getStoreUsage();
    }

    protected void startBroker() throws Exception {
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }


    /**
     *
     */
    @Test(timeout=30000)
    public void testDiskLimit() throws Exception {
        int freePercent = getFreePercentage();

        if (freePercent >= 2) {
            int maxUsage = freePercent / 2;

            //Set max usage to less than free space so we know that all space can be allocated
            storeUsage.setPercentLimit(maxUsage);
            tempUsage.setPercentLimit(maxUsage);
            startBroker();

            long diskLimit = broker.getSystemUsage().getStoreUsage().getLimit();

            //assert the disk limit is the same as the max usage percent * total available space
            //within 1 mb
            assertEquals(diskLimit, storeDir.getTotalSpace() * maxUsage / 100, 1000000);
        }
    }

    @Test(timeout=30000)
    public void testDiskLimitOverMaxFree() throws Exception {
        int freePercent = getFreePercentage();

        if (freePercent > 1) {
            storeUsage.setPercentLimit(freePercent + 1);
            startBroker();

            long diskLimit = broker.getSystemUsage().getStoreUsage().getLimit();

            //assert the disk limit is the same as the usable space
            //within 1 mb
            assertEquals(diskLimit, storeDir.getUsableSpace(), 1000000);
        }
    }

    @Test(timeout=30000)
    public void testStartFailDiskLimitOverMaxFree() throws Exception {
        broker.setAdjustUsageLimits(false);
        int freePercent = getFreePercentage();

        if (freePercent > 1) {
            storeUsage.setPercentLimit(freePercent + 1);

            try {
                startBroker();
                fail("Expect ex");
            } catch (ConfigurationException expected) {}
        }
    }

    @Test(timeout=30000)
    public void testDiskLimitOver100Percent() throws Exception {
        int freePercent = getFreePercentage();

        if (freePercent > 1) {
            storeUsage.setPercentLimit(110);
            startBroker();

            long diskLimit = broker.getSystemUsage().getStoreUsage().getLimit();

            //assert the disk limit is the same as the available space
            //within 1 mb
            assertEquals(diskLimit, storeDir.getUsableSpace(), 1000000);
        }
    }

    protected int getFreePercentage() {
        File storeDir = StoreUtil.findParentDirectory(adapter.getDirectory());
        return (int) (((double)storeDir.getUsableSpace() / storeDir.getTotalSpace()) * 100);
    }

}
