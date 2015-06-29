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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This test is for AMQ-5393  and will check that schedulePeriodForDiskLimitCheck
 * properly schedules a task that will update disk limits if the amount of usable disk space drops
 * because another process uses up disk space.
 *
 */
public class PeriodicDiskUsageLimitTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(PeriodicDiskUsageLimitTest.class);

    File dataFileDir = new File("target/test-amq-5393/datadb");
    File testfile = new File("target/test-amq-5393/testfile");
    private BrokerService broker;
    private PersistenceAdapter adapter;
    private TempUsage tempUsage;
    private StoreUsage storeUsage;

    @Before
    public void setUpBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir);
        broker.setDeleteAllMessagesOnStartup(true);
        adapter = broker.getPersistenceAdapter();

        FileUtils.deleteQuietly(testfile);
        FileUtils.forceMkdir(adapter.getDirectory());
        FileUtils.forceMkdir(broker.getTempDataStore().getDirectory());

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
        FileUtils.deleteQuietly(testfile);
        FileUtils.deleteQuietly(dataFileDir);
    }

    /**
     * This test will show that if a file is written to take away free space, and
     * if the usage limit is now less than the store size plus remaining free space, then
     * the usage limits will adjust lower.
     */
    @Test(timeout=30000)
    public void testDiskUsageAdjustLower() throws Exception {
        //set the limit to max space so that if a file is added to eat up free space then
        //the broker should adjust the usage limit..set time to 5 seconds for testing
        setLimitMaxSpace();
        broker.setSchedulePeriodForDiskUsageCheck(4000);
        startBroker();

        final long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        final long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 1 meg file to the file system
        writeTestFile(1024 * 1024);

        //Assert that the usage limits have been decreased because some free space was used
        //up by a file
        assertTrue("Store Usage should ramp down.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getSystemUsage().getStoreUsage().getLimit() <  originalDisk;
            }
        }));

        assertTrue("Temp Usage should ramp down.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getSystemUsage().getTempUsage().getLimit() <  originalTmp;
            }
        }));
    }

    /**
     * This test shows that the usage limits will not change if the
     * schedulePeriodForDiskLimitCheck property is not set because no task will run
     */
    @Test(timeout=30000)
    public void testDiskLimitCheckNotSet() throws Exception {
        setLimitMaxSpace();
        startBroker();

        long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 1 meg file to the file system
        writeTestFile(1024 * 1024);
        Thread.sleep(3000);

        //assert that the usage limits have not changed because a task should not have run
        assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
        assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
    }

    /**
     * This test will show that if a file is written to take away free space, but
     * if the limit is greater than the store size and the remaining free space, then
     * the usage limits will not adjust.
     */
    @Test(timeout=30000)
    public void testDiskUsageStaySame() throws Exception {
        //set a limit lower than max available space and set the period to 5 seconds
        tempUsage.setLimit(10000000);
        storeUsage.setLimit(100000000);
        broker.setSchedulePeriodForDiskUsageCheck(2000);
        startBroker();

        long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 1 meg file to the file system
        writeTestFile(1024 * 1024);
        Thread.sleep(5000);

        //Assert that the usage limits have not changed because writing a 1 meg file
        //did not decrease the the free space below the already set limit
        assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
        assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
    }

    protected void setLimitMaxSpace() {
        //Configure store limits to be the max usable space on startup
        tempUsage.setLimit(broker.getTempDataStore().getDirectory().getUsableSpace());
        storeUsage.setLimit(adapter.getDirectory().getUsableSpace());
    }

    protected void writeTestFile(int size) throws IOException {
        final byte[] data = new byte[size];
        final Random rng = new Random();
        rng.nextBytes(data);
        IOUtils.write(data, new FileOutputStream(testfile));
    }

}
