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
import java.net.URI;
import java.util.Random;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.StoreUtil;
import org.apache.activemq.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
public class PeriodicDiskUsageLimitTest {
    protected static final Logger LOG = LoggerFactory
            .getLogger(PeriodicDiskUsageLimitTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));
    File testfile;
    private BrokerService broker;
    private PersistenceAdapter adapter;
    private TempUsage tempUsage;
    private StoreUsage storeUsage;
    protected URI brokerConnectURI;

    @Before
    public void setUpBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(true);
        testfile = dataFileDir.newFile();
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        broker.setDeleteAllMessagesOnStartup(true);
        adapter = broker.getPersistenceAdapter();

        TransportConnector connector = broker.addConnector("tcp://0.0.0.0:0");
        connector.setName("tcp");

        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();

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
    }

    /**
     * This test will show that if a file is written to take away free space, and
     * if the usage limit is now less than the store size plus remaining free space, then
     * the usage limits will adjust lower.
     */
    @Test(timeout=90000)
    public void testDiskUsageAdjustLower() throws Exception {
        //set the limit to max space so that if a file is added to eat up free space then
        //the broker should adjust the usage limit..set time to 2 seconds for testing
        setLimitMaxSpace();
        broker.setSchedulePeriodForDiskUsageCheck(2000);
        startBroker();

        assertRampDown();
    }

    /**
     * This test will show that if a file is written to take away free space, and
     * if the usage limit is now less than the store size plus remaining free space, then
     * the usage limits will adjust lower.  Then test that size regrows when file is deleted.
     */
    @Test(timeout=90000)
    public void testDiskUsageAdjustLowerAndHigherUsingPercent() throws Exception {
        //set the limit to max space so that if a file is added to eat up free space then
        //the broker should adjust the usage limit..add 5% above free space
        tempUsage.setPercentLimit(getFreePercentage(broker.getTempDataStore().getDirectory()) + 5);
        storeUsage.setPercentLimit(getFreePercentage(adapter.getDirectory()) + 5);

        //set threshold to 1 megabyte
        broker.setDiskUsageCheckRegrowThreshold(1024 * 1024);
        broker.setSchedulePeriodForDiskUsageCheck(2000);
        startBroker();

        assertRampDown();

        //get the limits and then delete the test file to free up space
        final long storeLimit = broker.getSystemUsage().getStoreUsage().getLimit();
        final long tmpLimit = broker.getSystemUsage().getTempUsage().getLimit();
        FileUtils.deleteQuietly(testfile);

        //regrow
        assertTrue("Store Usage should ramp up.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getSystemUsage().getStoreUsage().getLimit() >  storeLimit;
            }
        }, 15000));

        //regrow
        assertTrue("Temp Usage should ramp up.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getSystemUsage().getTempUsage().getLimit() >  tmpLimit;
            }
        }, 15000));
    }

    /**
     * This test shows that the usage limits will not change if the
     * schedulePeriodForDiskLimitCheck property is not set because no task will run
     */
    @Test(timeout=60000)
    public void testDiskLimitCheckNotSet() throws Exception {
        setLimitMaxSpace();
        startBroker();

        long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 5 meg file to the file system
        writeTestFile(5 * 1024 * 1024);
        Thread.sleep(5000);

        //assert that the usage limits have not changed because a task should not have run
        assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
        assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
    }

    /**
     * This test shows that the usage limits will not change if the
     * schedulePeriodForDiskLimitCheck property is not set because no task will run
     */
    @Test(timeout=60000)
    public void testDiskLimitCheckNotSetUsingPercent() throws Exception {
        tempUsage.setPercentLimit(getFreePercentage(broker.getTempDataStore().getDirectory()) + 5);
        storeUsage.setPercentLimit(getFreePercentage(adapter.getDirectory()) + 5);
        startBroker();

        long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 5 meg file to the file system
        writeTestFile(5 * 1024 * 1024);
        Thread.sleep(5000);

        //assert that the usage limits have not changed because a task should not have run
        assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
        assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
    }

    /**
     * This test will show that if a file is written to take away free space, but
     * if the limit is greater than the store size and the remaining free space, then
     * the usage limits will not adjust.
     */
    @Test(timeout=60000)
    public void testDiskUsageStaySame() throws Exception {
        //set a limit lower than max available space and set the period to 2 seconds
        tempUsage.setLimit(10000000);
        storeUsage.setLimit(100000000);
        broker.setSchedulePeriodForDiskUsageCheck(2000);
        startBroker();

        long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
        long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

        //write a 2 meg file to the file system
        writeTestFile(2 * 1024 * 1024);
        Thread.sleep(5000);

        //Assert that the usage limits have not changed because writing a 2 meg file
        //did not decrease the the free space below the already set limit
        assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
        assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
    }

    /**
     * This test will show that if a file is written to take away free space, but
     * if the limit is greater than the store size and the remaining free space, then
     * the usage limits will not adjust.
     */
    @Test(timeout=60000)
    public void testDiskUsageStaySameUsingPercent() throws Exception {
        //set a limit lower than max available space and set the period to 5 seconds
        //only run if at least 4 percent disk space free
        int tempFreePercent = getFreePercentage(broker.getTempDataStore().getDirectory());
        int freePercent = getFreePercentage(adapter.getDirectory());
        if (freePercent >= 4 && tempFreePercent >= 4) {
            tempUsage.setPercentLimit(freePercent / 2);
            storeUsage.setPercentLimit(tempFreePercent / 2);

            broker.setSchedulePeriodForDiskUsageCheck(2000);
            startBroker();

            long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
            long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

            //write a 5 meg file to the file system
            writeTestFile(5 * 1024 * 1024);
            Thread.sleep(5000);

            //Assert that the usage limits have not changed because writing a 2 meg file
            //did not decrease the the free space below the already set limit
            assertEquals(originalDisk, broker.getSystemUsage().getStoreUsage().getLimit());
            assertEquals(originalTmp, broker.getSystemUsage().getTempUsage().getLimit());
        }
        LOG.info("Not running b/c there is less that 4% disk space, freePrecent:" + freePercent);
    }

    protected void assertRampDown() throws Exception {
        //Try a couple of times because other processes could write/delete from disk
        assertTrue("Store Usage should ramp down", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {

                FileUtils.deleteQuietly(testfile);
                final long originalDisk = broker.getSystemUsage().getStoreUsage().getLimit();
                final long originalTmp = broker.getSystemUsage().getTempUsage().getLimit();

                //write a 10 meg file to the file system
                writeTestFile(10 * 1024 * 1024);

                //Assert that the usage limits have been decreased because some free space was used
                //up by a file
                boolean storeUsageRampDown = Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return broker.getSystemUsage().getStoreUsage().getLimit() <  originalDisk;
                    }
                }, 12000);

                boolean tempUsageRampDown = false;
                if (storeUsageRampDown) {
                    tempUsageRampDown = Wait.waitFor(new Wait.Condition() {
                        @Override
                        public boolean isSatisified() throws Exception {
                            return broker.getSystemUsage().getTempUsage().getLimit() <  originalTmp;
                        }
                    }, 12000);
                }

                return storeUsageRampDown && tempUsageRampDown;
            }
        }, 60000));
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
        try(FileOutputStream stream = new FileOutputStream(testfile)) {
            IOUtils.write(data, stream);
        }
    }

    protected int getFreePercentage(File directory) {
        File storeDir = StoreUtil.findParentDirectory(directory);
        return (int) (((double)storeDir.getUsableSpace() / storeDir.getTotalSpace()) * 100);
    }

}
