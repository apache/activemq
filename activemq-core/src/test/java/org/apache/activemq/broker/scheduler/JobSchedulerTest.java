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
package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.util.IOHelper;
import org.apache.kahadb.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JobSchedulerTest {

    private JobSchedulerStore store;
    private JobScheduler scheduler;

    @Test
    public void testAddLongStringByteSequence() throws Exception {
        final int COUNT = 10;
        final CountDownLatch latch = new CountDownLatch(COUNT);
        scheduler.addListener(new JobListener() {

            public void scheduledJob(String id, ByteSequence job) {
                latch.countDown();
            }

        });
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), 10);
        }
        latch.await(1, TimeUnit.SECONDS);
        assertTrue(latch.getCount() == 0);
    }

    @Test
    public void testAddLongLongIntStringByteSequence() throws Exception {
        final int COUNT = 10;
        final CountDownLatch latch = new CountDownLatch(COUNT);
        scheduler.addListener(new JobListener() {

            public void scheduledJob(String id, ByteSequence job) {
                latch.countDown();
            }

        });
        long time = System.currentTimeMillis() + 2000;
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), time, 10, -1);
        }
        assertTrue(latch.getCount() == COUNT);
        latch.await(3000, TimeUnit.SECONDS);
        assertTrue(latch.getCount() == 0);
    }

    @Test
    public void testAddStopThenDeliver() throws Exception {
        final int COUNT = 10;
        final CountDownLatch latch = new CountDownLatch(COUNT);
        long time = System.currentTimeMillis() + 2000;
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), time, 10, -1);
        }
        File directory = store.getDirectory();
        tearDown();
        startStore(directory);
        scheduler.addListener(new JobListener() {

            public void scheduledJob(String id, ByteSequence job) {
                latch.countDown();
            }

        });
        assertTrue(latch.getCount() == COUNT);
        latch.await(3000, TimeUnit.SECONDS);
        assertTrue(latch.getCount() == 0);
    }

    @Test
    public void testRemoveLong() throws Exception {
        final int COUNT = 10;

        long time = System.currentTimeMillis() + 20000;
        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(str.getBytes()), time, 10, -1);

        }
        int size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT);
        long removeTime = scheduler.getNextScheduleTime();
        scheduler.remove(removeTime);
        size = scheduler.getNextScheduleJobs().size();
        assertEquals(0, size);
    }

    @Test
    public void testRemoveString() throws IOException {
        final int COUNT = 10;
        final String test = "TESTREMOVE";
        long time = System.currentTimeMillis() + 20000;
        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(str.getBytes()), time, 10, -1);
            if (i == COUNT / 2) {
                scheduler.schedule(test, new ByteSequence(test.getBytes()), time, 10, -1);
            }
        }

        int size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT + 1);
        scheduler.remove(test);
        size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT);
    }

    @Before
    public void setUp() throws Exception {
        File directory = new File("target/test/ScheduledDB");
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);
        startStore(directory);

    }

    protected void startStore(File directory) throws Exception {
        store = new JobSchedulerStore();
        store.setDirectory(directory);
        store.start();
        scheduler = store.getJobScheduler("test");
    }

    @After
    public void tearDown() throws Exception {
        store.stop();
    }

}
