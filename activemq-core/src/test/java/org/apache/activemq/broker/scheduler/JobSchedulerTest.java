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
import java.util.List;
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
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), 1000);
        }
        latch.await(5, TimeUnit.SECONDS);
        assertEquals(0,latch.getCount());
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
        long time = 2000;
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), "", time, 10, -1);
        }
        assertTrue(latch.getCount() == COUNT);
        latch.await(3000, TimeUnit.SECONDS);
        assertTrue(latch.getCount() == 0);
    }

    @Test
    public void testAddStopThenDeliver() throws Exception {
        final int COUNT = 10;
        final CountDownLatch latch = new CountDownLatch(COUNT);
        long time =  2000;
        for (int i = 0; i < COUNT; i++) {
            String test = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(test.getBytes()), "", time, 1000, -1);
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

        long time = 20000;
        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(str.getBytes()), "", time, 1000, -1);

        }
        int size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT);
        long removeTime = scheduler.getNextScheduleTime();
        scheduler.remove(removeTime);
        size = scheduler.getNextScheduleJobs().size();
        assertEquals(0, size);
    }

    @Test
    public void testRemoveString() throws Exception {
        final int COUNT = 10;
        final String test = "TESTREMOVE";
        long time = 20000;
        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
            scheduler.schedule("id" + i, new ByteSequence(str.getBytes()), "", time, 10, -1);
            if (i == COUNT / 2) {
                scheduler.schedule(test, new ByteSequence(test.getBytes()), "", time, 10, -1);
            }
        }

        int size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT + 1);
        scheduler.remove(test);
        size = scheduler.getNextScheduleJobs().size();
        assertEquals(size, COUNT);
    }

    @Test
    public void testgetAllJobs() throws Exception {
        final int COUNT = 10;
        final String ID = "id:";
        long time =  20000;
        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
            scheduler.schedule(ID + i, new ByteSequence(str.getBytes()), "", time, 10 + i, -1);
        }
        List<Job> list = scheduler.getAllJobs();

        assertEquals(list.size(), COUNT);
        int count = 0;
        for (Job job : list) {

            assertEquals(job.getJobId(), ID + count);
            count++;
        }
    }

    @Test
    public void testgetAllJobsInRange() throws Exception {
        final int COUNT = 10;
        final String ID = "id:";
        long start = 10000;

        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
           
                scheduler.schedule(ID + i, new ByteSequence(str.getBytes()), "", start + (i * 1000), 10000 + i, 0);
           
        }
        start = System.currentTimeMillis();
        long finish = start + 12000+ (COUNT * 1000);
        List<Job> list = scheduler.getAllJobs(start, finish);

        assertEquals( COUNT,list.size());
        int count = 0;
        for (Job job : list) {

            assertEquals(job.getJobId(), ID + count);
            count++;
        }
    }

    @Test
    public void testRemoveAllJobsInRange() throws Exception {
        final int COUNT = 10;
        final String ID = "id:";
        long start = 10000;

        for (int i = 0; i < COUNT; i++) {
            String str = new String("test" + i);
           
                scheduler.schedule(ID + i, new ByteSequence(str.getBytes()), "", start + (i * 1000), 10000 + i, 0);
           
        }
        start = System.currentTimeMillis();
        long finish = start + 12000+ (COUNT * 1000);
        scheduler.removeAllJobs(start, finish);

        assertTrue(scheduler.getAllJobs().isEmpty());
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
