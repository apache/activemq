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
package org.apache.activemq.thread;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchedulerTest {

    private final static String schedulerName = "testScheduler";
    private Scheduler scheduler;

    @Before
    public void before() throws Exception {
        scheduler = new Scheduler(schedulerName);
        scheduler.start();
    }

    @After
    public void after() throws Exception {
        scheduler.stop();
    }

    @Test
    public void testExecutePeriodically() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        scheduler.executePeriodically(new CountDownRunnable(latch), 10);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void executeAfterDelay() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        scheduler.executeAfterDelay(new CountDownRunnable(latch), 10);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testExecutePeriodicallyReplace() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownRunnable task = new CountDownRunnable(latch);

        scheduler.executePeriodically(task, 500);
        scheduler.executePeriodically(task, 500);
        scheduler.cancel(task);

        //make sure the task never runs
        assertFalse(latch.await(1000, TimeUnit.MILLISECONDS));
    }

    private static class CountDownRunnable implements Runnable {
        final CountDownLatch latch;

        CountDownRunnable(final CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            latch.countDown();
        }
    }

}
