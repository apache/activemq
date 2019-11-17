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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MemoryUsageTest {

    MemoryUsage underTest;
    ThreadPoolExecutor executor;
      
    @Test
    public final void testPercentUsageNeedsNoThread() {    
        int activeThreadCount = Thread.activeCount();
        underTest.setLimit(10);
        underTest.start();
        underTest.increaseUsage(1);
        assertEquals("usage is correct", 10, underTest.getPercentUsage());
        assertEquals("no new thread created without listener or callback",activeThreadCount, Thread.activeCount());
    }
    
    @Test
    public final void testAddUsageListenerStartsThread() throws Exception {       
        int activeThreadCount = Thread.activeCount();
        underTest = new MemoryUsage();
        underTest.setExecutor(executor);
        underTest.setLimit(10);
        underTest.start();
        final CountDownLatch called = new CountDownLatch(1);
        final String[] listnerThreadNameHolder = new String[1];
        underTest.addUsageListener(new UsageListener() {
            public void onUsageChanged(Usage usage, int oldPercentUsage,
                    int newPercentUsage) {
                called.countDown();
                listnerThreadNameHolder[0] = Thread.currentThread().toString();
            }
        });
        underTest.increaseUsage(1);
        assertTrue("listener was called", called.await(30, TimeUnit.SECONDS));
        assertTrue("listener called from another thread", !Thread.currentThread().toString().equals(listnerThreadNameHolder[0]));
        assertEquals("usage is correct", 10, underTest.getPercentUsage());
        assertEquals("new thread created with listener", activeThreadCount + 1, Thread.activeCount());        
    }

    @Test
    public void testPercentOfJvmHeap() throws Exception {
        underTest.setPercentOfJvmHeap(50);
        assertEquals("limit is half jvm limit", Math.round(Runtime.getRuntime().maxMemory() / 2.0), underTest.getLimit());
    }

    @Test
    public void testParentPortion() throws Exception {
        underTest.setLimit(1491035750);
        MemoryUsage child = new MemoryUsage(underTest, "child", 1f);
        assertEquals("limits are matched whole", underTest.getLimit(), child.getLimit());

        child.setUsagePortion(1f);
        assertEquals("limits are still matched whole", underTest.getLimit(), child.getLimit());
    }

    @Test(timeout=2000)
    public void testLimitedWaitFail() throws Exception {
        underTest.setLimit(10);
        underTest.start();
        underTest.increaseUsage(11);

        assertFalse("did not get usage within limit", underTest.waitForSpace(500));
    }

    @Before
    public void setUp() throws Exception {
        underTest = new MemoryUsage();
        this.executor = new ThreadPoolExecutor(1, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "Usage Async Task");
                thread.setDaemon(true);
                return thread;
            }
        });
        underTest.setExecutor(this.executor);

    }

    @After
    public void tearDown() {
        assertNotNull(underTest);
        underTest.stop();
        if (this.executor != null) {
            this.executor.shutdownNow();
        }
    }
}
