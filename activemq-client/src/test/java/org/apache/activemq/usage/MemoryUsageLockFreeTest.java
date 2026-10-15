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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that MemoryUsage keeps a consistent percentage and still wakes waiters when usage is updated
 * concurrently without holding the usage lock.
 */
public class MemoryUsageLockFreeTest {

    private static final int THREADS = 16;

    private ExecutorService executor;
    private ThreadPoolExecutor usageExecutor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(THREADS + 2);
        usageExecutor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        usageExecutor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(usageExecutor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testConcurrentUpdatesAcrossFullBoundaryLeaveConsistentPercentage() throws Exception {
        final MemoryUsage parent = new MemoryUsage("parent");
        parent.setLimit(THREADS * 64);
        parent.start();
        final List<MemoryUsage> children = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            final MemoryUsage child = new MemoryUsage(parent, "child" + i);
            child.setLimit(THREADS * 32);
            child.start();
            children.add(child);
        }

        final CyclicBarrier barrier = new CyclicBarrier(THREADS);
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            final MemoryUsage usage = children.get(t % children.size());
            futures.add(executor.submit(() -> {
                barrier.await();
                final ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < 200_000; i++) {
                    final int amount = 1 + random.nextInt(128);
                    usage.increaseUsage(amount);
                    usage.isFull();
                    usage.getPercentUsage();
                    usage.decreaseUsage(amount);
                }
                return null;
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }

        for (MemoryUsage child : children) {
            assertEquals(0, child.getUsage());
            assertEquals(0, child.getPercentUsage());
            assertFalse(child.isFull());
        }
        assertEquals(0, parent.getUsage());
        assertEquals(0, parent.getPercentUsage());
        assertFalse(parent.isFull());
    }

    @Test(timeout = 60000)
    public void testPercentageMatchesUsageAfterEveryQuiescentPoint() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setLimit(1000);
        usage.start();

        for (int round = 0; round < 200; round++) {
            final int perThread = ThreadLocalRandom.current().nextInt(1, 200);
            final CyclicBarrier barrier = new CyclicBarrier(THREADS);
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(executor.submit(() -> {
                    barrier.await();
                    for (int i = 0; i < perThread; i++) {
                        usage.increaseUsage(1);
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            final long expected = (long) perThread * THREADS;
            assertEquals(expected, usage.getUsage());
            assertEquals(Math.min(expected * 100 / 1000, Integer.MAX_VALUE), usage.getPercentUsage());
            assertEquals(expected >= 1000, usage.isFull());
            usage.decreaseUsage(expected);
            assertEquals(0, usage.getPercentUsage());
        }
    }

    @Test(timeout = 30000)
    public void testWaitForSpaceWakesWhenUsageDrops() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setLimit(100);
        usage.start();
        usage.increaseUsage(150);
        assertTrue(usage.isFull());

        final CountDownLatch waiting = new CountDownLatch(2);
        final Future<?> unbounded = executor.submit(() -> {
            waiting.countDown();
            usage.waitForSpace();
            return null;
        });
        final Future<Boolean> bounded = executor.submit(() -> {
            waiting.countDown();
            return usage.waitForSpace(20_000);
        });
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertFalse(usage.waitForSpace(50));

        usage.decreaseUsage(60);

        unbounded.get(10, TimeUnit.SECONDS);
        assertTrue(bounded.get(10, TimeUnit.SECONDS));
        assertFalse(usage.isFull());
        assertTrue(usage.waitForSpace(1));
    }

    @Test(timeout = 30000)
    public void testWaitForSpaceStoppedThrows() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setLimit(100);
        usage.start();
        usage.increaseUsage(100);

        final CountDownLatch waiting = new CountDownLatch(1);
        final Future<?> waiter = executor.submit(() -> {
            waiting.countDown();
            usage.waitForSpace();
            return null;
        });
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        TimeUnit.MILLISECONDS.sleep(100);
        usage.stop();
        try {
            waiter.get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException expected) {
            assertTrue(expected.getCause() instanceof InterruptedException);
            return;
        }
        throw new AssertionError("waitForSpace should fail when usage is stopped while full");
    }

    @Test(timeout = 30000)
    public void testCallbackWhenNotFullFiresOnLockFreeDecrease() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setExecutor(usageExecutor);
        usage.setLimit(100);
        usage.start();
        usage.increaseUsage(100);

        final CountDownLatch called = new CountDownLatch(1);
        assertTrue(usage.notifyCallbackWhenNotFull(called::countDown));
        assertEquals(1, called.getCount());

        usage.decreaseUsage(1);
        assertTrue(called.await(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 30000)
    public void testListenerSeesFullAndNotFullTransitions() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setExecutor(usageExecutor);
        usage.setLimit(100);
        usage.start();

        final CountDownLatch full = new CountDownLatch(1);
        final CountDownLatch notFull = new CountDownLatch(1);
        usage.addUsageListener((u, oldPercent, newPercent) -> {
            if (newPercent >= 100) {
                full.countDown();
            } else if (oldPercent >= 100) {
                notFull.countDown();
            }
        });

        usage.increaseUsage(100);
        assertTrue(full.await(5, TimeUnit.SECONDS));
        usage.decreaseUsage(100);
        assertTrue(notFull.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testHighWaterMarkUsesParent() throws Exception {
        final MemoryUsage parent = new MemoryUsage("parent");
        parent.setLimit(1000);
        parent.start();
        final MemoryUsage child = new MemoryUsage(parent, "child");
        child.setLimit(100);
        child.start();

        child.increaseUsage(90);
        assertFalse(child.isFull());
        assertTrue(child.isFull(90));
        assertFalse(child.isFull(91));
        assertTrue(child.waitForSpace(1, 91));
        assertFalse(child.waitForSpace(1, 90));

        // The child's 90 already propagated to the parent, so this takes the parent to exactly 100%.
        parent.increaseUsage(910);
        assertTrue(child.isFull());
        assertTrue(child.isFull(100));
        assertFalse(child.waitForSpace(1, 100));
    }

    @Test
    public void testSetUsageUpdatesPercentage() {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setLimit(100);
        usage.start();

        usage.setUsage(150);
        assertEquals(150, usage.getPercentUsage());
        assertTrue(usage.isFull());

        usage.setUsage(10);
        assertEquals(10, usage.getPercentUsage());
        assertFalse(usage.isFull());
    }

    @Test(timeout = 60000)
    public void testLimitAndMinDeltaChangesRacingWithUpdates() throws Exception {
        final MemoryUsage usage = new MemoryUsage("usage");
        usage.setLimit(1000);
        usage.start();

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger limitChanges = new AtomicInteger();
        final Future<?> limiter = executor.submit(() -> {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            while (running.get()) {
                usage.setLimit(500 + random.nextInt(1000));
                usage.setPercentUsageMinDelta(1 + random.nextInt(5));
                limitChanges.incrementAndGet();
            }
            return null;
        });

        final CyclicBarrier barrier = new CyclicBarrier(THREADS);
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            futures.add(executor.submit(() -> {
                barrier.await();
                for (int i = 0; i < 50_000; i++) {
                    usage.increaseUsage(7);
                    usage.isFull(110);
                    usage.decreaseUsage(7);
                }
                usage.increaseUsage(10);
                return null;
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
        running.set(false);
        limiter.get();
        assertTrue(limitChanges.get() > 0);

        usage.setPercentUsageMinDelta(1);
        usage.setLimit(1000);
        assertEquals(THREADS * 10, usage.getUsage());
        assertEquals(THREADS * 10 * 100 / 1000, usage.getPercentUsage());

        // A further lock-free update must still publish against the final limit.
        usage.increaseUsage(1000 - THREADS * 10);
        assertEquals(100, usage.getPercentUsage());
        assertTrue(usage.isFull());
    }
}
