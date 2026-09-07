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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryUsageConcurrencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageConcurrencyTest.class);

    /**
     * Liveness soak for the untimed waitForSpace(): unlike the timed variants (which poll),
     * it blocks on waitForSpaceCondition and depends entirely on the 100% -> below-100%
     * transition reaching the locked setPercentUsage() path that signals the condition.
     * With lock-free accounting this rests on the invariant that every usage mutation is
     * unconditionally followed by a percent re-check, so the temporally last mutation of a
     * lasting drop always signals. Each round parks waiters at exactly 100%, races balanced
     * churn pairs (usage never drops below full) against the bookkeeping, then issues a
     * lasting one-byte drop WHILE churn is still running. After churn quiesces the usage is
     * lastingly below the limit, so every waiter must be released.
     */
    @Test
    public void testUntimedWaitForSpaceLivenessSoak() throws Exception {
        final int rounds = 150;
        final int waiters = 4;
        final int churners = 6;

        for (int round = 0; round < rounds; round++) {
            final MemoryUsage u = new MemoryUsage();
            u.setLimit(100);
            u.start();
            final AtomicBoolean churnRunning = new AtomicBoolean(true);
            final CountDownLatch released = new CountDownLatch(waiters);
            final List<Thread> waiterThreads = new ArrayList<>();
            final List<Thread> churnThreads = new ArrayList<>();
            try {
                u.increaseUsage(100);   // exactly full

                for (int w = 0; w < waiters; w++) {
                    final Thread t = new Thread(() -> {
                        try {
                            u.waitForSpace();   // untimed: no polling fallback
                            released.countDown();
                        } catch (InterruptedException ignored) {
                        }
                    });
                    t.setDaemon(true);
                    waiterThreads.add(t);
                    t.start();
                }

                // wait until every waiter is parked on the condition
                final long deadline = System.currentTimeMillis() + 5000;
                for (Thread t : waiterThreads) {
                    while (t.getState() != Thread.State.WAITING && System.currentTimeMillis() < deadline) {
                        Thread.yield();
                    }
                    assertEquals("round " + round + " waiter failed to park", Thread.State.WAITING, t.getState());
                }

                // balanced churn: increase-then-decrease pairs keep usage >= 100 while
                // hammering the lock-free percent bookkeeping
                for (int c = 0; c < churners; c++) {
                    final Thread t = new Thread(() -> {
                        while (churnRunning.get()) {
                            u.increaseUsage(3);
                            u.decreaseUsage(3);
                        }
                    });
                    t.setDaemon(true);
                    churnThreads.add(t);
                    t.start();
                }
                Thread.sleep(2);

                // the lasting drop below 100%, deliberately concurrent with churn
                u.decreaseUsage(1);

                Thread.sleep(1);
                churnRunning.set(false);
                for (Thread t : churnThreads) {
                    t.join(5000);
                }

                // usage is now lastingly 99/100: every untimed waiter must have been signalled
                assertTrue("round " + round + " untimed waitForSpace waiters not released: " + u,
                        released.await(10, TimeUnit.SECONDS));
            } finally {
                churnRunning.set(false);
                u.stop();
            }
        }
    }

    /**
     * The cached percent-bucket bounds must keep getPercentUsage() tracking the exact
     * calculated percent across bucket boundaries, over the limit, back down, and after a
     * runtime limit change (which refreshes bounds via onLimitChange -> setPercentUsage).
     */
    @Test
    public void testPercentBoundsTracking() {
        final MemoryUsage u = new MemoryUsage();
        u.setLimit(1000);
        u.start();
        try {
            assertEquals(0, u.getPercentUsage());
            u.increaseUsage(5);                    // 5/1000 -> 0%
            assertEquals(0, u.getPercentUsage());
            u.increaseUsage(5);                    // 10/1000 -> 1%
            assertEquals(1, u.getPercentUsage());
            u.increaseUsage(490);                  // 500/1000 -> 50%
            assertEquals(50, u.getPercentUsage());
            u.increaseUsage(499);                  // 999/1000 -> 99%
            assertEquals(99, u.getPercentUsage());
            u.increaseUsage(1);                    // 1000/1000 -> 100%
            assertEquals(100, u.getPercentUsage());
            u.increaseUsage(50);                   // 1050/1000 -> 105% (over limit)
            assertEquals(105, u.getPercentUsage());
            u.decreaseUsage(51);                   // 999/1000 -> 99%
            assertEquals(99, u.getPercentUsage());

            u.setLimit(2000);                      // 999/2000 -> 49%; bounds must refresh
            assertEquals(49, u.getPercentUsage());
            u.increaseUsage(1);                    // 1000/2000 -> 50%
            assertEquals(50, u.getPercentUsage());

            u.decreaseUsage(1000);                 // 0/2000 -> 0%
            assertEquals(0, u.getPercentUsage());
            assertEquals(0, u.getUsage());
        } finally {
            u.stop();
        }
    }

    @Test
    public void testSetUsageSequential() {
        final MemoryUsage u = new MemoryUsage();
        u.setLimit(1000);
        u.start();
        try {
            u.increaseUsage(100);
            assertEquals(100, u.getUsage());
            u.setUsage(500);
            assertEquals(500, u.getUsage());
            assertEquals(50, u.getPercentUsage());
            u.setUsage(0);
            assertEquals(0, u.getUsage());
            assertEquals(0, u.getPercentUsage());
        } finally {
            u.stop();
        }
    }

    /**
     * setUsage() racing balanced increase/decrease pairs must leave the final usage within
     * one in-flight operation per thread of the set value. Each worker performs complete
     * increase(v);decrease(v) pairs, so after joining, the only legal deviations from the
     * set target come from pairs that straddle the set's linearization point (or its
     * non-atomic LongAdder.sum() sweep): at most one op of at most maxOp per thread, in
     * either direction. A setUsage() built on LongAdder.reset() can additionally lose
     * concurrent updates outright (reset() is documented as safe only with no concurrent
     * updates), allowing drift beyond this bound.
     */
    @Test
    public void testConcurrentSetUsageDriftBounded() throws Exception {
        final int threads = 16;
        final int maxOp = 100;
        final int rounds = 200;
        final long target = 123456;

        for (int round = 0; round < rounds; round++) {
            final MemoryUsage u = new MemoryUsage();
            u.setLimit(1L << 40);
            u.start();
            final AtomicBoolean running = new AtomicBoolean(true);
            final CountDownLatch startLatch = new CountDownLatch(1);
            final List<Thread> workers = new ArrayList<>();
            try {
                for (int t = 0; t < threads; t++) {
                    final int seed = round * 31 + t;
                    final Thread w = new Thread(() -> {
                        final Random r = new Random(seed);
                        try {
                            startLatch.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        while (running.get()) {
                            final int v = r.nextInt(maxOp) + 1;
                            u.increaseUsage(v);
                            u.decreaseUsage(v);
                        }
                    });
                    w.setDaemon(true);
                    workers.add(w);
                    w.start();
                }

                startLatch.countDown();
                Thread.sleep(2);
                u.setUsage(target);
                Thread.sleep(2);
                running.set(false);
                for (Thread w : workers) {
                    w.join(5000);
                }

                final long drift = u.getUsage() - target;
                final long bound = (long) threads * maxOp;
                if (Math.abs(drift) > bound) {
                    LOG.info("Round {} drift {} exceeds bound {} : {}", round, drift, bound, u);
                }
                assertEquals("round " + round + " drift " + drift + " exceeds per-thread in-flight bound " + bound,
                        0, Math.abs(drift) > bound ? drift : 0);
            } finally {
                running.set(false);
                u.stop();
            }
        }
    }

    @Test
    public void testCycle() throws Exception {
        final Random r = new Random(0xb4a14);
        for (int i = 0; i < 3000; i++) {
            checkPercentage(i, i, r.nextInt(100) + 10, i % 2 == 0, i % 5 == 0);
        }
    }

    private void checkPercentage(final int attempt, final int seed, final int operations,
                                 final boolean useArrayBlocking, final boolean useWaitForSpaceThread) throws InterruptedException {

        final BlockingQueue<Integer> toAdd;
        final BlockingQueue<Integer> toRemove;
        final BlockingQueue<Integer> removed;

        if (useArrayBlocking) {
            toAdd = new ArrayBlockingQueue<>(operations);
            toRemove = new ArrayBlockingQueue<>(operations);
            removed = new ArrayBlockingQueue<>(operations);
        } else {
            toAdd = new LinkedBlockingQueue<>();
            toRemove = new LinkedBlockingQueue<>();
            removed = new LinkedBlockingQueue<>();
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch startLatch = new CountDownLatch(1);

        final MemoryUsage memUsage = new MemoryUsage();
        memUsage.setLimit(1000);
        memUsage.start();

        try {
            final Thread addThread = new Thread(() -> {
                try {
                    startLatch.await();

                    while (true) {
                        final Integer add = toAdd.poll(1, TimeUnit.MILLISECONDS);
                        if (add == null) {
                            if (!running.get()) {
                                break;
                            }
                        } else {
                            // add to other queue before removing
                            toRemove.add(add);
                            memUsage.increaseUsage(add);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            final Thread removeThread = new Thread(() -> {
                try {
                    startLatch.await();

                    while (true) {
                        final Integer remove = toRemove.poll(1, TimeUnit.MILLISECONDS);
                        if (remove == null) {
                            if (!running.get()) {
                                break;
                            }
                        } else {
                            memUsage.decreaseUsage(remove);
                            removed.add(remove);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // Use waitForSpace(timeout) instead of unbounded waitForSpace() to avoid
            // indefinite blocking when usage is >= 100%. The bounded version will return
            // after the timeout, allowing the thread to check the running flag and exit.
            final Thread waitForSpaceThread = new Thread(() -> {
                try {
                    startLatch.await();

                    while (running.get()) {
                        memUsage.waitForSpace(100);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // Mark all threads as daemon so they cannot prevent JVM shutdown
            // even if cleanup logic fails to stop them
            addThread.setDaemon(true);
            removeThread.setDaemon(true);
            waitForSpaceThread.setDaemon(true);

            removeThread.start();
            addThread.start();
            if (useWaitForSpaceThread) {
                waitForSpaceThread.start();
            }

            final Random r = new Random(seed);

            startLatch.countDown();

            for (int i = 0; i < operations; i++) {
                toAdd.add(r.nextInt(100) + 1);
            }

            // we expect the failure percentage to be related to the last operation
            final List<Integer> ops = new ArrayList<>(operations);
            for (int i = 0; i < operations; i++) {
                final Integer op = removed.poll(1000, TimeUnit.MILLISECONDS);
                assertNotNull(op);
                ops.add(op);
            }

            running.set(false);

            addThread.join(5000);
            removeThread.join(5000);

            if (useWaitForSpaceThread) {
                waitForSpaceThread.join(5000);
                if (waitForSpaceThread.isAlive()) {
                    LOG.debug("Attempt: {} : {} waitForSpace thread still alive after join, interrupting", attempt, memUsage);
                    waitForSpaceThread.interrupt();
                    waitForSpaceThread.join(1000);
                }
            }

            if (memUsage.getPercentUsage() != 0 || memUsage.getUsage() != memUsage.getPercentUsage()) {
                LOG.debug("Attempt: {} : {}", attempt, memUsage);
                LOG.debug("Operations: {}", ops);
                assertEquals(0, memUsage.getPercentUsage());
            }
        } finally {
            // Stop the MemoryUsage to signal waitForSpaceCondition, which unblocks
            // any thread stuck in waitForSpace(). This is critical for cleanup.
            memUsage.stop();
        }
    }
}
