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

    @Test
    public void testCycle() throws Exception {
        Random r = new Random(0xb4a14);
        for (int i = 0; i < 30000; i++) {
            checkPercentage(i, i, r.nextInt(100) + 10, i % 2 == 0, i % 5 == 0);
        }
    }

    private void checkPercentage(int attempt, int seed, int operations, boolean useArrayBlocking, boolean useWaitForSpaceThread) throws InterruptedException {

        final BlockingQueue<Integer> toAdd;
        final BlockingQueue<Integer> toRemove;
        final BlockingQueue<Integer> removed;

        if (useArrayBlocking) {
            toAdd = new ArrayBlockingQueue<Integer>(operations);
            toRemove = new ArrayBlockingQueue<Integer>(operations);
            removed = new ArrayBlockingQueue<Integer>(operations);
        } else {
            toAdd = new LinkedBlockingQueue<Integer>();
            toRemove = new LinkedBlockingQueue<Integer>();
            removed = new LinkedBlockingQueue<Integer>();
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch startLatch = new CountDownLatch(1);

        final MemoryUsage memUsage = new MemoryUsage();
        memUsage.setLimit(1000);
        memUsage.start();

        Thread addThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startLatch.await();

                    while (true) {
                        Integer add = toAdd.poll(1, TimeUnit.MILLISECONDS);
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
            }
        });

        Thread removeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startLatch.await();

                    while (true) {
                        Integer remove = toRemove.poll(1, TimeUnit.MILLISECONDS);
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
            }
        });

        Thread waitForSpaceThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startLatch.await();

                    while (running.get()) {
                        memUsage.waitForSpace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        removeThread.start();
        addThread.start();
        if (useWaitForSpaceThread) {
            waitForSpaceThread.start();
        }

        Random r = new Random(seed);

        startLatch.countDown();

        for (int i = 0; i < operations; i++) {
            toAdd.add(r.nextInt(100) + 1);
        }

        // we expect the failure percentage to be related to the last operation
        List<Integer> ops = new ArrayList<Integer>(operations);
        for (int i = 0; i < operations; i++) {
            Integer op = removed.poll(1000, TimeUnit.MILLISECONDS);
            assertNotNull(op);
            ops.add(op);
        }

        running.set(false);

        if (useWaitForSpaceThread) {
            try {
                waitForSpaceThread.join(1000);
            } catch (InterruptedException e) {
                LOG.debug("Attempt: {} : {} waitForSpace never returned", attempt, memUsage);
                waitForSpaceThread.interrupt();
                waitForSpaceThread.join();
            }
        }

        removeThread.join();
        addThread.join();

        if (memUsage.getPercentUsage() != 0 || memUsage.getUsage() != memUsage.getPercentUsage()) {
            LOG.debug("Attempt: {} : {}", attempt, memUsage);
            LOG.debug("Operations: {}", ops);
            assertEquals(0, memUsage.getPercentUsage());
        }
    }
}
