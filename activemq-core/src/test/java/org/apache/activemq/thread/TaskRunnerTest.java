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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.transport.TopicClusterTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class TaskRunnerTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(TaskRunnerTest.class);

    public void testWakeupPooled() throws InterruptedException, BrokenBarrierException {
        System.setProperty("org.apache.activemq.UseDedicatedTaskRunner", "false");
        doTestWakeup();
    }

    public void testWakeupDedicated() throws InterruptedException, BrokenBarrierException {
        System.setProperty("org.apache.activemq.UseDedicatedTaskRunner", "true");
        doTestWakeup();
    }

    /**
     * Simulate multiple threads queuing work for the TaskRunner. The Task
     * Runner dequeues the work.
     * 
     * @throws InterruptedException
     * @throws BrokenBarrierException
     */
    public void doTestWakeup() throws InterruptedException, BrokenBarrierException {

        final int enqueueCount = 100000;
        final AtomicInteger iterations = new AtomicInteger(0);
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger queue = new AtomicInteger(0);
        final CountDownLatch doneCountDownLatch = new CountDownLatch(1);

        TaskRunnerFactory factory = new TaskRunnerFactory();
        final TaskRunner runner = factory.createTaskRunner(new Task() {
            public boolean iterate() {
                if (queue.get() == 0) {
                    return false;
                } else {
                    while (queue.get() > 0) {
                        queue.decrementAndGet();
                        counter.incrementAndGet();
                    }
                    iterations.incrementAndGet();
                    if (counter.get() == enqueueCount) {
                        doneCountDownLatch.countDown();
                    }
                    return true;
                }
            }
        }, "Thread Name");

        long start = System.currentTimeMillis();
        final int workerCount = 5;
        final CyclicBarrier barrier = new CyclicBarrier(workerCount + 1);
        for (int i = 0; i < workerCount; i++) {
            new Thread() {
                public void run() {
                    try {
                        barrier.await();
                        for (int i = 0; i < enqueueCount / workerCount; i++) {
                            queue.incrementAndGet();
                            runner.wakeup();
                            yield();
                        }
                    } catch (BrokenBarrierException e) {
                    } catch (InterruptedException e) {
                    }
                }
            }.start();
        }
        barrier.await();

        boolean b = doneCountDownLatch.await(30, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        LOG.info("Iterations: " + iterations.get());
        LOG.info("counter: " + counter.get());
        LOG.info("Dequeues/s: " + (1000.0 * enqueueCount / (end - start)));
        LOG.info("duration: " + ((end - start) / 1000.0));
        assertTrue(b);

        runner.shutdown();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(TaskRunnerTest.class);
    }

}
