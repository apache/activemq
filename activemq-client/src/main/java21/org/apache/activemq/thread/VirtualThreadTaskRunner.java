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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskRunner that runs the task loop on one long-lived virtual thread.
 * The thread parks when the task has no work and wakeup() unparks it.
 *
 * Creating a virtual thread per wakeup causes high thread creation rates
 * for tasks that go idle every few iterations, such as dispatch queues
 * with rapidly acking consumers. One thread per runner avoids the creation cost.
 */
class VirtualThreadTaskRunner implements TaskRunner {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualThreadTaskRunner.class);

    private final int maxIterationsPerRun;
    private final Task task;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wakeupCondition = lock.newCondition();
    private final Condition terminationCondition = lock.newCondition();
    // wakeup() sets this with a CAS and only the CAS winner takes the lock
    // to signal. The loop clears it before calling iterate(), so a wakeup
    // that arrives after iterate() returns false is seen before the park.
    private final AtomicBoolean pending = new AtomicBoolean();
    private volatile boolean shutdown;
    private boolean terminated;
    private volatile Thread runningThread;

    public VirtualThreadTaskRunner(Executor executor, final Task task, int maxIterationsPerRun) {
        this.task = task;
        this.maxIterationsPerRun = maxIterationsPerRun;
        // The executor starts a virtual thread per submission. This is the
        // only submission for this runner.
        executor.execute(new Runnable() {
            @Override
            public void run() {
                runningThread = Thread.currentThread();
                try {
                    runTask();
                } finally {
                    LOG.trace("Run task done: {}", task);
                    runningThread = null;
                }
            }
        });
    }

    @Override
    public void wakeup() throws InterruptedException {
        if (shutdown) {
            return;
        }
        if (pending.compareAndSet(false, true)) {
            lock.lock();
            try {
                wakeupCondition.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void shutdown(long timeout) throws InterruptedException {
        LOG.trace("Shutdown timeout: {} task: {}", timeout, task);
        lock.lock();
        try {
            shutdown = true;
            pending.set(true);
            wakeupCondition.signal();
            // skip the wait when the task thread calls shutdown on itself
            if (timeout > 0 && runningThread != Thread.currentThread() && !terminated) {
                terminationCondition.await(timeout, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        shutdown(0);
    }

    final void runTask() {
        try {
            int sinceYield = 0;
            while (!shutdown) {
                // clear before iterate() so a wakeup during the call leaves
                // the flag set and the loop runs again instead of parking
                pending.set(false);
                if (task.iterate()) {
                    if (++sinceYield >= maxIterationsPerRun) {
                        sinceYield = 0;
                        Thread.yield();
                    }
                    continue;
                }
                sinceYield = 0;
                lock.lock();
                try {
                    while (!pending.get() && !shutdown) {
                        wakeupCondition.await();
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            LOG.error("Virtual Thread task runner terminating on unexpected error: {}", task, t);
            throw t;
        } finally {
            lock.lock();
            try {
                terminated = true;
                terminationCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
