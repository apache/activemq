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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to keep track of how much of something is being used so that a
 * productive working set usage can be controlled. Main use case is manage
 * memory usage.
 *
 * @org.apache.xbean.XBean
 *
 */
public class MemoryUsage extends Usage<MemoryUsage> {

    // Lock-free usage accounting: the counter is an AtomicLong so increase/decrease never
    // take an exclusive lock; the usageLock is only taken when the counter crosses out of the
    // current percent bucket (at most ~100/percentUsageMinDelta times per limit traversal),
    // which preserves listener events and waitForSpace signalling. AtomicLong was chosen over
    // a striped LongAdder after benchmarking showed equal throughput at 1-22 producer threads
    // on an 11-core machine, while AtomicLong keeps get() exact, makes setUsage() a plain
    // atomic set, and avoids per-instance cell inflation.
    private final AtomicLong usage = new AtomicLong();


    public MemoryUsage() {
        this(null, null);
    }

    /**
     * Create the memory manager linked to a parent. When the memory manager is
     * linked to a parent then when usage increased or decreased, the parent's
     * usage is also increased or decreased.
     *
     * @param parent
     */
    public MemoryUsage(MemoryUsage parent) {
        this(parent, "default");
    }

    public MemoryUsage(String name) {
        this(null, name);
    }

    public MemoryUsage(MemoryUsage parent, String name) {
        this(parent, name, 1.0f);
    }

    public MemoryUsage(MemoryUsage parent, String name, float portion) {
        super(parent, name, portion);
    }

    /**
     * @throws InterruptedException
     */
    @Override
    public void waitForSpace() throws InterruptedException {
        if (parent != null) {
            parent.waitForSpace();
        }
        usageLock.readLock().lock();
        try {
            if (percentUsage >= 100 && isStarted()) {
                usageLock.readLock().unlock();
                usageLock.writeLock().lock();
                try {
                    while (percentUsage >= 100 && isStarted()) {
                        waitForSpaceCondition.await();
                    }
                } finally {
                    usageLock.writeLock().unlock();
                    usageLock.readLock().lock();
                }
            }

            if (percentUsage >= 100 && !isStarted()) {
                throw new InterruptedException("waitForSpace stopped during wait.");
            }
        } finally {
            usageLock.readLock().unlock();
        }
    }

    /**
     * @param timeout
     * @throws InterruptedException
     * @return true if space
     */
    @Override
    public boolean waitForSpace(final long timeout) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout)) {
                return false;
            }
        }
        usageLock.readLock().lock();
        try {
            if (percentUsage >= 100) {
                usageLock.readLock().unlock();
                usageLock.writeLock().lock();
                try {
                    final long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
                    long timeleft = deadline;
                    while (percentUsage >= 100 && timeleft > 0) {
                        waitForSpaceCondition.await(Math.min(getPollingTime(), timeleft), TimeUnit.MILLISECONDS);
                        timeleft = deadline - System.currentTimeMillis();
                    }
                } finally {
                    usageLock.writeLock().unlock();
                    usageLock.readLock().lock();
                }
            }

            return percentUsage < 100;
        } finally {
            usageLock.readLock().unlock();
        }
    }

    @Override
    public boolean isFull() {
        if (parent != null && parent.isFull()) {
            return true;
        }
        // percentUsage is volatile; no lock needed for a read.
        return percentUsage >= 100;
    }

    /**
     * Tries to increase the usage by value amount but blocks if this object is
     * currently full.
     *
     * @param value
     * @throws InterruptedException
     */
    public void enqueueUsage(long value) throws InterruptedException {
        waitForSpace();
        increaseUsage(value);
    }

    /**
     * Increases the usage by the value amount.
     *
     * @param value
     */
    public void increaseUsage(long value) {
        if (value == 0) {
            return;
        }

        // INVARIANT: every usage.addAndGet() MUST be followed unconditionally by the bounds
        // check in the same method (no early return or throw between them). The liveness of
        // untimed waitForSpace() depends on it: the temporally last mutation compares the
        // complete counter value against the current bucket bounds, so a lasting
        // 100% -> <100% transition always reaches the locked updatePercent() path, which
        // signals waitForSpaceCondition. Breaking this ordering can strand waiters forever.
        final long v = usage.addAndGet(value);
        if (!bounds.contains(v)) {
            updatePercent();
        }

        if (parent != null) {
            parent.increaseUsage(value);
        }
    }

    /**
     * Decreases the usage by the value amount.
     *
     * @param value
     */
    public void decreaseUsage(long value) {
        if (value == 0) {
            return;
        }

        // INVARIANT: addAndGet() must be followed unconditionally by the bounds check
        // (see increaseUsage for the full liveness rationale).
        final long v = usage.addAndGet(-value);
        if (!bounds.contains(v)) {
            updatePercent();
        }

        if (parent != null) {
            parent.decreaseUsage(value);
        }
    }

    /**
     * Cold path, entered only when the counter crosses out of the cached percent bucket.
     * Recomputes percentUsage from the live counter and publishes it via setPercentUsage()
     * (firing listener events and signalling waitForSpace waiters), which also installs the
     * new bucket bounds. The recompute-after-publish loop makes the update race-proof: after
     * publishing we re-read the live counter, and either we observe a concurrent mutation
     * (loop and correct), or that mutation's addAndGet follows our read in the counter's
     * synchronization order - in which case its bounds check is guaranteed to see the bounds
     * we just published and takes this path itself.
     */
    private void updatePercent() {
        usageLock.writeLock().lock();
        try {
            int p;
            do {
                p = caclPercentUsage();
                setPercentUsage(p);
            } while (caclPercentUsage() != p);
        } finally {
            usageLock.writeLock().unlock();
        }
    }



    @Override
    protected long retrieveUsage() {
        return usage.get();
    }

    @Override
    public long getUsage() {
        return usage.get();
    }

    /**
     * Sets the usage to the given value as a single atomic store; a concurrent
     * increase/decrease linearizes cleanly before or after it. Note: as with the historical
     * field assignment, this does not propagate an adjustment to the parent usage.
     */
    public void setUsage(long value) {
        this.usage.set(value);
        updatePercent();
    }

    public void setPercentOfJvmHeap(int percentOfJvmHeap) {
        if (percentOfJvmHeap > 0) {
            setLimit(Math.round(Runtime.getRuntime().maxMemory() * percentOfJvmHeap / 100.0));
        }
    }
}
