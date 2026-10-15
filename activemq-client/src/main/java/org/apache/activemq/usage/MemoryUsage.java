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

    /*
     * Usage is updated without taking usageLock: it is changed on every message reference count change, and a
     * broker-wide ReentrantReadWriteLock acquired on that path serialises all producer and dispatch threads.
     *
     * percentUsage is still only written with the write lock held (so listeners, waitForSpace waiters and
     * notifyCallbackWhenNotFull callbacks are signalled exactly as before), but it is only taken when the
     * percentage actually changes. Every writer of percentUsage re-reads usage after publishing (see
     * updatePercentUsage), and every lock-free updater reads percentUsage after changing usage, so a stale
     * percentage cannot survive a concurrent update.
     */
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
        if (percentUsage < 100) {
            return;
        }
        usageLock.writeLock().lock();
        try {
            while (percentUsage >= 100 && isStarted()) {
                waitForSpaceCondition.await();
            }
            if (percentUsage >= 100 && !isStarted()) {
                throw new InterruptedException("waitForSpace stopped during wait.");
            }
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    /**
     * @param timeout
     * @throws InterruptedException
     * @return true if space
     */
    @Override
    public boolean waitForSpace(final long timeout) throws InterruptedException {
        return waitForSpace(timeout, 100);
    }

    @Override
    public boolean waitForSpace(final long timeout, final int highWaterMark) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout, highWaterMark)) {
                return false;
            }
        }
        if (percentUsage < highWaterMark) {
            return true;
        }
        usageLock.writeLock().lock();
        try {
            final long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
            long timeleft = deadline;
            while (percentUsage >= highWaterMark && timeleft > 0) {
                waitForSpaceCondition.await(Math.min(getPollingTime(), timeleft), TimeUnit.MILLISECONDS);
                timeleft = deadline - System.currentTimeMillis();
            }
            return percentUsage < highWaterMark;
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isFull() {
        if (parent != null && parent.isFull()) {
            return true;
        }
        return percentUsage >= 100;
    }

    @Override
    public boolean isFull(int highWaterMark) {
        if (parent != null && parent.isFull(highWaterMark)) {
            return true;
        }
        return percentUsage >= highWaterMark;
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

        usage.addAndGet(value);
        updatePercentUsage();

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

        usage.addAndGet(-value);
        updatePercentUsage();

        if (parent != null) {
            parent.decreaseUsage(value);
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

    public void setUsage(long usage) {
        this.usage.set(usage);
        updatePercentUsage();
    }

    @Override
    protected void onLimitChange() {
        super.onLimitChange();
        updatePercentUsage();
    }

    @Override
    public void setPercentUsageMinDelta(int percentUsageMinDelta) {
        super.setPercentUsageMinDelta(percentUsageMinDelta);
        updatePercentUsage();
    }

    /**
     * Publishes the percentage for the current usage, taking the write lock only when it has changed. The value is
     * re-checked after it is published so that a concurrent lock-free usage change that read the old percentage
     * cannot leave a stale value behind.
     */
    private void updatePercentUsage() {
        if (caclPercentUsage() == percentUsage) {
            return;
        }
        usageLock.writeLock().lock();
        try {
            int value;
            do {
                value = caclPercentUsage();
                setPercentUsage(value);
            } while (value != caclPercentUsage());
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public void setPercentOfJvmHeap(int percentOfJvmHeap) {
        if (percentOfJvmHeap > 0) {
            setLimit(Math.round(Runtime.getRuntime().maxMemory() * percentOfJvmHeap / 100.0));
        }
    }
}
