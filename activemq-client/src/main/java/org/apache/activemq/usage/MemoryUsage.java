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

/**
 * Used to keep track of how much of something is being used so that a
 * productive working set usage can be controlled. Main use case is manage
 * memory usage.
 *
 * @org.apache.xbean.XBean
 *
 */
public class MemoryUsage extends Usage<MemoryUsage> {

    private long usage;

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
    public boolean waitForSpace(long timeout) throws InterruptedException {
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
                    while (percentUsage >= 100 ) {
                        waitForSpaceCondition.await(timeout, TimeUnit.MILLISECONDS);
                    }
                    usageLock.readLock().lock();
                } finally {
                    usageLock.writeLock().unlock();
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
        usageLock.readLock().lock();
        try {
            return percentUsage >= 100;
        } finally {
            usageLock.readLock().unlock();
        }
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

        usageLock.writeLock().lock();
        try {
            usage += value;
            setPercentUsage(caclPercentUsage());
        } finally {
            usageLock.writeLock().unlock();
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

        usageLock.writeLock().lock();
        try {
            usage -= value;
            setPercentUsage(caclPercentUsage());
        } finally {
            usageLock.writeLock().unlock();
        }

        if (parent != null) {
            parent.decreaseUsage(value);
        }
    }

    @Override
    protected long retrieveUsage() {
        return usage;
    }

    @Override
    public long getUsage() {
        return usage;
    }

    public void setUsage(long usage) {
        this.usage = usage;
    }

    public void setPercentOfJvmHeap(int percentOfJvmHeap) {
        if (percentOfJvmHeap > 0) {
            setLimit(Math.round(Runtime.getRuntime().maxMemory() * percentOfJvmHeap / 100.0));
        }
    }
}
