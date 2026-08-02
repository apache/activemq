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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to keep track of how much of something is being used so that a productive working set usage can be controlled.
 * Main use case is manage memory usage.
 *
 * @org.apache.xbean.XBean
 *
 */
public abstract class Usage<T extends Usage> implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(Usage.class);

    protected final ReentrantReadWriteLock usageLock = new ReentrantReadWriteLock();
    protected final Condition waitForSpaceCondition = usageLock.writeLock().newCondition();
    // volatile so lock-free hot paths (isFull, percent-change detection) can read it without
    // taking the usageLock; all writes still happen under the writeLock via setPercentUsage().
    protected volatile int percentUsage;
    // The absolute usage-value bounds of the current percentUsage bucket, kept in one volatile
    // reference (immutable pair, cannot tear). Installed under the write lock whenever
    // percentUsage is published (setPercentUsage/refreshPercentUsage), which also covers limit
    // and percentUsageMinDelta changes via onLimitChange/setPercentUsageMinDelta. Lock-free
    // hot paths compare a usage value against it to decide if a locked recompute is needed.
    protected volatile PercentBounds bounds = PercentBounds.ALWAYS_CROSS;
    protected T parent;
    protected String name;

    private UsageCapacity limiter = new DefaultUsageCapacity();
    private volatile int percentUsageMinDelta = 1;
    private final List<UsageListener> listeners = new CopyOnWriteArrayList<UsageListener>();
    private final boolean debug = LOG.isDebugEnabled();
    private float usagePortion = 1.0f;
    private final List<T> children = new CopyOnWriteArrayList<T>();
    private final List<Runnable> callbacks = new LinkedList<Runnable>();
    private volatile int pollingTime = 100;
    private final AtomicBoolean started = new AtomicBoolean();
    private ThreadPoolExecutor executor;

    public Usage(T parent, String name, float portion) {
        this.parent = parent;
        this.usagePortion = portion;
        if (parent != null) {
            this.limiter.setLimit((long) (parent.getLimit() * (double)portion));
            name = parent.name + ":" + name;
        }
        this.name = name;
    }

    protected abstract long retrieveUsage();

    /**
     * @throws InterruptedException
     */
    public void waitForSpace() throws InterruptedException {
        waitForSpace(0);
    }

    public boolean waitForSpace(long timeout) throws InterruptedException {
        return waitForSpace(timeout, 100);
    }

    /**
     * @param timeout
     * @throws InterruptedException
     * @return true if space
     */
    public boolean waitForSpace(long timeout, int highWaterMark) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout, highWaterMark)) {
                return false;
            }
        }
        usageLock.writeLock().lock();
        try {
            refreshPercentUsage(caclPercentUsage());
            if (percentUsage >= highWaterMark) {
                long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
                long timeleft = deadline;
                while (timeleft > 0) {
                    refreshPercentUsage(caclPercentUsage());
                    if (percentUsage >= highWaterMark) {
                        waitForSpaceCondition.await(pollingTime, TimeUnit.MILLISECONDS);
                        timeleft = deadline - System.currentTimeMillis();
                    } else {
                        break;
                    }
                }
            }
            return percentUsage < highWaterMark;
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public boolean isFull() {
        return isFull(100);
    }

    public boolean isFull(int highWaterMark) {
        if (parent != null && parent.isFull(highWaterMark)) {
            return true;
        }
        // Fast path: while the usage value stays inside the cached percent bucket the
        // published percentUsage is still valid - no lock, no division. retrieveUsage() is
        // safe to call unlocked for every implementation (atomic counters or constant).
        if (bounds.contains(retrieveUsage())) {
            return percentUsage >= highWaterMark;
        }
        usageLock.writeLock().lock();
        try {
            refreshPercentUsage(caclPercentUsage());
            return percentUsage >= highWaterMark;
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public void addUsageListener(UsageListener listener) {
        listeners.add(listener);
    }

    public void removeUsageListener(UsageListener listener) {
        listeners.remove(listener);
    }

    public int getNumUsageListeners() {
        return listeners.size();
    }

    public long getLimit() {
        usageLock.readLock().lock();
        try {
            return limiter.getLimit();
        } finally {
            usageLock.readLock().unlock();
        }
    }

    /**
     * Sets the memory limit in bytes. Setting the limit in bytes will set the usagePortion to 0 since the UsageManager
     * is not going to be portion based off the parent. When set using Xbean, values of the form "20 Mb", "1024kb", and
     * "1g" can be used
     *
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setLimit(long limit) {
        if (percentUsageMinDelta < 0) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        usageLock.writeLock().lock();
        try {
            this.limiter.setLimit(limit);
            this.usagePortion = 0;
        } finally {
            usageLock.writeLock().unlock();
        }
        onLimitChange();
    }

    protected void onLimitChange() {
        // We may need to calculate the limit
        if (usagePortion > 0 && parent != null) {
            usageLock.writeLock().lock();
            try {
                this.limiter.setLimit((long) (parent.getLimit() * (double) usagePortion));
            } finally {
                usageLock.writeLock().unlock();
            }
        }
        // Reset the percent currently being used.
        usageLock.writeLock().lock();
        try {
            setPercentUsage(caclPercentUsage());
        } finally {
            usageLock.writeLock().unlock();
        }
        // Let the children know that the limit has changed. They may need to
        // set their limits based on ours.
        for (T child : children) {
            child.onLimitChange();
        }
    }

    public float getUsagePortion() {
        usageLock.readLock().lock();
        try {
            return usagePortion;
        } finally {
            usageLock.readLock().unlock();
        }
    }

    public void setUsagePortion(float usagePortion) {
        usageLock.writeLock().lock();
        try {
            this.usagePortion = usagePortion;
        } finally {
            usageLock.writeLock().unlock();
        }
        onLimitChange();
    }

    public int getPercentUsage() {
        // Fresh-on-read without a lock: if the usage value has crossed out of the cached
        // percent bucket, take the write lock once and silently refresh (no listener events -
        // preserving the historical behavior of read-driven recomputes). Subclasses whose
        // usage value changes externally (StoreUsage/TempUsage/JobSchedulerUsage) get accurate
        // reads from this shared path instead of per-class write-locked overrides.
        if (!bounds.contains(retrieveUsage())) {
            usageLock.writeLock().lock();
            try {
                refreshPercentUsage(caclPercentUsage());
            } finally {
                usageLock.writeLock().unlock();
            }
        }
        return percentUsage;
    }

    public int getPercentUsageMinDelta() {
        // volatile field - no lock needed; also avoids a nested read-lock acquisition when
        // called from subclass code already holding the write lock (MemoryUsage.computeBounds)
        return percentUsageMinDelta;
    }

    /**
     * Sets the minimum number of percentage points the usage has to change before a UsageListener event is fired by the
     * manager.
     *
     * @param percentUsageMinDelta
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setPercentUsageMinDelta(int percentUsageMinDelta) {
        if (percentUsageMinDelta < 1) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater than 0");
        }

        usageLock.writeLock().lock();
        try {
            this.percentUsageMinDelta = percentUsageMinDelta;
            setPercentUsage(caclPercentUsage());
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public long getUsage() {
        usageLock.readLock().lock();
        try {
            return retrieveUsage();
        } finally {
            usageLock.readLock().unlock();
        }
    }

    protected void setPercentUsage(int value) {
        usageLock.writeLock().lock();
        try {
            int oldValue = percentUsage;
            percentUsage = value;
            bounds = PercentBounds.compute(value, limiter.getLimit(), percentUsageMinDelta);
            if (oldValue != value) {
                fireEvent(oldValue, value);
            }
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    /**
     * Silently refresh the cached percentUsage (no listener events, no waiter signalling) -
     * used by the internal recompute sites in waitForSpace(long,int) and isFull(int).
     * Must be called with the usageLock write lock held. Subclasses that cache values derived
     * from percentUsage override this to refresh them in the same critical section.
     */
    protected void refreshPercentUsage(int value) {
        percentUsage = value;
        bounds = PercentBounds.compute(value, limiter.getLimit(), percentUsageMinDelta);
    }

    protected int caclPercentUsage() {
        if (limiter.getLimit() == 0) {
            return 0;
        }
        return (int) ((((retrieveUsage() * 100) / limiter.getLimit()) / percentUsageMinDelta) * percentUsageMinDelta);
    }

    // Must be called with the usage lock's writeLock held.
    private void fireEvent(final int oldPercentUsage, final int newPercentUsage) {
        if (debug) {
            LOG.debug(getName() + ": usage change from: " + oldPercentUsage + "% of available memory, to: " + newPercentUsage + "% of available memory");
        }
        if (started.get()) {
            // Switching from being full to not being full..
            if (oldPercentUsage >= 100 && newPercentUsage < 100) {
                waitForSpaceCondition.signalAll();
                if (!callbacks.isEmpty()) {
                    for (Runnable callback : callbacks) {
                        getExecutor().execute(callback);
                    }
                    callbacks.clear();
                }
            }
            if (!listeners.isEmpty()) {
                // Let the listeners know on a separate thread
                Runnable listenerNotifier = new Runnable() {
                    @Override
                    public void run() {
                        for (UsageListener listener : listeners) {
                            listener.onUsageChanged(Usage.this, oldPercentUsage, newPercentUsage);
                        }
                    }
                };
                if (started.get()) {
                    getExecutor().execute(listenerNotifier);
                } else {
                    LOG.warn("Not notifying memory usage change to listeners on shutdown");
                }
            }
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Usage(" + getName() + ") percentUsage=" + percentUsage + "%, usage=" + retrieveUsage() + ", limit=" + limiter.getLimit()
            + ", percentUsageMinDelta=" + percentUsageMinDelta + "%" + (parent != null ? ";Parent:" + parent.toString() : "");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        if (started.compareAndSet(false, true)) {
            if (parent != null) {
                parent.addChild(this);
                if (getLimit() > parent.getLimit()) {
                    LOG.info("Usage({}) limit={} should be smaller than its parent limit={}", new Object[] { getName(), getLimit(), parent.getLimit() });
                }
            }
            for (T t : children) {
                t.start();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void stop() {
        if (started.compareAndSet(true, false)) {
            if (parent != null) {
                parent.removeChild(this);
            }

            // clear down any callbacks
            usageLock.writeLock().lock();
            try {
                waitForSpaceCondition.signalAll();
                for (Runnable callback : this.callbacks) {
                    callback.run();
                }
                this.callbacks.clear();
            } finally {
                usageLock.writeLock().unlock();
            }

            for (T t : children) {
                t.stop();
            }
        }
    }

    protected void addChild(T child) {
        children.add(child);
        if (started.get()) {
            child.start();
        }
    }

    protected void removeChild(T child) {
        children.remove(child);
    }

    protected List<T> getChildren() {
        return children;
    }

    /**
     * @param callback
     * @return true if the UsageManager was full. The callback will only be called if this method returns true.
     */
    public boolean notifyCallbackWhenNotFull(final Runnable callback) {
        if (parent != null) {
            Runnable r = new Runnable() {

                @Override
                public void run() {
                    usageLock.writeLock().lock();
                    try {
                        if (percentUsage >= 100) {
                            callbacks.add(callback);
                        } else {
                            callback.run();
                        }
                    } finally {
                        usageLock.writeLock().unlock();
                    }
                }
            };
            if (parent.notifyCallbackWhenNotFull(r)) {
                return true;
            }
        }
        usageLock.writeLock().lock();
        try {
            if (percentUsage >= 100) {
                callbacks.add(callback);
                return true;
            } else {
                return false;
            }
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    /**
     * @return the limiter
     */
    public UsageCapacity getLimiter() {
        return this.limiter;
    }

    /**
     * Creation-time setter. Swapping the limiter on a live Usage does not trigger
     * onLimitChange(): percentUsage - and any subclass caches derived from it, such as
     * MemoryUsage's percent bucket bounds - remain stale until the next recompute.
     *
     * @param limiter
     *            the limiter to set
     */
    public void setLimiter(UsageCapacity limiter) {
        this.limiter = limiter;
    }

    /**
     * @return the pollingTime
     */
    public int getPollingTime() {
        return this.pollingTime;
    }

    /**
     * @param pollingTime
     *            the pollingTime to set
     */
    public void setPollingTime(int pollingTime) {
        this.pollingTime = pollingTime;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getParent() {
        return parent;
    }

    public void setParent(T parent) {
        this.parent = parent;
    }

    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public boolean isStarted() {
        return started.get();
    }
}
