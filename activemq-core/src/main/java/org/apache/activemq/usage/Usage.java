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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Used to keep track of how much of something is being used so that a
 * productive working set usage can be controlled. Main use case is manage
 * memory usage.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision: 1.3 $
 */
public abstract class Usage<T extends Usage> implements Service {

    private static final Log LOG = LogFactory.getLog(Usage.class);
    protected final Object usageMutex = new Object();
    protected int percentUsage;
    protected T parent;
    private UsageCapacity limiter = new DefaultUsageCapacity();
    private int percentUsageMinDelta = 1;
    private final List<UsageListener> listeners = new CopyOnWriteArrayList<UsageListener>();
    private final boolean debug = LOG.isDebugEnabled();
    private String name;
    private float usagePortion = 1.0f;
    private List<T> children = new CopyOnWriteArrayList<T>();
    private final List<Runnable> callbacks = new LinkedList<Runnable>();
    private int pollingTime = 100;

    public Usage(T parent, String name, float portion) {
        this.parent = parent;
        this.usagePortion = portion;
        if (parent != null) {
            this.limiter.setLimit((long)(parent.getLimit() * portion));
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

    /**
     * @param timeout
     * @throws InterruptedException
     * @return true if space
     */
    public boolean waitForSpace(long timeout) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout)) {
                return false;
            }
        }
        synchronized (usageMutex) {
            caclPercentUsage();
            if (percentUsage >= 100) {
                long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;
                long timeleft = deadline;
                while (timeleft > 0) {
                    caclPercentUsage();
                    if (percentUsage >= 100) {
                        usageMutex.wait(pollingTime);
                        timeleft = deadline - System.currentTimeMillis();
                    } else {
                        break;
                    }
                }
            }
            return percentUsage < 100;
        }
    }

    public boolean isFull() {
        if (parent != null && parent.isFull()) {
            return true;
        }
        synchronized (usageMutex) {
            caclPercentUsage();
            return percentUsage >= 100;
        }
    }

    public void addUsageListener(UsageListener listener) {
        listeners.add(listener);
    }

    public void removeUsageListener(UsageListener listener) {
        listeners.remove(listener);
    }

    public long getLimit() {
        synchronized (usageMutex) {
            return limiter.getLimit();
        }
    }

    /**
     * Sets the memory limit in bytes. Setting the limit in bytes will set the
     * usagePortion to 0 since the UsageManager is not going to be portion based
     * off the parent. When set using XBean, you can use values such as: "20
     * mb", "1024 kb", or "1 gb"
     * 
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setLimit(long limit) {
        if (percentUsageMinDelta < 0) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        synchronized (usageMutex) {
            this.limiter.setLimit(limit);
            this.usagePortion = 0;
        }
        onLimitChange();
    }

    protected void onLimitChange() {
        // We may need to calculate the limit
        if (usagePortion > 0 && parent != null) {
            synchronized (usageMutex) {
                this.limiter.setLimit((long)(parent.getLimit() * usagePortion));
            }
        }
        // Reset the percent currently being used.
        int percentUsage;
        synchronized (usageMutex) {
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
        // Let the children know that the limit has changed. They may need to
        // set
        // their limits based on ours.
        for (T child : children) {
            child.onLimitChange();
        }
    }

    public float getUsagePortion() {
        synchronized (usageMutex) {
            return usagePortion;
        }
    }

    public void setUsagePortion(float usagePortion) {
        synchronized (usageMutex) {
            this.usagePortion = usagePortion;
        }
        onLimitChange();
    }

    /*
     * Sets the minimum number of percentage points the usage has to change
     * before a UsageListener event is fired by the manager.
     */
    public int getPercentUsage() {
        synchronized (usageMutex) {
            return percentUsage;
        }
    }

    public int getPercentUsageMinDelta() {
        synchronized (usageMutex) {
            return percentUsageMinDelta;
        }
    }

    /**
     * Sets the minimum number of percentage points the usage has to change
     * before a UsageListener event is fired by the manager.
     * 
     * @param percentUsageMinDelta
     */
    public void setPercentUsageMinDelta(int percentUsageMinDelta) {
        if (percentUsageMinDelta < 1) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater than 0");
        }
        int percentUsage;
        synchronized (usageMutex) {
            this.percentUsageMinDelta = percentUsageMinDelta;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    public long getUsage() {
        synchronized (usageMutex) {
            return retrieveUsage();
        }
    }

    protected void setPercentUsage(int value) {
        synchronized (usageMutex) {
            int oldValue = percentUsage;
            percentUsage = value;
            if (oldValue != value) {
                fireEvent(oldValue, value);
            }
        }
    }

    protected int caclPercentUsage() {
        if (limiter.getLimit() == 0) {
            return 0;
        }
        return (int)((((retrieveUsage() * 100) / limiter.getLimit()) / percentUsageMinDelta) * percentUsageMinDelta);
    }

    private void fireEvent(int oldPercentUsage, int newPercentUsage) {
        if (debug) {
            LOG.debug("Memory usage change.  from: " + oldPercentUsage + ", to: " + newPercentUsage);
        }
        // Switching from being full to not being full..
        if (oldPercentUsage >= 100 && newPercentUsage < 100) {
            synchronized (usageMutex) {
                usageMutex.notifyAll();
                for (Iterator<Runnable> iter = new ArrayList<Runnable>(callbacks).iterator(); iter.hasNext();) {
                    Runnable callback = iter.next();
                    callback.run();
                }
                callbacks.clear();
            }
        }
        // Let the listeners know
        for (Iterator<UsageListener> iter = listeners.iterator(); iter.hasNext();) {
            UsageListener l = iter.next();
            l.onUsageChanged(this, oldPercentUsage, newPercentUsage);
        }
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return "Usage(" + getName() + ") percentUsage=" + percentUsage + "%, usage=" + retrieveUsage() + " limit=" + limiter.getLimit() + " percentUsageMinDelta=" + percentUsageMinDelta + "%";
    }

    @SuppressWarnings("unchecked")
    public void start() {
        if (parent != null) {
            parent.addChild(this);
        }
    }

    @SuppressWarnings("unchecked")
    public void stop() {
        if (parent != null) {
            parent.removeChild(this);
        }
    }

    private void addChild(T child) {
        children.add(child);
    }

    private void removeChild(T child) {
        children.remove(child);
    }

    /**
     * @param callback
     * @return true if the UsageManager was full. The callback will only be
     *         called if this method returns true.
     */
    public boolean notifyCallbackWhenNotFull(final Runnable callback) {
        if (parent != null) {
            Runnable r = new Runnable() {

                public void run() {
                    synchronized (usageMutex) {
                        if (percentUsage >= 100) {
                            callbacks.add(callback);
                        } else {
                            callback.run();
                        }
                    }
                }
            };
            if (parent.notifyCallbackWhenNotFull(r)) {
                return true;
            }
        }
        synchronized (usageMutex) {
            if (percentUsage >= 100) {
                callbacks.add(callback);
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * @return the limiter
     */
    public UsageCapacity getLimiter() {
        return this.limiter;
    }

    /**
     * @param limiter the limiter to set
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
     * @param pollingTime the pollingTime to set
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
}
