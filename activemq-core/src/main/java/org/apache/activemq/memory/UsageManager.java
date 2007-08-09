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
package org.apache.activemq.memory;

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
 * productive working set usage can be controlled.
 * 
 * Main use case is manage memory usage.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.3 $
 */
public class UsageManager implements Service {

    private static final Log LOG = LogFactory.getLog(UsageManager.class);

    private final UsageManager parent;
    private long limit;
    private long usage;

    private int percentUsage;
    private int percentUsageMinDelta = 1;

    private final Object usageMutex = new Object();

    private final CopyOnWriteArrayList listeners = new CopyOnWriteArrayList();

    private boolean sendFailIfNoSpace;

    /**
     * True if someone called setSendFailIfNoSpace() on this particular usage
     * manager
     */
    private boolean sendFailIfNoSpaceExplicitySet;
    private final boolean debug = LOG.isDebugEnabled();
    private String name = "";
    private float usagePortion = 1.0f;
    private List<UsageManager> children = new CopyOnWriteArrayList<UsageManager>();
    private final LinkedList<Runnable> callbacks = new LinkedList<Runnable>();

    public UsageManager() {
        this(null, "default");
    }

    /**
     * Create the memory manager linked to a parent. When the memory manager is
     * linked to a parent then when usage increased or decreased, the parent's
     * usage is also increased or decreased.
     * 
     * @param parent
     */
    public UsageManager(UsageManager parent) {
        this(parent, "default");
    }

    public UsageManager(String name) {
        this(null, name);
    }

    public UsageManager(UsageManager parent, String name) {
        this(parent, name, 1.0f);
    }

    public UsageManager(UsageManager parent, String name, float portion) {
        this.parent = parent;
        this.usagePortion = portion;
        if (parent != null) {
            this.limit = (long)(parent.limit * portion);
            this.name = parent.name + ":";
        }
        this.name += name;
    }

    /**
     * Tries to increase the usage by value amount but blocks if this object is
     * currently full.
     * 
     * @throws InterruptedException
     */
    public void enqueueUsage(long value) throws InterruptedException {
        waitForSpace();
        increaseUsage(value);
    }

    /**
     * @throws InterruptedException
     */
    public void waitForSpace() throws InterruptedException {
        if (parent != null)
            parent.waitForSpace();
        synchronized (usageMutex) {
            for (int i = 0; percentUsage >= 100; i++) {
                usageMutex.wait();
            }
        }
    }

    /**
     * @throws InterruptedException
     * 
     * @param timeout
     */
    public boolean waitForSpace(long timeout) throws InterruptedException {
        if (parent != null) {
            if (!parent.waitForSpace(timeout))
                return false;
        }
        synchronized (usageMutex) {
            if (percentUsage >= 100) {
                usageMutex.wait(timeout);
            }
            return percentUsage < 100;
        }
    }

    /**
     * Increases the usage by the value amount.
     * 
     * @param value
     */
    public void increaseUsage(long value) {
        if (value == 0)
            return;
        if (parent != null)
            parent.increaseUsage(value);
        int percentUsage;
        synchronized (usageMutex) {
            usage += value;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    /**
     * Decreases the usage by the value amount.
     * 
     * @param value
     */
    public void decreaseUsage(long value) {
        if (value == 0)
            return;
        if (parent != null)
            parent.decreaseUsage(value);
        int percentUsage;
        synchronized (usageMutex) {
            usage -= value;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
    }

    public boolean isFull() {
        if (parent != null && parent.isFull())
            return true;
        synchronized (usageMutex) {
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
            return limit;
        }
    }

    /**
     * Sets the memory limit in bytes. Setting the limit in bytes will set the
     * usagePortion to 0 since the UsageManager is not going to be portion based
     * off the parent.
     * 
     * When set using XBean, you can use values such as: "20 mb", "1024 kb", or
     * "1 gb"
     * 
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setLimit(long limit) {
        if (percentUsageMinDelta < 0) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        synchronized (usageMutex) {
            this.limit = limit;
            this.usagePortion = 0;
        }
        onLimitChange();
    }

    private void onLimitChange() {

        // We may need to calculate the limit
        if (usagePortion > 0 && parent != null) {
            synchronized (usageMutex) {
                limit = (long)(parent.getLimit() * usagePortion);
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
        for (UsageManager child : children) {
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
            return usage;
        }
    }

    /**
     * Sets whether or not a send() should fail if there is no space free. The
     * default value is false which means to block the send() method until space
     * becomes available
     */
    public void setSendFailIfNoSpace(boolean failProducerIfNoSpace) {
        sendFailIfNoSpaceExplicitySet = true;
        this.sendFailIfNoSpace = failProducerIfNoSpace;
    }

    public boolean isSendFailIfNoSpace() {
        if (sendFailIfNoSpaceExplicitySet || parent == null) {
            return sendFailIfNoSpace;
        } else {
            return parent.isSendFailIfNoSpace();
        }
    }

    private void setPercentUsage(int value) {
        synchronized (usageMutex) {
            int oldValue = percentUsage;
            percentUsage = value;
            if (oldValue != value) {
                fireEvent(oldValue, value);
            }
        }
    }

    private int caclPercentUsage() {
        if (limit == 0)
            return 0;
        return (int)((((usage * 100) / limit) / percentUsageMinDelta) * percentUsageMinDelta);
    }

    private void fireEvent(int oldPercentUsage, int newPercentUsage) {
        if (debug) {
            LOG.debug("Memory usage change.  from: " + oldPercentUsage + ", to: " + newPercentUsage);
        }
        // Switching from being full to not being full..
        if (oldPercentUsage >= 100 && newPercentUsage < 100) {
            synchronized (usageMutex) {
                usageMutex.notifyAll();
                for (Iterator iter = new ArrayList<Runnable>(callbacks).iterator(); iter.hasNext();) {
                    Runnable callback = (Runnable)iter.next();
                    callback.run();
                }
                callbacks.clear();
            }
        }
        // Let the listeners know
        for (Iterator iter = listeners.iterator(); iter.hasNext();) {
            UsageListener l = (UsageListener)iter.next();
            l.onMemoryUseChanged(this, oldPercentUsage, newPercentUsage);
        }
    }

    public String getName() {
        return name;
    }

    public String toString() {

        return "UsageManager(" + getName() + ") percentUsage=" + percentUsage + "%, usage=" + usage
               + " limit=" + limit + " percentUsageMinDelta=" + percentUsageMinDelta + "%";
    }

    public void start() {
        if (parent != null) {
            parent.addChild(this);
        }
    }

    public void stop() {
        if (parent != null) {
            parent.removeChild(this);
        }
    }

    private void addChild(UsageManager child) {
        children.add(child);
    }

    private void removeChild(UsageManager child) {
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

}
