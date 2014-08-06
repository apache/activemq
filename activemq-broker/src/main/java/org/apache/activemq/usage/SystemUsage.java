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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.activemq.Service;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PersistenceAdapter;

/**
 * Holder for Usage instances for memory, store and temp files Main use case is
 * manage memory usage.
 *
 * @org.apache.xbean.XBean
 *
 */
public class SystemUsage implements Service {

    private SystemUsage parent;
    private String name;
    private MemoryUsage memoryUsage;
    private StoreUsage storeUsage;
    private TempUsage tempUsage;
    private ThreadPoolExecutor executor;
    private JobSchedulerUsage jobSchedulerUsage;
    private String checkLimitsLogLevel = "warn";

    /**
     * True if someone called setSendFailIfNoSpace() on this particular usage
     * manager
     */
    private boolean sendFailIfNoSpaceExplicitySet;
    private boolean sendFailIfNoSpace;
    private boolean sendFailIfNoSpaceAfterTimeoutExplicitySet;
    private long sendFailIfNoSpaceAfterTimeout = 0;

    private final List<SystemUsage> children = new CopyOnWriteArrayList<SystemUsage>();

    public SystemUsage() {
        this("default", null, null, null);
    }

    public SystemUsage(String name, PersistenceAdapter adapter, PListStore tempStore, JobSchedulerStore jobSchedulerStore) {
        this.parent = null;
        this.name = name;
        this.memoryUsage = new MemoryUsage(name + ":memory");
        this.storeUsage = new StoreUsage(name + ":store", adapter);
        this.tempUsage = new TempUsage(name + ":temp", tempStore);
        this.jobSchedulerUsage = new JobSchedulerUsage(name + ":jobScheduler", jobSchedulerStore);
        this.memoryUsage.setExecutor(getExecutor());
        this.storeUsage.setExecutor(getExecutor());
        this.tempUsage.setExecutor(getExecutor());
    }

    public SystemUsage(SystemUsage parent, String name) {
        this.parent = parent;
        this.executor = parent.getExecutor();
        this.name = name;
        this.memoryUsage = new MemoryUsage(parent.memoryUsage, name + ":memory");
        this.storeUsage = new StoreUsage(parent.storeUsage, name + ":store");
        this.tempUsage = new TempUsage(parent.tempUsage, name + ":temp");
        this.jobSchedulerUsage = new JobSchedulerUsage(parent.jobSchedulerUsage, name + ":jobScheduler");
        this.memoryUsage.setExecutor(getExecutor());
        this.storeUsage.setExecutor(getExecutor());
        this.tempUsage.setExecutor(getExecutor());
    }

    public String getName() {
        return name;
    }

    /**
     * @return the memoryUsage
     */
    public MemoryUsage getMemoryUsage() {
        return this.memoryUsage;
    }

    /**
     * @return the storeUsage
     */
    public StoreUsage getStoreUsage() {
        return this.storeUsage;
    }

    /**
     * @return the tempDiskUsage
     */
    public TempUsage getTempUsage() {
        return this.tempUsage;
    }

    /**
     * @return the schedulerUsage
     */
    public JobSchedulerUsage getJobSchedulerUsage() {
        return this.jobSchedulerUsage;
    }

    @Override
    public String toString() {
        return "UsageManager(" + getName() + ")";
    }

    @Override
    public void start() {
        if (parent != null) {
            parent.addChild(this);
        }
        this.memoryUsage.start();
        this.storeUsage.start();
        this.tempUsage.start();
        this.jobSchedulerUsage.start();
    }

    @Override
    public void stop() {
        if (parent != null) {
            parent.removeChild(this);
        }
        this.memoryUsage.stop();
        this.storeUsage.stop();
        this.tempUsage.stop();
        this.jobSchedulerUsage.stop();
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

    private void addChild(SystemUsage child) {
        children.add(child);
    }

    private void removeChild(SystemUsage child) {
        children.remove(child);
    }

    public SystemUsage getParent() {
        return parent;
    }

    public void setParent(SystemUsage parent) {
        this.parent = parent;
    }

    public boolean isSendFailIfNoSpaceExplicitySet() {
        return sendFailIfNoSpaceExplicitySet;
    }

    public void setSendFailIfNoSpaceExplicitySet(boolean sendFailIfNoSpaceExplicitySet) {
        this.sendFailIfNoSpaceExplicitySet = sendFailIfNoSpaceExplicitySet;
    }

    public long getSendFailIfNoSpaceAfterTimeout() {
        if (sendFailIfNoSpaceAfterTimeoutExplicitySet || parent == null) {
            return sendFailIfNoSpaceAfterTimeout;
        } else {
            return parent.getSendFailIfNoSpaceAfterTimeout();
        }
    }

    public void setSendFailIfNoSpaceAfterTimeout(long sendFailIfNoSpaceAfterTimeout) {
        this.sendFailIfNoSpaceAfterTimeoutExplicitySet = true;
        this.sendFailIfNoSpaceAfterTimeout = sendFailIfNoSpaceAfterTimeout;
    }

    public void setName(String name) {
        this.name = name;
        this.memoryUsage.setName(name + ":memory");
        this.storeUsage.setName(name + ":store");
        this.tempUsage.setName(name + ":temp");
        this.jobSchedulerUsage.setName(name + ":jobScheduler");
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
        if (memoryUsage.getName() == null) {
            memoryUsage.setName(this.memoryUsage.getName());
        }
        if (parent != null) {
            memoryUsage.setParent(parent.memoryUsage);
        }
        this.memoryUsage = memoryUsage;
        this.memoryUsage.setExecutor(getExecutor());
    }

    public void setStoreUsage(StoreUsage storeUsage) {
        if (storeUsage.getStore() == null) {
            storeUsage.setStore(this.storeUsage.getStore());
        }
        if (storeUsage.getName() == null) {
            storeUsage.setName(this.storeUsage.getName());
        }
        if (parent != null) {
            storeUsage.setParent(parent.storeUsage);
        }
        this.storeUsage = storeUsage;
        this.storeUsage.setExecutor(executor);
    }

    public void setTempUsage(TempUsage tempDiskUsage) {
        if (tempDiskUsage.getStore() == null) {
            tempDiskUsage.setStore(this.tempUsage.getStore());
        }
        if (tempDiskUsage.getName() == null) {
            tempDiskUsage.setName(this.tempUsage.getName());
        }
        if (parent != null) {
            tempDiskUsage.setParent(parent.tempUsage);
        }
        this.tempUsage = tempDiskUsage;
        this.tempUsage.setExecutor(getExecutor());
    }

    public void setJobSchedulerUsage(JobSchedulerUsage jobSchedulerUsage) {
        if (jobSchedulerUsage.getStore() == null) {
            jobSchedulerUsage.setStore(this.jobSchedulerUsage.getStore());
        }
        if (jobSchedulerUsage.getName() == null) {
            jobSchedulerUsage.setName(this.jobSchedulerUsage.getName());
        }
        if (parent != null) {
            jobSchedulerUsage.setParent(parent.jobSchedulerUsage);
        }
        this.jobSchedulerUsage = jobSchedulerUsage;
        this.jobSchedulerUsage.setExecutor(getExecutor());
    }

    /**
     * @return the executor
     */
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

    /**
     * @param executor
     *            the executor to set
     */
    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
        if (this.memoryUsage != null) {
            this.memoryUsage.setExecutor(this.executor);
        }
        if (this.storeUsage != null) {
            this.storeUsage.setExecutor(this.executor);
        }
        if (this.tempUsage != null) {
            this.tempUsage.setExecutor(this.executor);
        }
        if(this.jobSchedulerUsage != null) {
            this.jobSchedulerUsage.setExecutor(this.executor);
        }
    }

   public String getCheckLimitsLogLevel() {
       return checkLimitsLogLevel;
   }

   public void setCheckLimitsLogLevel(String checkLimitsLogLevel) {
       this.checkLimitsLogLevel = checkLimitsLogLevel;
   }
}
