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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CacheEvictionUsageListener implements UsageListener {

    private static final Log LOG = LogFactory.getLog(CacheEvictionUsageListener.class);

    private final CopyOnWriteArrayList evictors = new CopyOnWriteArrayList();
    private final int usageHighMark;
    private final int usageLowMark;

    private final TaskRunner evictionTask;
    private final UsageManager usageManager;

    public CacheEvictionUsageListener(UsageManager usageManager, int usageHighMark, int usageLowMark,
                                      TaskRunnerFactory taskRunnerFactory) {
        this.usageManager = usageManager;
        this.usageHighMark = usageHighMark;
        this.usageLowMark = usageLowMark;
        evictionTask = taskRunnerFactory.createTaskRunner(new Task() {
            public boolean iterate() {
                return evictMessages();
            }
        }, "Cache Evictor: " + System.identityHashCode(this));
    }

    boolean evictMessages() {
        // Try to take the memory usage down below the low mark.
        try {
            LOG.debug("Evicting cache memory usage: " + usageManager.getPercentUsage());

            LinkedList list = new LinkedList(evictors);
            while (list.size() > 0 && usageManager.getPercentUsage() > usageLowMark) {

                // Evenly evict messages from all evictors
                for (Iterator iter = list.iterator(); iter.hasNext();) {
                    CacheEvictor evictor = (CacheEvictor)iter.next();
                    if (evictor.evictCacheEntry() == null)
                        iter.remove();
                }
            }
        } finally {
        }
        return false;
    }

    public void onMemoryUseChanged(UsageManager memoryManager, int oldPercentUsage, int newPercentUsage) {
        // Do we need to start evicting cache entries? Usage > than the
        // high mark
        if (oldPercentUsage < newPercentUsage && memoryManager.getPercentUsage() >= usageHighMark) {
            try {
                evictionTask.wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void add(CacheEvictor evictor) {
        evictors.add(evictor);
    }

    public void remove(CacheEvictor evictor) {
        evictors.remove(evictor);
    }
}
