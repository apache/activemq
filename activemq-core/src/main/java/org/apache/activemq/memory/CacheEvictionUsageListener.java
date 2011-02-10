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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheEvictionUsageListener implements UsageListener {

    private static final Logger LOG = LoggerFactory.getLogger(CacheEvictionUsageListener.class);

    private final List<CacheEvictor> evictors = new CopyOnWriteArrayList<CacheEvictor>();
    private final int usageHighMark;
    private final int usageLowMark;

    private final TaskRunner evictionTask;
    private final Usage usage;

    public CacheEvictionUsageListener(Usage usage, int usageHighMark, int usageLowMark, TaskRunnerFactory taskRunnerFactory) {
        this.usage = usage;
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
        LOG.debug("Evicting cache memory usage: " + usage.getPercentUsage());

        List<CacheEvictor> list = new LinkedList<CacheEvictor>(evictors);
        while (list.size() > 0 && usage.getPercentUsage() > usageLowMark) {

            // Evenly evict messages from all evictors
            for (Iterator<CacheEvictor> iter = list.iterator(); iter.hasNext();) {
                CacheEvictor evictor = iter.next();
                if (evictor.evictCacheEntry() == null) {
                    iter.remove();
                }
            }
        }
        return false;
    }

    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        // Do we need to start evicting cache entries? Usage > than the
        // high mark
        if (oldPercentUsage < newPercentUsage && usage.getPercentUsage() >= usageHighMark) {
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
