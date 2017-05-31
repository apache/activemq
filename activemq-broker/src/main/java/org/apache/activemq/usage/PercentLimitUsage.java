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


import org.apache.activemq.util.StoreUtil;

import java.io.File;

public abstract class PercentLimitUsage <T extends Usage> extends Usage<T> {

    protected int percentLimit = 0;
    protected long total = 0;

    /**
     * @param parent
     * @param name
     * @param portion
     */
    public PercentLimitUsage(T parent, String name, float portion) {
        super(parent, name, portion);
    }

    public void setPercentLimit(int percentLimit) {
        usageLock.writeLock().lock();
        try {
            this.percentLimit = percentLimit;
            updateLimitBasedOnPercent();
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public int getPercentLimit() {
        usageLock.readLock().lock();
        try {
            return percentLimit;
        } finally {
            usageLock.readLock().unlock();
        }
    }

    /**
     * Sets the total available space in bytes. When non zero, the filesystem totalAvailableSpace is ignored.
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can be used
     *
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setTotal(long max) {
        this.total = max;
    }

    public long getTotal() {
        return total;
    }


    protected void percentLimitFromFile(File directory) {
        if (percentLimit > 0) {
            if (total > 0) {
                this.setLimit(total * percentLimit / 100);
            } else if (directory != null) {
                File dir = StoreUtil.findParentDirectory(directory);
                if (dir != null) {
                    this.setLimit(dir.getTotalSpace() * percentLimit / 100);
                }
            }
        }
    }

    protected abstract void updateLimitBasedOnPercent();
}
