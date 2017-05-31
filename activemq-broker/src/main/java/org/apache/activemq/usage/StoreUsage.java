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

import org.apache.activemq.store.PersistenceAdapter;

/**
 * Used to keep track of how much of something is being used so that a
 * productive working set usage can be controlled. Main use case is manage
 * memory usage.
 *
 * @org.apache.xbean.XBean
 *
 */
public class StoreUsage extends PercentLimitUsage<StoreUsage> {

    private PersistenceAdapter store;

    public StoreUsage() {
        super(null, null, 1.0f);
    }

    public StoreUsage(String name, PersistenceAdapter store) {
        super(null, name, 1.0f);
        this.store = store;
        updateLimitBasedOnPercent();
    }

    public StoreUsage(StoreUsage parent, String name) {
        super(parent, name, 1.0f);
        this.store = parent.store;
        updateLimitBasedOnPercent();
    }

    @Override
    protected long retrieveUsage() {
        if (store == null)
            return 0;
        return store.size();
    }

    public PersistenceAdapter getStore() {
        return store;
    }

    public void setStore(PersistenceAdapter store) {
        this.store = store;
        if (percentLimit > 0 && store != null) {
            //will trigger onLimitChange
            updateLimitBasedOnPercent();
        } else {
            onLimitChange();
        }
    }

    @Override
    public int getPercentUsage() {
        usageLock.writeLock().lock();
        try {
            percentUsage = caclPercentUsage();
            return super.getPercentUsage();
        } finally {
            usageLock.writeLock().unlock();
        }
    }


    @Override
    protected void updateLimitBasedOnPercent() {
        usageLock.writeLock().lock();
        try {
            percentLimitFromFile(store != null ? store.getDirectory() : null);
        } finally {
            usageLock.writeLock().unlock();
        }
    }

    public StoreUsage copy() {
        StoreUsage storeUsage = new StoreUsage();
        storeUsage.name = name;
        storeUsage.parent = parent;
        storeUsage.total = total;
        storeUsage.percentLimit = percentLimit;
        storeUsage.getLimiter().setLimit(getLimiter().getLimit());
        return storeUsage;
    }
}
