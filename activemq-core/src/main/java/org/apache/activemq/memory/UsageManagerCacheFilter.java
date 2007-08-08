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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple CacheFilter that increases/decreases usage on a UsageManager as
 * objects are added/removed from the Cache.
 * 
 * @version $Revision$
 */
public class UsageManagerCacheFilter extends CacheFilter {

    private final AtomicLong totalUsage = new AtomicLong(0);
    private final UsageManager um;

    public UsageManagerCacheFilter(Cache next, UsageManager um) {
        super(next);
        this.um = um;
    }

    public Object put(Object key, Object value) {
        long usage = getUsageOfAddedObject(value);
        Object rc = super.put(key, value);
        if( rc !=null ) {
            usage -= getUsageOfRemovedObject(rc);
        }
        totalUsage.addAndGet(usage);
        um.increaseUsage(usage);
        return rc;
    }
    
    public Object remove(Object key) {
        Object rc = super.remove(key);
        if( rc !=null ) {
            long usage = getUsageOfRemovedObject(rc);
            totalUsage.addAndGet(-usage);
            um.decreaseUsage(usage);
        }
        return rc;
    }
    
    
    protected long getUsageOfAddedObject(Object value) {
        return 1;
    }
    
    protected long getUsageOfRemovedObject(Object value) {
        return 1;
    }

    public void close() {
        um.decreaseUsage(totalUsage.get());
    }
}
