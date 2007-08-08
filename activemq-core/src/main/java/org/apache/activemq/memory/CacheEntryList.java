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

/**
 * Maintains a simple linked list of CacheEntry objects. It is thread safe.
 * 
 * @version $Revision$
 */
public class CacheEntryList {

    // Points at the tail of the CacheEntry list
    public final CacheEntry tail = new CacheEntry(null, null);

    public CacheEntryList() {
        tail.next = tail.previous = tail;
    }

    public void add(CacheEntry ce) {
        addEntryBefore(tail, ce);
    }

    private void addEntryBefore(CacheEntry position, CacheEntry ce) {
        assert ce.key != null && ce.next == null && ce.owner == null;

        synchronized (tail) {
            ce.owner = this;
            ce.next = position;
            ce.previous = position.previous;
            ce.previous.next = ce;
            ce.next.previous = ce;
        }
    }

    public void clear() {
        synchronized (tail) {
            tail.next = tail.previous = tail;
        }
    }

    public CacheEvictor createFIFOCacheEvictor() {
        return new CacheEvictor() {
            public CacheEntry evictCacheEntry() {
                CacheEntry rc;
                synchronized (tail) {
                    rc = tail.next;
                }
                return rc.remove() ? rc : null;
            }
        };
    }

    public CacheEvictor createLIFOCacheEvictor() {
        return new CacheEvictor() {
            public CacheEntry evictCacheEntry() {
                CacheEntry rc;
                synchronized (tail) {
                    rc = tail.previous;
                }
                return rc.remove() ? rc : null;
            }
        };
    }

}
