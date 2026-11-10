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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.Test;

/**
 * Storage usage values change externally (the store grows without the Usage layer being
 * told), so percent freshness comes from the read-driven bounds check in base
 * Usage.getPercentUsage()/isFull(int) rather than a mutation hook. This test drives an
 * externally-mutated store size and asserts the percent tracks exactly across bucket
 * boundaries, over the limit, back down, and across a runtime limit change. TempUsage and
 * JobSchedulerUsage share the identical base-class read path exercised here.
 */
public class StorageUsagePercentBoundsTest {

    static class SettableSizeAdapter extends MemoryPersistenceAdapter {
        final AtomicLong size = new AtomicLong();

        @Override
        public long size() {
            return size.get();
        }
    }

    @Test
    public void testStoreUsagePercentTracksExternalSizeChanges() throws Exception {
        final SettableSizeAdapter adapter = new SettableSizeAdapter();
        final StoreUsage u = new StoreUsage();
        u.setLimit(1000);
        u.setStore(adapter);
        u.start();
        try {
            assertEquals(0, u.getPercentUsage());
            adapter.size.set(5);                    // 5/1000 -> 0%
            assertEquals(0, u.getPercentUsage());
            adapter.size.set(10);                   // 10/1000 -> 1%
            assertEquals(1, u.getPercentUsage());
            adapter.size.set(500);                  // 50%
            assertEquals(50, u.getPercentUsage());
            assertFalse(u.isFull(90));
            adapter.size.set(999);                  // 99%
            assertEquals(99, u.getPercentUsage());
            assertTrue(u.isFull(90));
            assertFalse(u.isFull(100));
            adapter.size.set(1000);                 // 100%
            assertTrue(u.isFull(100));
            assertEquals(100, u.getPercentUsage());
            adapter.size.set(1050);                 // 105% (over limit)
            assertEquals(105, u.getPercentUsage());
            assertTrue(u.isFull(100));
            adapter.size.set(999);                  // back down
            assertEquals(99, u.getPercentUsage());
            assertFalse(u.isFull(100));

            u.setLimit(2000);                       // 999/2000 -> 49%; bounds must refresh
            assertEquals(49, u.getPercentUsage());
            adapter.size.set(1000);                 // 50%
            assertEquals(50, u.getPercentUsage());

            adapter.size.set(0);
            assertEquals(0, u.getPercentUsage());
            assertFalse(u.isFull(1));
        } finally {
            u.stop();
        }
    }
}
