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
package org.apache.activemq.store.jdbc;

import static org.junit.Assert.assertEquals;

import org.apache.activemq.broker.AbstractLocker;
import org.junit.Test;

public class DatabaseLockerConfigTest {

    @Test
    public void testSleepConfig() throws Exception {
        LeaseDatabaseLocker underTest = new LeaseDatabaseLocker();
        underTest.setLockAcquireSleepInterval(50);
        underTest.configure(null);
        assertEquals("configured sleep value retained", 50, underTest.getLockAcquireSleepInterval());
    }

    @Test
    public void testDefaultSleepConfig() throws Exception {
        LeaseDatabaseLocker underTest = new LeaseDatabaseLocker();
        underTest.configure(null);
        assertEquals("configured sleep value retained", AbstractLocker.DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL, underTest.getLockAcquireSleepInterval());
    }

        @Test
    public void testSleepConfigOrig() throws Exception {
        DefaultDatabaseLocker underTest = new DefaultDatabaseLocker();
        underTest.setLockAcquireSleepInterval(50);
        underTest.configure(null);
        assertEquals("configured sleep value retained", 50, underTest.getLockAcquireSleepInterval());
    }

    @Test
    public void testDefaultSleepConfigOrig() throws Exception {
        DefaultDatabaseLocker underTest = new DefaultDatabaseLocker();
        underTest.configure(null);
        assertEquals("configured sleep value retained", AbstractLocker.DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL, underTest.getLockAcquireSleepInterval());
    }
}
