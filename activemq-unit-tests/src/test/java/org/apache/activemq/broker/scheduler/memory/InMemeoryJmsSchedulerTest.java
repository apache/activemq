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
package org.apache.activemq.broker.scheduler.memory;

import org.apache.activemq.broker.scheduler.JmsSchedulerTest;

/**
 * Test for the In-Memory Scheduler variant.
 */
public class InMemeoryJmsSchedulerTest extends JmsSchedulerTest {

    @Override
    protected boolean isPersistent() {
        return false;
    }

    @Override
    public void testScheduleRestart() throws Exception {
        // No persistence so scheduled jobs don't survive restart.
    }

    @Override
    public void testScheduleFullRecoveryRestart() throws Exception {
        // No persistence so scheduled jobs don't survive restart.
    }

    @Override
    public void testJobSchedulerStoreUsage() throws Exception {
        // No store usage numbers for in-memory store.
    }

    @Override
    public void testUpdatesAppliedToIndexBeforeJournalShouldBeDiscarded() throws Exception {
        // not applicable when non persistent
    }
}
