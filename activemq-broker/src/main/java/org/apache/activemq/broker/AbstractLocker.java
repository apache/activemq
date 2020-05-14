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
package org.apache.activemq.broker;

import org.apache.activemq.util.ServiceSupport;

import java.io.IOException;

public abstract class AbstractLocker extends ServiceSupport implements Locker {

    public static final long DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL = 10 * 1000;

    protected String name;
    protected boolean failIfLocked = false;
    protected long lockAcquireSleepInterval = DEFAULT_LOCK_ACQUIRE_SLEEP_INTERVAL;
    protected LockableServiceSupport lockable;

    @Override
    public boolean keepAlive() throws IOException {
        return true;
    }

    @Override
    public void setLockAcquireSleepInterval(long lockAcquireSleepInterval) {
        this.lockAcquireSleepInterval = lockAcquireSleepInterval;
    }

    public long getLockAcquireSleepInterval() {
        return lockAcquireSleepInterval;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setFailIfLocked(boolean failIfLocked) {
        this.failIfLocked = failIfLocked;
    }

    @Override
    public void setLockable(LockableServiceSupport lockableServiceSupport) {
        this.lockable = lockableServiceSupport;
    }

}
