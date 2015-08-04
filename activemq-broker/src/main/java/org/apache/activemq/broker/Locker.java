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

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.store.PersistenceAdapter;

/**
 * Represents a lock service to ensure that a broker is the only master
 */
public interface Locker extends Service {

    /**
     * Used by a timer to keep alive the lock.
     * If the method returns false the broker should be terminated
     * if an exception is thrown, the lock state cannot be determined
     */
    boolean keepAlive() throws IOException;

    /**
     * set the delay interval in milliseconds between lock acquire attempts
     *
     * @param lockAcquireSleepInterval the sleep interval in miliseconds
     */
    void setLockAcquireSleepInterval(long lockAcquireSleepInterval);

    /**
     * Set the name of the lock to use.
     */
    public void setName(String name);

    /**
     * Specify whether to fail immediately if the lock is already held.  When set, the CustomLock must throw an
     * IOException immediately upon detecting the lock is already held.
     *
     * @param failIfLocked: true => fail immediately if the lock is held; false => block until the lock can be obtained
     *                      (default).
     */
    public void setFailIfLocked(boolean failIfLocked);

    /**
     * A reference to what is locked
     */
    public void setLockable(LockableServiceSupport lockable);

    /**
     * Optionally configure the locker with the persistence adapter currently used
     * You can use persistence adapter configuration details like, data directory
     * datasource, etc. to be used by the locker
     *
     * @param persistenceAdapter
     * @throws IOException
     */
    public void configure(PersistenceAdapter persistenceAdapter) throws IOException;

}
