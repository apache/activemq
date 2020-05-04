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

/**
 * A lockable broker resource. Uses {@link Locker} to guarantee that only single instance is running
 *
 */
public interface Lockable {

    /**
     * Turn locking on/off on the resource
     *
     * @param useLock
     */
    public void setUseLock(boolean useLock);

    /**
     * Stop the broker if the locker get an exception while processing lock.
     *
     * @param stopOnError
     */
    public void setStopOnError(boolean stopOnError);

    /**
     * Create a default locker
     *
     * @return default locker
     * @throws IOException
     */
    public Locker createDefaultLocker() throws IOException;

    /**
     * Set locker to be used
     *
     * @param locker
     * @throws IOException
     */
    public void setLocker(Locker locker) throws IOException;

    /**
     * Period (in milliseconds) on which {@link org.apache.activemq.broker.Locker#keepAlive()} should be checked
     *
     * @param lockKeepAlivePeriod
     */
    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod);

    long getLockKeepAlivePeriod();
}
