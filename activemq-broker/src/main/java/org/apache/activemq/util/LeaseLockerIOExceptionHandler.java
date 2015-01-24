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
package org.apache.activemq.util;

import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.SuppressReplyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @org.apache.xbean.XBean
 */
public class LeaseLockerIOExceptionHandler extends DefaultIOExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseLockerIOExceptionHandler.class);

    public LeaseLockerIOExceptionHandler() {
        setIgnoreSQLExceptions(false);
        setStopStartConnectors(true);
    }

    // fail only when we get an authoritative answer from the db w/o exceptions
    @Override
    protected boolean hasLockOwnership() throws IOException {
        boolean hasLock = true;

        if (broker.getPersistenceAdapter() instanceof LockableServiceSupport) {
            Locker locker = ((LockableServiceSupport) broker.getPersistenceAdapter()).getLocker();

            if (locker != null) {
                try {
                    if (!locker.keepAlive()) {
                        hasLock = false;
                    }
                }
                catch (SuppressReplyException ignoreWhileHandlingInProgress) {
                }
                catch (IOException ignored) {
                }

                if (!hasLock) {
                    LOG.warn("Lock keepAlive failed, no longer lock owner with: {}", locker);
                    throw new IOException("Lock keepAlive failed, no longer lock owner with: " + locker);
                }
            }
        }

        return hasLock;
    }
}
