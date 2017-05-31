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
package org.apache.activemq.store;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.AbstractLocker;
import org.apache.activemq.util.LockFile;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 *
 * @org.apache.xbean.XBean element="shared-file-locker"
 *
 */
public class SharedFileLocker extends AbstractLocker {

    public static final File DEFAULT_DIRECTORY = new File("KahaDB");
    private static final Logger LOG = LoggerFactory.getLogger(SharedFileLocker.class);

    private LockFile lockFile;
    protected File directory = DEFAULT_DIRECTORY;

    @Override
    public void doStart() throws Exception {
        if (lockFile == null) {
            File lockFileName = new File(directory, "lock");
            lockFile = new LockFile(lockFileName, false);
            if (failIfLocked) {
                lockFile.lock();
            } else {
                // Print a warning only once
                boolean warned = false;
                boolean locked = false;
                while ((!isStopped()) && (!isStopping())) {
                    try {
                        lockFile.lock();
                        if (warned) {
                            // ensure lockHolder has released; wait for one keepAlive iteration
                            try {
                                TimeUnit.MILLISECONDS.sleep(lockable != null ? lockable.getLockKeepAlivePeriod() : 0l);
                            } catch (InterruptedException e1) {
                            }
                        }
                        locked = keepAlive();
                        break;
                    } catch (IOException e) {
                        if (!warned)
                        {
                            LOG.info("Database "
                                         + lockFileName
                                         + " is locked by another server. This broker is now in slave mode waiting a lock to be acquired");
                            warned = true;
                        }

                        LOG.debug("Database "
                                    + lockFileName
                                    + " is locked... waiting "
                                    + (lockAcquireSleepInterval / 1000)
                                    + " seconds for the database to be unlocked. Reason: "
                                    + e);
                        try {
                            TimeUnit.MILLISECONDS.sleep(lockAcquireSleepInterval);
                        } catch (InterruptedException e1) {
                        }
                    }
                }
                if (!locked) {
                    throw new IOException("attempt to obtain lock aborted due to shutdown");
                }
            }
        }
    }

    @Override
    public boolean keepAlive() {
        boolean result = lockFile != null && lockFile.keepAlive();
        LOG.trace("keepAlive result: " + result + (name != null ? ", name: " + name : ""));
        return result;
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        if (lockFile != null) {
            lockFile.unlock();
            lockFile = null;
        }
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    @Override
    public void configure(PersistenceAdapter persistenceAdapter) throws IOException {
        this.setDirectory(persistenceAdapter.getDirectory());
        if (name == null) {
            name = getDirectory().toString();
        }
    }
}
