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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Date;

/**
 * Used to lock a File.
 *
 * @author chirino
 */
public class LockFile {

    private static final boolean DISABLE_FILE_LOCK = Boolean.getBoolean("java.nio.channels.FileLock.broken");
    final private File file;
    private long lastModified;

    private FileLock lock;
    private RandomAccessFile randomAccessLockFile;
    private int lockCounter;
    private final boolean deleteOnUnlock;
    private volatile boolean locked;
    private String lockSystemPropertyName = "";

    private static final Logger LOG = LoggerFactory.getLogger(LockFile.class);

    public LockFile(File file, boolean deleteOnUnlock) {
        this.file = file;
        this.deleteOnUnlock = deleteOnUnlock;
    }

    /**
     * @throws IOException
     */
    synchronized public void lock() throws IOException {
        if (DISABLE_FILE_LOCK) {
            return;
        }

        if (lockCounter > 0) {
            return;
        }

        IOHelper.mkdirs(file.getParentFile());
        synchronized (LockFile.class) {
            lockSystemPropertyName = getVmLockKey();
            if (System.getProperty(lockSystemPropertyName) != null) {
                throw new IOException("File '" + file + "' could not be locked as lock is already held for this jvm. Value: " + System.getProperty(lockSystemPropertyName));
            }
            System.setProperty(lockSystemPropertyName, new Date().toString());
        }
        try {
            if (lock == null) {
                randomAccessLockFile = new RandomAccessFile(file, "rw");
                IOException reason = null;
                try {
                    lock = randomAccessLockFile.getChannel().tryLock(0, Math.max(1, randomAccessLockFile.getChannel().size()), false);
                } catch (OverlappingFileLockException e) {
                    reason = IOExceptionSupport.create("File '" + file + "' could not be locked.", e);
                } catch (IOException ioe) {
                    reason = ioe;
                }
                if (lock != null) {
                    //track lastModified only if we are able to successfully obtain the lock.
                    randomAccessLockFile.writeLong(System.currentTimeMillis());
                    randomAccessLockFile.getChannel().force(true);
                    lastModified = file.lastModified();
                    lockCounter++;
                    System.setProperty(lockSystemPropertyName, new Date().toString());
                    locked = true;
                } else {
                    // new read file for next attempt
                    closeReadFile();
                    if (reason != null) {
                        throw reason;
                    }
                    throw new IOException("File '" + file + "' could not be locked.");
                }

            }
        } finally {
            synchronized (LockFile.class) {
                if (lock == null) {
                    System.getProperties().remove(lockSystemPropertyName);
                }
            }
        }
    }

    /**
     */
    synchronized public void unlock() {
        if (DISABLE_FILE_LOCK) {
            return;
        }

        lockCounter--;
        if (lockCounter != 0) {
            return;
        }

        // release the lock..
        if (lock != null) {
            try {
                lock.release();
            } catch (Throwable ignore) {
            } finally {
                if (lockSystemPropertyName != null) {
                    System.getProperties().remove(lockSystemPropertyName);
                }
                lock = null;
            }
        }
        closeReadFile();

        if (locked && deleteOnUnlock) {
            file.delete();
        }
    }

    private String getVmLockKey() throws IOException {
        return getClass().getName() + ".lock." + file.getCanonicalPath();
    }

    private void closeReadFile() {
        // close the file.
        if (randomAccessLockFile != null) {
            try {
                randomAccessLockFile.close();
            } catch (Throwable ignore) {
            }
            randomAccessLockFile = null;
        }
    }

    /**
     * @return true if the lock file's last modified does not match the locally cached lastModified, false otherwise
     */
    private boolean hasBeenModified() {
        boolean modified = false;

        //Create a new instance of the File object so we can get the most up to date information on the file.
        File localFile = new File(file.getAbsolutePath());

        if (localFile.exists()) {
            if(localFile.lastModified() != lastModified) {
                LOG.info("Lock file " + file.getAbsolutePath() + ", locked at " + new Date(lastModified) + ", has been modified at " + new Date(localFile.lastModified()));
                modified = true;
            }
        }
        else {
            //The lock file is missing
            LOG.info("Lock file " + file.getAbsolutePath() + ", does not exist");
            modified = true;
        }

        return modified;
    }

    public boolean keepAlive() {
        locked = locked && lock != null && lock.isValid() && !hasBeenModified();
        return locked;
    }

}
