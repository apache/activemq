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
package org.apache.kahadb.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * Used to lock a File.
 * 
 * @author chirino
 */
public class LockFile {
    
    private static final boolean DISABLE_FILE_LOCK = "true".equals(System.getProperty("java.nio.channels.FileLock.broken", "false"));
    final private File file;
    
    private FileLock lock;
    private RandomAccessFile readFile;
    private int lockCounter;
    private final boolean deleteOnUnlock;
    
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

        lockCounter++;
        if( lockCounter!=1 ) {
            return;
        }
        
        IOHelper.mkdirs(file.getParentFile());
        readFile = new RandomAccessFile(file, "rw");        
        if (lock == null) {
            try {
                lock = readFile.getChannel().tryLock();
            } catch (OverlappingFileLockException e) {
                throw IOExceptionSupport.create("File '" + file + "' could not be locked.",e);
            }
            if (lock == null) {
                throw new IOException("File '" + file + "' could not be locked.");
            }
        }
    }

    /**
     */
    public void unlock() {
        if (DISABLE_FILE_LOCK) {
            return;
        }
        
        lockCounter--;
        if( lockCounter!=0 ) {
            return;
        }
        
        // release the lock..
        if (lock != null) {
            try {
                lock.release();
            } catch (Throwable ignore) {
            }
            lock = null;
        }
        // close the file.
        if (readFile != null) {
            try {
                readFile.close();
            } catch (Throwable ignore) {
            }
            readFile = null;
        }
        
        if( deleteOnUnlock ) {
            file.delete();
        }
    }

}
