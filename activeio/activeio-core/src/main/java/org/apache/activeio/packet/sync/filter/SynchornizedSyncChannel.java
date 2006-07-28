/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet.sync.filter;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.FilterSyncChannel;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelServer;

import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.locks.Lock;
import edu.emory.mathcs.backport.java.util.concurrent.locks.ReentrantLock;

/**
 * Used to synchronize concurrent access to a SynchChannel.  
 * 
 * Uses two different {@see edu.emory.mathcs.backport.java.util.concurrent.Sync} objects
 * for write and read operations.  All other operations such as {@see #stop(long)}
 * and {@see #stop} just do a normal java synchronization against the SynchornizedSynchChannel
 * object instance.
 * 
 */
public class SynchornizedSyncChannel extends FilterSyncChannel {

    private final Lock readLock;
    private final Lock writeLock;

    public SynchornizedSyncChannel(SyncChannel next) {
        this(next, new ReentrantLock(), new ReentrantLock());
    }
    
    public SynchornizedSyncChannel(SyncChannel next, Lock readLock, Lock writeLock) {
        super(next);
        this.readLock = readLock;
        this.writeLock = writeLock;
    }
    
    public Packet read(long timeout) throws IOException {
        try {            
            
            if( timeout==SyncChannelServer.WAIT_FOREVER_TIMEOUT ) {
                readLock.lock();
            } else {
                long start = System.currentTimeMillis();
                if( !readLock.tryLock(0, TimeUnit.MILLISECONDS) ) {
                    return null;
                }
                // Adjust the resulting timeout down to account for time taken to 
                // get the readLock.
                timeout = Math.max(0, timeout-(System.currentTimeMillis()-start));
            }
            
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());            
        }
        
        try {
            return getNext().read(timeout);            
        } finally {
            readLock.unlock();
        }
    }
    
    public void write(Packet packet) throws IOException {
        writeLock.lock();
        try {
            getNext().write(packet);            
        } finally {
            writeLock.unlock();
        }
    }
    
    public void flush() throws IOException {
        writeLock.lock();
        try {
            getNext().flush();            
        } finally {
            writeLock.unlock();
        }
    }

    synchronized public Object getAdapter(Class target) {
        return super.getAdapter(target);
    }

    synchronized public void start() throws IOException {
        super.start();
    }

    synchronized public void stop() throws IOException {
        super.stop();
    }
    
    public Lock getReadLock() {
        return readLock;
    }
    
    public Lock getWriteLock() {
        return writeLock;
    }
}
