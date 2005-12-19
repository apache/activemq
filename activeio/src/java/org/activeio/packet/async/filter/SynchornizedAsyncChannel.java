/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.activeio.packet.async.filter;

import edu.emory.mathcs.backport.java.util.concurrent.locks.Lock;
import edu.emory.mathcs.backport.java.util.concurrent.locks.ReentrantLock;

import org.activeio.packet.Packet;
import org.activeio.packet.async.AsyncChannel;
import org.activeio.packet.async.FilterAsyncChannel;

import java.io.IOException;

/**
 * Used to synchronize concurrent access to an ASynchChannel.  
 * 
 * Uses a {@see edu.emory.mathcs.backport.java.util.concurrent.Sync} object
 * for write operations.  All other operations such as {@see #stop(long)}
 * and {@see #stop} just do a normal java synchronization against the SynchornizedSynchChannel
 * object instance.  It is assumed that the Async message delivery is not 
 * concurrent and therefore does not require synchronization.
 * 
 */
public class SynchornizedAsyncChannel extends FilterAsyncChannel {

    private final Lock writeLock;

    public SynchornizedAsyncChannel(AsyncChannel next) {
        this(next, new ReentrantLock());
    }
    
    public SynchornizedAsyncChannel(AsyncChannel next, Lock writeLock) {
        super(next);
        this.writeLock = writeLock;
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
    
    public Lock getWriteLock() {
        return writeLock;
    }
}
