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
package org.apache.activemq.store.journal.active;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.activemq.store.journal.packet.ByteBufferPacket;
import org.apache.activemq.store.journal.packet.Packet;

/**
 * Control file holds the last known good state of the journal.  It stores the state in 
 * record that is versioned and repeated twice in the file so that a failure in the
 * middle of the write of the first or second record do not not result in an unknown
 * state. 
 * 
 * @version $Revision: 1.1 $
 */
final public class ControlFile {

    /** The File that holds the control data. */
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final ByteBufferPacket controlData;
    
    private final static boolean brokenFileLock = "true".equals(System.getProperty("java.nio.channels.FileLock.broken", "false"));

    private long controlDataVersion=0;
    private FileLock lock;
	private boolean disposed;
    private static Set lockSet;
    private String canonicalPath;

    public ControlFile(File fileName, int controlDataSize) throws IOException {
        canonicalPath = fileName.getCanonicalPath();
        boolean existed = fileName.exists();        
        file = new RandomAccessFile(fileName, "rw");
        channel = file.getChannel();
        controlData = new ByteBufferPacket(ByteBuffer.allocateDirect(controlDataSize));

    }

    /**
     * Locks the control file.
     * @throws IOException 
     */
    public void lock() throws IOException {

        Properties properties = System.getProperties();
        synchronized(properties) {
            String lockKey = "org.apache.activemq.store.journal.active.lockMap:"+canonicalPath;
            if( properties.setProperty(lockKey, "true")!=null ) {
                throw new JournalLockedException("Journal is already opened by this application.");
            }

            if( !brokenFileLock ) {
                lock = channel.tryLock();
                if (lock == null) {
                   properties.remove(lockKey);
                   throw new JournalLockedException("Journal is already opened by another application");
                }
            }
        }
    }

    /**
     * Un locks the control file.
     * 
     * @throws IOException
     */
    public void unlock() throws IOException {

        Properties properties = System.getProperties();
        synchronized(properties) {
            if (lock != null) {
                String lockKey = "org.apache.activemq.store.journal.active.lockMap:"+canonicalPath;
                properties.remove(lockKey);
                lock.release();
                lock = null;
            }
        }
    }
    
    public boolean load() throws IOException {
        long l = file.length();
        if( l < controlData.capacity() ) {
            controlDataVersion=0;
            controlData.position(0);
            controlData.limit(0);
            return false;
        } else {            
            file.seek(0);
            long v1 = file.readLong();
            file.seek(controlData.capacity()+8);
            long v1check = file.readLong();
            
            file.seek(controlData.capacity()+16);
            long v2 = file.readLong();
            file.seek((controlData.capacity()*2)+24);
            long v2check = file.readLong();
            
            if( v2 == v2check ) {
                controlDataVersion = v2;
                file.seek(controlData.capacity()+24);
                controlData.clear();
                channel.read(controlData.getByteBuffer());
            } else if ( v1 == v1check ){
                controlDataVersion = v1;
                file.seek(controlData.capacity()+8);
                controlData.clear();
                channel.read(controlData.getByteBuffer());
            } else {
                // Bummer.. Both checks are screwed. we don't know
                // if any of the two buffer are ok.  This should
                // only happen is data got corrupted.
                throw new IOException("Control data corrupted.");
            }         
            return true;
        }
    }
    
    public void store() throws IOException {
        controlDataVersion++;
        file.setLength((controlData.capacity()*2)+32);
        file.seek(0);
        
        // Write the first copy of the control data.
        file.writeLong(controlDataVersion);
        controlData.clear();
        channel.write(controlData.getByteBuffer());
        file.writeLong(controlDataVersion);

        // Write the second copy of the control data.
        file.writeLong(controlDataVersion);
        controlData.clear();
        channel.write(controlData.getByteBuffer());
        file.writeLong(controlDataVersion);
        
        channel.force(false);        
    }

    public Packet getControlData() {
        controlData.clear();
        return controlData;
    }

    public void dispose() {
    	if( disposed )
    		return;
    	disposed=true;
        try {
            unlock();
        } catch (IOException e) {
        }
        try {
            file.close();
        } catch (IOException e) {
        }
    }
}
