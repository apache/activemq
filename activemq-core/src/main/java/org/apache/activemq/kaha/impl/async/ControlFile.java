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
package org.apache.activemq.kaha.impl.async;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import org.apache.activemq.util.ByteSequence;

/**
 * Use to reliably store fixed sized state data. It stores the state in record
 * that is versioned and repeated twice in the file so that a failure in the
 * middle of the write of the first or second record do not not result in an
 * unknown state.
 * 
 * @version $Revision: 1.1 $
 */
public final class ControlFile {

    private static final boolean DISABLE_FILE_LOCK = "true".equals(System.getProperty("java.nio.channels.FileLock.broken", "false"));
    private final File file;

    /** The File that holds the control data. */
    private final RandomAccessFile randomAccessFile;
    private final int maxRecordSize;

    private long version;
    private FileLock lock;
    private boolean disposed;

    public ControlFile(File file, int recordSize) throws IOException {
        this.file = file;
        this.maxRecordSize = recordSize + 4;
        randomAccessFile = new RandomAccessFile(file, "rw");
    }

    /**
     * Locks the control file.
     * 
     * @throws IOException
     */
    public void lock() throws IOException {
        if (DISABLE_FILE_LOCK) {
            return;
        }

        if (lock == null) {
            lock = randomAccessFile.getChannel().tryLock();
            if (lock == null) {
                throw new IOException("Control file '" + file + "' could not be locked.");
            }
        }
    }

    /**
     * Un locks the control file.
     * 
     * @throws IOException
     */
    public void unlock() throws IOException {
        if (DISABLE_FILE_LOCK) {
            return;
        }

        if (lock != null) {
            lock.release();
            lock = null;
        }
    }

    public void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        try {
            unlock();
        } catch (IOException ignore) {
        }
        try {
            randomAccessFile.close();
        } catch (IOException ignore) {
        }
    }

    public synchronized ByteSequence load() throws IOException {
        long l = randomAccessFile.length();
        if (l < maxRecordSize) {
            return null;
        }

        randomAccessFile.seek(0);
        long v1 = randomAccessFile.readLong();
        randomAccessFile.seek(maxRecordSize + 8);
        long v1check = randomAccessFile.readLong();

        randomAccessFile.seek(maxRecordSize + 16);
        long v2 = randomAccessFile.readLong();
        randomAccessFile.seek((maxRecordSize * 2) + 24);
        long v2check = randomAccessFile.readLong();

        byte[] data = null;
        if (v2 == v2check) {
            version = v2;
            randomAccessFile.seek(maxRecordSize + 24);
            int size = randomAccessFile.readInt();
            data = new byte[size];
            randomAccessFile.readFully(data);
        } else if (v1 == v1check) {
            version = v1;
            randomAccessFile.seek(maxRecordSize + 8);
            int size = randomAccessFile.readInt();
            data = new byte[size];
            randomAccessFile.readFully(data);
        } else {
            // Bummer.. Both checks are screwed. we don't know
            // if any of the two buffer are ok. This should
            // only happen is data got corrupted.
            throw new IOException("Control data corrupted.");
        }
        return new ByteSequence(data, 0, data.length);
    }

    public void store(ByteSequence data, boolean sync) throws IOException {

        version++;
        randomAccessFile.setLength((maxRecordSize * 2) + 32);
        randomAccessFile.seek(0);

        // Write the first copy of the control data.
        randomAccessFile.writeLong(version);
        randomAccessFile.writeInt(data.getLength());
        randomAccessFile.write(data.getData());
        randomAccessFile.writeLong(version);

        // Write the second copy of the control data.
        randomAccessFile.writeLong(version);
        randomAccessFile.writeInt(data.getLength());
        randomAccessFile.write(data.getData());
        randomAccessFile.writeLong(version);

        if (sync) {
            randomAccessFile.getFD().sync();
        }
    }

}
