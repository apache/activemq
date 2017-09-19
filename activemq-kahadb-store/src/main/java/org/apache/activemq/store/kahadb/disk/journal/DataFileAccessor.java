/*
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
package org.apache.activemq.store.kahadb.disk.journal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimized Store reader and updater. Single threaded and synchronous. Use in
 * conjunction with the DataFileAccessorPool of concurrent use.
 */
final class DataFileAccessor {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileAccessor.class);
    private final DataFile dataFile;
    private final Map<Journal.WriteKey, Journal.WriteCommand> inflightWrites;
    private final RecoverableRandomAccessFile file;
    private boolean disposed;

    /**
     * Construct a Store reader
     *
     * @throws IOException
     */
    public DataFileAccessor(Journal dataManager, DataFile dataFile) throws IOException {
        this.dataFile = dataFile;
        this.inflightWrites = dataManager.getInflightWrites();
        this.file = dataFile.openRandomAccessFile();
    }

    public DataFile getDataFile() {
        return dataFile;
    }

    public void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        try {
            dataFile.closeRandomAccessFile(file);
        } catch (IOException e) {
            LOG.warn("Failed to close file", e);
        }
    }

    public ByteSequence readRecord(Location location) throws IOException {

        if (!location.isValid()) {
            throw new IOException("Invalid location: " + location);
        }

        Journal.WriteCommand asyncWrite = inflightWrites.get(new Journal.WriteKey(location));
        if (asyncWrite != null) {
            return asyncWrite.data;
        }

        try {

            if (location.getSize() == Location.NOT_SET) {
                file.seek(location.getOffset());
                location.setSize(file.readInt());
                location.setType(file.readByte());
            } else {
                file.seek(location.getOffset() + Journal.RECORD_HEAD_SPACE);
            }
            if ((long)location.getOffset() + location.getSize() > dataFile.length) {
                throw new IOException("Invalid location size: " + location + ", size: " + location.getSize());
            }
            byte[] data = new byte[location.getSize() - Journal.RECORD_HEAD_SPACE];
            file.readFully(data);
            return new ByteSequence(data, 0, data.length);

        } catch (RuntimeException e) {
            throw new IOException("Invalid location: " + location + " : " + e, e);
        }
    }

    public void readFully(long offset, byte data[]) throws IOException {
       file.seek(offset);
       file.readFully(data);
    }

    public int read(long offset, byte data[]) throws IOException {
       file.seek(offset);
       return file.read(data);
    }

    public void readLocationDetails(Location location) throws IOException {
        Journal.WriteCommand asyncWrite = inflightWrites.get(new Journal.WriteKey(location));
        if (asyncWrite != null) {
            location.setSize(asyncWrite.location.getSize());
            location.setType(asyncWrite.location.getType());
        } else {
            file.seek(location.getOffset());
            location.setSize(file.readInt());
            location.setType(file.readByte());
        }
    }


    public void updateRecord(Location location, ByteSequence data, boolean sync) throws IOException {

        file.seek(location.getOffset() + Journal.RECORD_HEAD_SPACE);
        int size = Math.min(data.getLength(), location.getSize());
        file.write(data.getData(), data.getOffset(), size);
        if (sync) {
            file.sync();
        }
    }

    public RecoverableRandomAccessFile getRaf() {
        return file;
    }
}
