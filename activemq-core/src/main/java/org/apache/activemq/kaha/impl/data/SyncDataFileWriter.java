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
package org.apache.activemq.kaha.impl.data;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.util.DataByteArrayOutputStream;

/**
 * Optimized Store writer. Synchronously marshalls and writes to the data file.
 * Simple but may introduce a bit of contention when put under load.
 * 
 * @version $Revision: 1.1.1.1 $
 */
final public class SyncDataFileWriter {

    private DataByteArrayOutputStream buffer;
    private DataManagerImpl dataManager;

    /**
     * Construct a Store writer
     * 
     * @param fileId
     */
    SyncDataFileWriter(DataManagerImpl fileManager) {
        this.dataManager = fileManager;
        this.buffer = new DataByteArrayOutputStream();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.impl.data.DataFileWriter#storeItem(org.apache.activemq.kaha.Marshaller,
     *      java.lang.Object, byte)
     */
    public synchronized DataItem storeItem(Marshaller marshaller, Object payload, byte type)
        throws IOException {

        // Write the packet our internal buffer.
        buffer.reset();
        buffer.position(DataManagerImpl.ITEM_HEAD_SIZE);
        marshaller.writePayload(payload, buffer);
        int size = buffer.size();
        int payloadSize = size - DataManagerImpl.ITEM_HEAD_SIZE;
        buffer.reset();
        buffer.writeByte(type);
        buffer.writeInt(payloadSize);

        // Find the position where this item will land at.
        DataItem item = new DataItem();
        item.setSize(payloadSize);
        DataFile dataFile = dataManager.findSpaceForData(item);

        // Now splat the buffer to the file.
        dataFile.getRandomAccessFile().seek(item.getOffset());
        dataFile.getRandomAccessFile().write(buffer.getData(), 0, size);
        dataFile.setWriterData(Boolean.TRUE); // Use as dirty marker..

        dataManager.addInterestInFile(dataFile);
        return item;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.impl.data.DataFileWriter#updateItem(org.apache.activemq.kaha.StoreLocation,
     *      org.apache.activemq.kaha.Marshaller, java.lang.Object, byte)
     */
    public synchronized void updateItem(DataItem item, Marshaller marshaller, Object payload, byte type)
        throws IOException {
        // Write the packet our internal buffer.
        buffer.reset();
        buffer.position(DataManagerImpl.ITEM_HEAD_SIZE);
        marshaller.writePayload(payload, buffer);
        int size = buffer.size();
        int payloadSize = size - DataManagerImpl.ITEM_HEAD_SIZE;
        buffer.reset();
        buffer.writeByte(type);
        buffer.writeInt(payloadSize);
        item.setSize(payloadSize);
        DataFile dataFile = dataManager.getDataFile(item);
        RandomAccessFile file = dataFile.getRandomAccessFile();
        file.seek(item.getOffset());
        file.write(buffer.getData(), 0, size);
        dataFile.setWriterData(Boolean.TRUE); // Use as dirty marker..
    }

    public synchronized void force(DataFile dataFile) throws IOException {
        // If our dirty marker was set.. then we need to sync
        if (dataFile.getWriterData() != null && dataFile.isDirty()) {
            dataFile.getRandomAccessFile().getFD().sync();
            dataFile.setWriterData(null);
            dataFile.setDirty(false);
        }
    }

    public void close() throws IOException {
    }
}
