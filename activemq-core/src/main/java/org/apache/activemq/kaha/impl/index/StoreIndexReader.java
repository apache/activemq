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
package org.apache.activemq.kaha.impl.index;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.activemq.util.DataByteArrayInputStream;

/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
class StoreIndexReader {
    protected RandomAccessFile file;
    protected DataByteArrayInputStream dataIn;
    protected byte[] buffer = new byte[IndexItem.INDEX_SIZE];

    /**
     * Construct a Store reader
     * 
     * @param file
     */
    StoreIndexReader(RandomAccessFile file) {
        this.file = file;
        this.dataIn = new DataByteArrayInputStream();
    }

    protected IndexItem readItem(long offset) throws IOException {
        file.seek(offset);
        file.readFully(buffer);
        dataIn.restart(buffer);
        IndexItem result = new IndexItem();
        result.setOffset(offset);
        result.read(dataIn);
        return result;
    }

    void updateIndexes(IndexItem indexItem) throws IOException {
        if (indexItem != null) {
            file.seek(indexItem.getOffset());
            file.readFully(buffer, 0, IndexItem.INDEXES_ONLY_SIZE);
            dataIn.restart(buffer);
            indexItem.readIndexes(dataIn);
        }
    }
}
