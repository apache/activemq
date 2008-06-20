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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class IndexManager {

    public static final String NAME_PREFIX = "index-";
    private static final Log LOG = LogFactory.getLog(IndexManager.class);
    private final String name;
    private File directory;
    private File file;
    private RandomAccessFile indexFile;
    private StoreIndexReader reader;
    private StoreIndexWriter writer;
    private DataManager redoLog;
    private String mode;
    private long length;
    private IndexItem firstFree;
    private IndexItem lastFree;
    private boolean dirty;
    private final AtomicLong storeSize;
    private int freeSize = 0;

    public IndexManager(File directory, String name, String mode, DataManager redoLog, AtomicLong storeSize) throws IOException {
        this.directory = directory;
        this.name = name;
        this.mode = mode;
        this.redoLog = redoLog;
        this.storeSize=storeSize;
        initialize();
    }

    public synchronized boolean isEmpty() {
        return lastFree == null && length == 0;
    }

    public synchronized IndexItem getIndex(long offset) throws IOException {
        IndexItem result = null;
        if (offset >= 0) {
            result = reader.readItem(offset);
        }
        return result;
    }

    public synchronized IndexItem refreshIndex(IndexItem item) throws IOException {
        reader.updateIndexes(item);
        return item;
    }

    public synchronized void freeIndex(IndexItem item) throws IOException {
        item.reset();
        item.setActive(false);
        if (lastFree == null) {
            firstFree = item;
            lastFree = item;
        } else {
            lastFree.setNextItem(item.getOffset());
            if (lastFree.equals(firstFree)) {
                firstFree=new IndexItem();
                firstFree.copyIndex(lastFree);
                writer.updateIndexes(firstFree);
            }
            writer.updateIndexes(lastFree);
            lastFree=item;
        }
        writer.updateIndexes(item);
        freeSize++;
        dirty = true;
    }

    public synchronized void storeIndex(IndexItem index) throws IOException {
        writer.storeItem(index);
        dirty = true;
    }

    public synchronized void updateIndexes(IndexItem index) throws IOException {
        try {
            writer.updateIndexes(index);
        } catch (Throwable e) {
            LOG.error(name + " error updating indexes ", e);
        }
        dirty = true;
    }

    public synchronized void redo(final RedoStoreIndexItem redo) throws IOException {
        writer.redoStoreItem(redo);
        dirty = true;
    }

    public synchronized IndexItem createNewIndex() throws IOException {
        IndexItem result = getNextFreeIndex();
        if (result == null) {
            // allocate one
            result = new IndexItem();
            result.setOffset(length);
            length += IndexItem.INDEX_SIZE;
            storeSize.addAndGet(IndexItem.INDEX_SIZE);
        }
        return result;
    }

    public synchronized void close() throws IOException {
        if (indexFile != null) {
            indexFile.close();
            indexFile = null;
        }
    }

    public synchronized void force() throws IOException {
        if (indexFile != null && dirty) {
            indexFile.getFD().sync();
            dirty = false;
        }
    }

    public synchronized boolean delete() throws IOException {
        firstFree = null;
        lastFree = null;
        if (indexFile != null) {
            indexFile.close();
            indexFile = null;
        }
        return file.delete();
    }

    private synchronized IndexItem getNextFreeIndex() throws IOException {
        IndexItem result = null;
        if (firstFree != null) {
            if (firstFree.equals(lastFree)) {
                result = firstFree;
                firstFree = null;
                lastFree = null;
            } else {
                result = firstFree;
                firstFree = getIndex(firstFree.getNextItem());
                if (firstFree == null) {
                    lastFree = null;
                }
            }
            result.reset();
            writer.updateIndexes(result);
            freeSize--;
        }
        return result;
    }

    synchronized long getLength() {
        return length;
    }
    
    public final long size() {
        return length;
    }

    public synchronized void setLength(long value) {
        this.length = value;
        storeSize.addAndGet(length);
    }
    
    public synchronized FileLock getLock() throws IOException {
        return indexFile.getChannel().tryLock();
    }


    public String toString() {
        return "IndexManager:(" + NAME_PREFIX + name + ")";
    }

    protected void initialize() throws IOException {
        file = new File(directory, NAME_PREFIX + IOHelper.toFileSystemSafeName(name) );
        IOHelper.mkdirs(file.getParentFile());        
        indexFile = new RandomAccessFile(file, mode);
        reader = new StoreIndexReader(indexFile);
        writer = new StoreIndexWriter(indexFile, name, redoLog);
        long offset = 0;
        while ((offset + IndexItem.INDEX_SIZE) <= indexFile.length()) {
            IndexItem index = reader.readItem(offset);
            if (!index.isActive()) {
                index.reset();
                if (lastFree != null) {
                    lastFree.setNextItem(index.getOffset());
                    updateIndexes(lastFree);
                    lastFree = index;
                } else {
                    lastFree = index;
                    firstFree = index;
                }
               freeSize++;
            }
            offset += IndexItem.INDEX_SIZE;
        }
        length = offset;
        storeSize.addAndGet(length);
    }
}
