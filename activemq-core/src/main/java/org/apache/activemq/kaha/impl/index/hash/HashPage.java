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
package org.apache.activemq.kaha.impl.index.hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.kaha.Marshaller;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Page within a HashPage
 * 
 * @version $Revision: 1.1.1.1 $
 */
class HashPage {
    static final int PAGE_HEADER_SIZE = 17;
    private static final transient Log LOG = LogFactory.getLog(HashPage.class);

    private int maximumEntries;
    private long id;
    private int binId;
    private int persistedSize;
    private List<HashEntry> hashIndexEntries;
    /*
     * for persistence only
     */
    private long nextFreePageId = HashEntry.NOT_SET;
    private boolean active = true;

    /**
     * Constructor
     * 
     * @param hashIndex
     * @param id
     * @param parentId
     * @param maximumEntries
     */
    HashPage(long id, int maximumEntries) {
        this(maximumEntries);

        this.id = id;
    }

    /**
     * Constructor
     * 
     * @param maximumEntries
     */
    public HashPage(int maximumEntries) {
        this.maximumEntries = maximumEntries;
        this.hashIndexEntries = new ArrayList<HashEntry>(maximumEntries);
    }

    public String toString() {
        return "HashPage[" + getId() + ":" + binId + "]";
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof HashPage) {
            HashPage other = (HashPage)o;
            result = other.id == id;
        }
        return result;
    }

    public int hashCode() {
        return (int)id;
    }

    boolean isActive() {
        return this.active;
    }

    void setActive(boolean active) {
        this.active = active;
    }

    long getNextFreePageId() {
        return this.nextFreePageId;
    }

    void setNextFreePageId(long nextPageId) {
        this.nextFreePageId = nextPageId;
    }

    long getId() {
        return id;
    }

    void setId(long id) {
        this.id = id;
    }

    int getPersistedSize() {
        return persistedSize;
    }

    void write(Marshaller keyMarshaller, DataOutput dataOut) throws IOException {
        writeHeader(dataOut);
        dataOut.writeInt(hashIndexEntries.size());
        for (HashEntry entry : hashIndexEntries) {
            entry.write(keyMarshaller, dataOut);
        }
    }

    void read(Marshaller keyMarshaller, DataInput dataIn) throws IOException {
        readHeader(dataIn);
        int size = dataIn.readInt();
        hashIndexEntries.clear();
        for (int i = 0; i < size; i++) {
            HashEntry entry = new HashEntry();
            entry.read(keyMarshaller, dataIn);
            hashIndexEntries.add(entry);
        }
    }

    void readHeader(DataInput dataIn) throws IOException {
        active = dataIn.readBoolean();
        nextFreePageId = dataIn.readLong();
        binId = dataIn.readInt();
        persistedSize = dataIn.readInt();
    }

    void writeHeader(DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(isActive());
        dataOut.writeLong(nextFreePageId);
        dataOut.writeInt(binId);
        dataOut.writeInt(size());
    }

    boolean isEmpty() {
        return hashIndexEntries.isEmpty();
    }

    boolean isFull() {
        return hashIndexEntries.size() >= maximumEntries;
    }

    boolean isUnderflowed() {
        return hashIndexEntries.size() < (maximumEntries / 2);
    }

    boolean isOverflowed() {
        return hashIndexEntries.size() > maximumEntries;
    }

    List<HashEntry> getEntries() {
        return hashIndexEntries;
    }

    void setEntries(List<HashEntry> newEntries) {
        this.hashIndexEntries = newEntries;
    }

    int getMaximumEntries() {
        return this.maximumEntries;
    }

    void setMaximumEntries(int maximumEntries) {
        this.maximumEntries = maximumEntries;
    }

    int size() {
        return hashIndexEntries.size();
    }

    void reset() throws IOException {
        hashIndexEntries.clear();
        setNextFreePageId(HashEntry.NOT_SET);
    }

    void addHashEntry(int index, HashEntry entry) throws IOException {
        // index = index >= 0 ? index : 0;
        // index = (index == 0 || index< size()) ? index : size()-1;
        hashIndexEntries.add(index, entry);
    }

    HashEntry getHashEntry(int index) {
        HashEntry result = hashIndexEntries.get(index);
        return result;
    }

    HashEntry removeHashEntry(int index) throws IOException {
        HashEntry result = hashIndexEntries.remove(index);
        return result;
    }

    void removeAllTreeEntries(List<HashEntry> c) {
        hashIndexEntries.removeAll(c);
    }

    List<HashEntry> getSubList(int from, int to) {
        return new ArrayList<HashEntry>(hashIndexEntries.subList(from, to));
    }

    /**
     * @return the binId
     */
    int getBinId() {
        return this.binId;
    }

    /**
     * @param binId the binId to set
     */
    void setBinId(int binId) {
        this.binId = binId;
    }

    void dump() {

        StringBuffer str = new StringBuffer(32);
        str.append(toString());
        str.append(": ");
        for (HashEntry entry : hashIndexEntries) {
            str.append(entry);
            str.append(",");
        }
        LOG.info(str);
    }
}
