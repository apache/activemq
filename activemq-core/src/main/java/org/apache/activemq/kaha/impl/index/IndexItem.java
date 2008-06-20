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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.DataItem;
import org.apache.activemq.kaha.impl.data.Item;

/**
 * A an Item with a relative position and location to other Items in the Store
 * 
 * @version $Revision: 1.2 $
 */
public class IndexItem implements Item, StoreEntry {

    public static final int INDEX_SIZE = 51;
    public static final int INDEXES_ONLY_SIZE = 19;

    protected long offset = POSITION_NOT_SET;

    // used by linked list
    IndexItem next;
    IndexItem prev;

    private long previousItem = POSITION_NOT_SET;
    private long nextItem = POSITION_NOT_SET;
    private boolean active = true;

    // TODO: consider just using a DataItem for the following fields.
    private long keyOffset = POSITION_NOT_SET;
    private int keyFile = (int)POSITION_NOT_SET;
    private int keySize;

    private long valueOffset = POSITION_NOT_SET;
    private int valueFile = (int)POSITION_NOT_SET;
    private int valueSize;

    /**
     * Default Constructor
     */
    public IndexItem() {
    }

    void reset() {
        previousItem = POSITION_NOT_SET;
        nextItem = POSITION_NOT_SET;
        keyOffset = POSITION_NOT_SET;
        keyFile = (int)POSITION_NOT_SET;
        keySize = 0;
        valueOffset = POSITION_NOT_SET;
        valueFile = (int)POSITION_NOT_SET;
        valueSize = 0;
        active = true;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getKeyDataItem()
     */
    public StoreLocation getKeyDataItem() {
        DataItem result = new DataItem();
        result.setOffset(keyOffset);
        result.setFile(keyFile);
        result.setSize(keySize);
        return result;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getValueDataItem()
     */
    public StoreLocation getValueDataItem() {
        DataItem result = new DataItem();
        result.setOffset(valueOffset);
        result.setFile(valueFile);
        result.setSize(valueSize);
        return result;
    }

    public void setValueData(StoreLocation item) {
        valueOffset = item.getOffset();
        valueFile = item.getFile();
        valueSize = item.getSize();
    }

    public void setKeyData(StoreLocation item) {
        keyOffset = item.getOffset();
        keyFile = item.getFile();
        keySize = item.getSize();
    }

    /**
     * @param dataOut
     * @throws IOException
     */
    public void write(DataOutput dataOut) throws IOException {
        dataOut.writeShort(MAGIC);
        dataOut.writeBoolean(active);
        dataOut.writeLong(previousItem);
        dataOut.writeLong(nextItem);
        dataOut.writeInt(keyFile);
        dataOut.writeLong(keyOffset);
        dataOut.writeInt(keySize);
        dataOut.writeInt(valueFile);
        dataOut.writeLong(valueOffset);
        dataOut.writeInt(valueSize);
    }

    void updateIndexes(DataOutput dataOut) throws IOException {
        dataOut.writeShort(MAGIC);
        dataOut.writeBoolean(active);
        dataOut.writeLong(previousItem);
        dataOut.writeLong(nextItem);
    }

    /**
     * @param dataIn
     * @throws IOException
     */
    public void read(DataInput dataIn) throws IOException {
        if (dataIn.readShort() != MAGIC) {
            throw new BadMagicException();
        }
        active = dataIn.readBoolean();
        previousItem = dataIn.readLong();
        nextItem = dataIn.readLong();
        keyFile = dataIn.readInt();
        keyOffset = dataIn.readLong();
        keySize = dataIn.readInt();
        valueFile = dataIn.readInt();
        valueOffset = dataIn.readLong();
        valueSize = dataIn.readInt();
    }

    void readIndexes(DataInput dataIn) throws IOException {
        if (dataIn.readShort() != MAGIC) {
            throw new BadMagicException();
        }
        active = dataIn.readBoolean();
        previousItem = dataIn.readLong();
        nextItem = dataIn.readLong();
    }

    /**
     * @param newPrevEntry
     */
    public void setPreviousItem(long newPrevEntry) {
        previousItem = newPrevEntry;
    }

    /**
     * @return prev item
     */
    long getPreviousItem() {
        return previousItem;
    }

    /**
     * @param newNextEntry
     */
    public void setNextItem(long newNextEntry) {
        nextItem = newNextEntry;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getNextItem()
     */
    public long getNextItem() {
        return nextItem;
    }

    /**
     * @param newObjectOffset
     */
    void setKeyOffset(long newObjectOffset) {
        keyOffset = newObjectOffset;
    }

    /**
     * @return key offset
     */
    long getKeyOffset() {
        return keyOffset;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getKeyFile()
     */
    public int getKeyFile() {
        return keyFile;
    }

    /**
     * @param keyFile The keyFile to set.
     */
    void setKeyFile(int keyFile) {
        this.keyFile = keyFile;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getValueFile()
     */
    public int getValueFile() {
        return valueFile;
    }

    /**
     * @param valueFile The valueFile to set.
     */
    void setValueFile(int valueFile) {
        this.valueFile = valueFile;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getValueOffset()
     */
    public long getValueOffset() {
        return valueOffset;
    }

    /**
     * @param valueOffset The valueOffset to set.
     */
    public void setValueOffset(long valueOffset) {
        this.valueOffset = valueOffset;
    }

    /**
     * @return Returns the active.
     */
    boolean isActive() {
        return active;
    }

    /**
     * @param active The active to set.
     */
    void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getOffset()
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @param offset The offset to set.
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getKeySize()
     */
    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreEntry#getValueSize()
     */
    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }
    
    void copyIndex(IndexItem other) {
        this.offset=other.offset;
        this.active=other.active;
        this.previousItem=other.previousItem;
        this.nextItem=other.nextItem;
    }

    /**
     * @return print of 'this'
     */
    public String toString() {
        String result = "offset=" + offset + ", key=(" + keyFile + ", " + keyOffset + ", " + keySize + ")" + ", value=(" + valueFile + ", " + valueOffset + ", " + valueSize + ")"
                        + ", previousItem=" + previousItem + ", nextItem=" + nextItem;
        return result;
    }

    public boolean equals(Object obj) {
        boolean result = obj == this;
        if (!result && obj != null && obj instanceof IndexItem) {
            IndexItem other = (IndexItem)obj;
            result = other.offset == this.offset;
        }
        return result;
    }

    public int hashCode() {
        return (int)offset;
    }
}
