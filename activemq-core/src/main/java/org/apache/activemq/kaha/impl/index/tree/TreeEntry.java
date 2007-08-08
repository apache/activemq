/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.kaha.impl.index.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;

/**
 * Key and index for a BTree
 * 
 * @version $Revision: 1.1.1.1 $
 */
class TreeEntry implements Comparable {

    static final int NOT_SET = -1;
    private Comparable key;
    private long indexOffset;
    private long prevPageId = NOT_SET;
    private long nextPageId = NOT_SET;

    public int compareTo(Object o) {
        if (o instanceof TreeEntry) {
            TreeEntry other = (TreeEntry)o;
            return key.compareTo(other.key);
        } else {
            return key.compareTo(o);
        }
    }

    public boolean equals(Object o) {
        return compareTo(o) == 0;
    }

    public int hashCode() {
        return key.hashCode();
    }

    public String toString() {
        return "TreeEntry(" + key + "," + indexOffset + ")prev=" + prevPageId + ",next=" + nextPageId;
    }

    void reset() {
        prevPageId = nextPageId = NOT_SET;
    }

    TreeEntry copy() {
        TreeEntry copy = new TreeEntry();
        copy.key = this.key;
        copy.indexOffset = this.indexOffset;
        copy.prevPageId = this.prevPageId;
        copy.nextPageId = this.nextPageId;
        return copy;
    }

    /**
     * @return the key
     */
    Comparable getKey() {
        return this.key;
    }

    /**
     * @param key the key to set
     */
    void setKey(Comparable key) {
        this.key = key;
    }

    /**
     * @return the nextPageId
     */
    long getNextPageId() {
        return this.nextPageId;
    }

    /**
     * @param nextPageId the nextPageId to set
     */
    void setNextPageId(long nextPageId) {
        this.nextPageId = nextPageId;
    }

    /**
     * @return the prevPageId
     */
    long getPrevPageId() {
        return this.prevPageId;
    }

    /**
     * @param prevPageId the prevPageId to set
     */
    void setPrevPageId(long prevPageId) {
        this.prevPageId = prevPageId;
    }

    /**
     * @return the indexOffset
     */
    long getIndexOffset() {
        return this.indexOffset;
    }

    /**
     * @param indexOffset the indexOffset to set
     */
    void setIndexOffset(long indexOffset) {
        this.indexOffset = indexOffset;
    }

    boolean hasChildPagesReferences() {
        return prevPageId != NOT_SET || nextPageId != NOT_SET;
    }

    void write(Marshaller keyMarshaller, DataOutput dataOut) throws IOException {
        keyMarshaller.writePayload(key, dataOut);
        dataOut.writeLong(indexOffset);
        dataOut.writeLong(nextPageId);
        dataOut.writeLong(prevPageId);
    }

    void read(Marshaller keyMarshaller, DataInput dataIn) throws IOException {
        key = (Comparable)keyMarshaller.readPayload(dataIn);
        indexOffset = dataIn.readLong();
        nextPageId = dataIn.readLong();
        prevPageId = dataIn.readLong();
    }

}
