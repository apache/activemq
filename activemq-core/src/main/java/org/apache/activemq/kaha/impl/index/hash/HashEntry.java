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
import org.apache.activemq.kaha.Marshaller;

/**
 * Key and index for DiskBased Hash Index
 * 
 * @version $Revision: 1.1.1.1 $
 */
class HashEntry implements Comparable {

    static final int NOT_SET = -1;
    private Comparable key;
    private long indexOffset;

    public int compareTo(Object o) {
        if (o instanceof HashEntry) {
            HashEntry other = (HashEntry)o;
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
        return "HashEntry(" + key + "," + indexOffset + ")";
    }

    HashEntry copy() {
        HashEntry copy = new HashEntry();
        copy.key = this.key;
        copy.indexOffset = this.indexOffset;
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

    void write(Marshaller keyMarshaller, DataOutput dataOut) throws IOException {
        dataOut.writeLong(indexOffset);
        keyMarshaller.writePayload(key, dataOut);
    }

    void read(Marshaller keyMarshaller, DataInput dataIn) throws IOException {
        indexOffset = dataIn.readLong();
        key = (Comparable)keyMarshaller.readPayload(dataIn);
    }
}
