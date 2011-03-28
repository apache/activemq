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
package org.apache.kahadb.page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.Marshaller;

/**
 * A Page within a file.
 * 
 * 
 */
public class Page<T> {

    public static final int PAGE_HEADER_SIZE = 21;

    public static final byte PAGE_FREE_TYPE = 0;
    public static final byte PAGE_PART_TYPE = 1;
    public static final byte PAGE_END_TYPE = 2;

    long pageId;

    // The following fields are persisted
    byte type = PAGE_FREE_TYPE;
    long txId;
    // A field reserved to hold checksums..  Not in use (yet)
    int checksum;
    
    // Points to the next page in the chunk stream
    long next;
    T data;

    public Page() {
    }

    public Page(long pageId) {
        this.pageId=pageId;
    }

    public void copy(Page<T> other) {
        this.pageId = other.pageId;
        this.txId = other.txId;
        this.type = other.type;
        this.next = other.next;
        this.data = other.data;
    }

    Page<T> copy() {
        Page<T> rc = new Page<T>();
        rc.copy(this);
        return rc;
    }

    void makeFree(long txId) {
        this.type = Page.PAGE_FREE_TYPE;
        this.txId = txId;
        this.data = null;
        this.next = 0;
    }
    
    public void makePagePart(long next, long txId) {
        this.type = Page.PAGE_PART_TYPE;
        this.next = next;
        this.txId = txId;
    }
    
    public void makePageEnd(long size, long txId) {
        this.type = Page.PAGE_END_TYPE;
        this.next = size;
        this.txId = txId;
    }

    void write(DataOutput os) throws IOException {
        os.writeByte(type);
        os.writeLong(txId);
        os.writeLong(next);
        os.writeInt(checksum);
    }

    void read(DataInput is) throws IOException {
        type = is.readByte();
        txId = is.readLong();
        next = is.readLong();
        checksum = is.readInt();
    }

    public long getPageId() {
        return pageId;
    }

    public long getTxId() {
        return txId;
    }

    public T get() {
        return data;
    }

    public void set(T data) {
        this.data = data;
    }

    public short getType() {
        return type;
    }

    public long getNext() {
        return next;
    }

    public String toString() {
        return "[Page:" + getPageId()+", type: "+type+"]";
    }

    public int getChecksum() {
        return checksum;
    }

    public void setChecksum(int checksum) {
        this.checksum = checksum;
    }


}
