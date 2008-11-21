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
package org.apache.kahadb.journal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Used as a location in the data store.
 * 
 * @version $Revision$
 */
public final class Location implements Comparable<Location> {

    public static final byte USER_TYPE = 1;
    public static final byte NOT_SET_TYPE = 0;
    public static final int NOT_SET = -1;

    private int dataFileId = NOT_SET;
    private int offset = NOT_SET;
    private int size = NOT_SET;
    private byte type = NOT_SET_TYPE;
    private CountDownLatch latch;

    public Location() {
    }

    public Location(Location item) {
        this.dataFileId = item.dataFileId;
        this.offset = item.offset;
        this.size = item.size;
        this.type = item.type;
    }

    public Location(int dataFileId, int offset) {
        this.dataFileId=dataFileId;
        this.offset=offset;
    }

    boolean isValid() {
        return dataFileId != NOT_SET;
    }

    /**
     * @return the size of the data record including the header.
     */
    public int getSize() {
        return size;
    }

    /**
     * @param size the size of the data record including the header.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @return the size of the payload of the record.
     */
    public int getPaylodSize() {
        return size - Journal.ITEM_HEAD_FOOT_SPACE;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getDataFileId() {
        return dataFileId;
    }

    public void setDataFileId(int file) {
        this.dataFileId = file;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public String toString() {
        return dataFileId+":"+offset;
    }

    public void writeExternal(DataOutput dos) throws IOException {
        dos.writeInt(dataFileId);
        dos.writeInt(offset);
        dos.writeInt(size);
        dos.writeByte(type);
    }

    public void readExternal(DataInput dis) throws IOException {
        dataFileId = dis.readInt();
        offset = dis.readInt();
        size = dis.readInt();
        type = dis.readByte();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public int compareTo(Location o) {
        Location l = (Location)o;
        if (dataFileId == l.dataFileId) {
            int rc = offset - l.offset;
            return rc;
        }
        return dataFileId - l.dataFileId;
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof Location) {
            result = compareTo((Location)o) == 0;
        }
        return result;
    }

    public int hashCode() {
        return dataFileId ^ offset;
    }

}
