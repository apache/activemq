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

import org.apache.activemq.kaha.StoreLocation;

/**
 * A a wrapper for a data in the store
 * 
 * @version $Revision: 1.2 $
 */
public final class DataItem implements Item, StoreLocation {

    private int file = (int)POSITION_NOT_SET;
    private long offset = POSITION_NOT_SET;
    private int size;

    public DataItem() {
    }

    DataItem(DataItem item) {
        this.file = item.file;
        this.offset = item.offset;
        this.size = item.size;
    }

    boolean isValid() {
        return file != POSITION_NOT_SET;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreLocation#getSize()
     */
    public int getSize() {
        return size;
    }

    /**
     * @param size The size to set.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @return
     * @see org.apache.activemq.kaha.StoreLocation#getOffset()
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
     * @see org.apache.activemq.kaha.StoreLocation#getFile()
     */
    public int getFile() {
        return file;
    }

    /**
     * @param file The file to set.
     */
    public void setFile(int file) {
        this.file = file;
    }

    /**
     * @return a pretty print
     */
    public String toString() {
        String result = "offset = " + offset + ", file = " + file + ", size = " + size;
        return result;
    }

    public DataItem copy() {
        return new DataItem(this);
    }
}
