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

import java.io.IOException;

/**
 * A Page within a HashPageInfo
 * 
 * @version $Revision: 1.1.1.1 $
 */
class HashPageInfo {

    private HashIndex hashIndex;
    private long id;
    private int size;
    private HashPage page;
    private boolean dirty;

    HashPageInfo(HashIndex index) {
        this.hashIndex = index;
    }

    /**
     * @return the id
     */
    long getId() {
        return this.id;
    }

    /**
     * @param id the id to set
     */
    void setId(long id) {
        this.id = id;
    }

    /**
     * @return the size
     */
    int size() {
        return this.size;
    }
    
    boolean isEmpty() {
        return size <= 0;
    }

    /**
     * @param size the size to set
     */
    void setSize(int size) {
        this.size = size;
    }

    void addHashEntry(int index, HashEntry entry) throws IOException {
        page.addHashEntry(index, entry);
        size++;
        dirty = true;
    }

    HashEntry getHashEntry(int index) throws IOException {
        return page.getHashEntry(index);
    }

    HashEntry removeHashEntry(int index) throws IOException {
        HashEntry result = page.removeHashEntry(index);
        if (result != null) {
            size--;
            dirty = true;
        }
        return result;
    }

    void dump() {
        page.dump();
    }

    void begin() throws IOException {
        if (page == null) {
            page = hashIndex.lookupPage(id);
        }
    }

    void end() throws IOException {
        if (page != null) {
            if (dirty) {
                hashIndex.writeFullPage(page);
            }
        }
        page = null;
        dirty = false;
    }

    HashPage getPage() {
        return page;
    }

    void setPage(HashPage page) {
        this.page = page;
    }
}
