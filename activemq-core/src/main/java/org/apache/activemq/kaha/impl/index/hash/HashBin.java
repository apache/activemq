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
package org.apache.activemq.kaha.impl.index.hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Bin in a HashIndex
 *
 * @version $Revision: 1.1.1.1 $
 */
class HashBin {
    private static final transient Log log = LogFactory.getLog(HashBin.class);
    private HashIndex hashIndex;
    private int id;
    private int maximumEntries;
    private int size = 0;
    private List<HashPageInfo> hashPages = new ArrayList<HashPageInfo>();

    /**
     * Constructor
     *
     * @param hashIndex
     * @param id
     * @param maximumEntries
     */
    HashBin(HashIndex hashIndex, int id, int maximumEntries) {
        this.hashIndex = hashIndex;
        this.id = id;
        this.maximumEntries = maximumEntries;
    }

    public String toString() {
        return "HashBin[" + getId() + "]";
    }

    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof HashBin) {
            HashBin other = (HashBin) o;
            result = other.id == id;
        }
        return result;
    }

    public int hashCode() {
        return (int) id;
    }

    int getId() {
        return id;
    }

    void setId(int id) {
        this.id = id;
    }

    boolean isEmpty() {
        return true;
    }

    int getMaximumEntries() {
        return this.maximumEntries;
    }

    void setMaximumEntries(int maximumEntries) {
        this.maximumEntries = maximumEntries;
    }

    int size() {
        return size;
    }

    HashPageInfo addHashPageInfo(long id, int size) {
        HashPageInfo info = new HashPageInfo(hashIndex);
        info.setId(id);
        info.setSize(size);
        hashPages.add(info);
        this.size += size;
        return info;
    }

    public HashEntry find(HashEntry key) throws IOException {
        HashEntry result = null;
        try {
            int low = 0;
            int high = size() - 1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                HashEntry te = getHashEntry(mid);
                int cmp = te.compareTo(key);
                if (cmp == 0) {
                    result = te;
                    break;
                }
                else if (cmp < 0) {
                    low = mid + 1;
                }
                else {
                    high = mid - 1;
                }
            }
        }
        finally {
            end();
        }
        return result;
    }

    void put(HashEntry newEntry) throws IOException {
        try {
            boolean replace = false;
            int low = 0;
            int high = size() - 1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                HashEntry midVal = getHashEntry(mid);
                int cmp = midVal.compareTo(newEntry);
                if (cmp < 0) {
                    low = mid + 1;
                }
                else if (cmp > 0) {
                    high = mid - 1;
                }
                else {
                    replace = true;
                    midVal.setIndexOffset(newEntry.getIndexOffset());
                    break;
                }
            }
            if (!replace) {
                if (low > size()) {
                    log.info("SIZE() " + size() + " low = " + low);
                }
                addHashEntry(low, newEntry);
                size++;
            }
        }
        finally {
            end();
        }
    }

    HashEntry remove(HashEntry entry) throws IOException {
        HashEntry result = null;
        try {
            int low = 0;
            int high = size() - 1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                HashEntry te = getHashEntry(mid);
                int cmp = te.compareTo(entry);
                if (cmp == 0) {
                    result = te;
                    removeHashEntry(mid);
                    size--;
                    break;
                }
                else if (cmp < 0) {
                    low = mid + 1;
                }
                else {
                    high = mid - 1;
                }
            }
        }
        finally {
            end();
        }
        return result;
    }

    private void addHashEntry(int index, HashEntry entry) throws IOException {
        HashPageInfo page = getInsertPage(index);
        int offset = index % maximumEntries;
        page.addHashEntry(offset, entry);
        doOverFlow(index);
    }

    private HashEntry removeHashEntry(int index) throws IOException {
        HashPageInfo page = getRetrievePage(index);
        int offset = getRetrieveOffset(index);
        HashEntry result = page.removeHashEntry(offset);
        doUnderFlow(index);
        return result;
    }

    private HashEntry getHashEntry(int index) throws IOException {
        HashPageInfo page = getRetrievePage(index);
        page.begin();
        int offset = getRetrieveOffset(index);
        HashEntry result = page.getHashEntry(offset);
        return result;
    }

    private int maximumBinSize() {
        return maximumEntries * hashPages.size();
    }

    private HashPageInfo getInsertPage(int index) throws IOException {
        HashPageInfo result = null;
        if (index >= maximumBinSize()) {
            HashPage page = hashIndex.createPage(id);
            result = addHashPageInfo(page.getId(), 0);
            result.setPage(page);
        }
        else {
            int offset = index / maximumEntries;
            result = hashPages.get(offset);
        }
        result.begin();
        return result;
    }

    private HashPageInfo getRetrievePage(int index) throws IOException {
        HashPageInfo result = null;
        int count = 0;
        int pageNo = 0;
        for (HashPageInfo page : hashPages) {
            count += page.size();
            if (index < count) {
                break;
            }
            pageNo++;
        }
        result = hashPages.get(pageNo);
        result.begin();
        return result;
    }

    private int getRetrieveOffset(int index) throws IOException {
        int result = 0;
        int count = 0;
        for (HashPageInfo page : hashPages) {
            if ((index + 1) <= (count + page.size())) {
                // count=count==0?count:count+1;
                result = index - count;
                break;
            }
            count += page.size();
        }
        return result;
    }

    private int getInsertPageNo(int index) {
        int result = index / maximumEntries;
        return result;
    }

    private int getOffset(int index) {
        int result = index % maximumEntries;
        return result;
    }

    private void doOverFlow(int index) throws IOException {
        int pageNo = index / maximumEntries;
        HashPageInfo info = hashPages.get(pageNo);
        if (info.size() > maximumEntries) {
            // overflowed
            HashEntry entry = info.removeHashEntry(info.size() - 1);
            doOverFlow(pageNo + 1, entry);
        }
    }

    private void doOverFlow(int pageNo, HashEntry entry) throws IOException {
        HashPageInfo info = null;
        if (pageNo >= hashPages.size()) {
            HashPage page = hashIndex.createPage(id);
            info = addHashPageInfo(page.getId(), 0);
            info.setPage(page);
        }
        else {
            info = hashPages.get(pageNo);
        }
        info.begin();
        info.addHashEntry(0, entry);
        if (info.size() > maximumEntries) {
            // overflowed
            HashEntry overflowed = info.removeHashEntry(info.size() - 1);
            doOverFlow(pageNo + 1, overflowed);
        }
    }

    private void doUnderFlow(int index) {
        int pageNo = index / maximumEntries;
        int nextPageNo = pageNo + 1;
        if (nextPageNo < hashPages.size()) {
        }
        HashPageInfo info = hashPages.get(pageNo);
    }

    private void end() throws IOException {
        for (HashPageInfo info : hashPages) {
            info.end();
        }
    }
}
