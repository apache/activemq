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
 * Bin in a HashIndex
 * 
 * 
 */
class HashBin {
    private HashIndex hashIndex;
    private int id;
    private int maximumEntries;
    private int size;
    private int numberOfPages =0;
    private HashPageInfo root = null;
    private HashPageInfo tail = null;

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
            HashBin other = (HashBin)o;
            result = other.id == id;
        }
        return result;
    }

    public int hashCode() {
        return (int)getId();
    }

     int  getId() {
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

     HashPageInfo addHashPageInfo(long id, int size) throws IOException {
        HashPageInfo info = new HashPageInfo(hashIndex);
        info.setId(id);
        info.setSize(size);
        if (root == null) {
            root=info;
        }else {
            tail.linkAfter(info);
        }
        tail=info;
        this.numberOfPages++;
        this.size += size;
        return info;
    }

     public HashEntry find(HashEntry key) throws IOException {
        HashEntry result = null;
        try {
            int low = 0;
            int high = size()-1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                HashEntry te = getHashEntry(mid);
                int cmp = te.compareTo(key);
                if (cmp == 0) {
                    result = te;
                    break;
                } else if (cmp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        } finally {
            end();
        }
        return result;
    }

     boolean put(HashEntry newEntry) throws IOException {
        boolean replace = false;
        try {
            int low = 0;
            int high = size()-1;
            while (low <= high) {
                int mid = (low + high) >> 1;
                HashEntry midVal = getHashEntry(mid);
                int cmp = midVal.compareTo(newEntry);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    replace = true;
                    midVal.setIndexOffset(newEntry.getIndexOffset());
                    break;
                }
            }
            if (!replace) {
                addHashEntry(low, newEntry);
                size++;
            }
        } finally {
            end();
        }
        return replace;
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
                } else if (cmp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
        } finally {
            end();
        }
        return result;
    }

    private void addHashEntry(int index, HashEntry entry) throws IOException {
        HashPageInfo pageToUse = null;
        int offset = 0;
        if (index >= getMaximumBinSize()) {
            while(index >= getMaximumBinSize()) {
                HashPage hp = hashIndex.createPage(id);
                pageToUse = addHashPageInfo(hp.getId(), 0);
                pageToUse.setPage(hp);
            }
            offset = 0;
        } else {
            int count = 0;
            int countSoFar=0;
            int pageNo = 0;
            HashPageInfo page = root;
            while (page != null) {
                count += page.size();
                pageToUse=page;
                if (index < count ) {
                    offset = index - countSoFar;
                    break;
                }
                if (index == count && page.size()+1 <= maximumEntries) {
                    offset = page.size();
                    break;
                }
                countSoFar += page.size();
                pageNo++;
                page = (HashPageInfo) page.getNext();
            }
            while(pageNo >= this.numberOfPages) {
                HashPage hp  = hashIndex.createPage(id);
                pageToUse = addHashPageInfo(hp.getId(), 0);               
            }            
        }
        pageToUse.begin();  
        pageToUse.addHashEntry(offset, entry);
        doOverFlow(index);
    }

    private HashEntry removeHashEntry(int index) throws IOException {
        HashPageInfo page = getRetrievePage(index);
        int offset = getRetrieveOffset(index);
        HashEntry result = page.removeHashEntry(offset);
        
        if (page.isEmpty()) {
            if (root.equals(page)) {
                root=(HashPageInfo) root.getNext();
            }
            if (tail.equals(page)) {
                tail=(HashPageInfo) page.getPrevious();
            }
            page.unlink();
            this.numberOfPages--;
            hashIndex.releasePage(page.getPage());
        }
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
    

    private int getMaximumBinSize() {
        return maximumEntries * this.numberOfPages;
    }

    private HashPageInfo getRetrievePage(int index) throws IOException {
        HashPageInfo result = null;
        int count = 0;
        HashPageInfo page = root;
        while (page != null) {
            count += page.size();
            result = page;
            if (index < count) {
                break;
            }
            page = (HashPageInfo) page.getNext();
        }
        
        result.begin();
        return result;
    }

    private int getRetrieveOffset(int index) throws IOException {
        int result = 0;
        int count = 0;
        HashPageInfo page = root;
        while (page != null) {
            if ((index + 1) <= (count + page.size())) {
                result = index - count;
                break;
            }
            count += page.size();
            page = (HashPageInfo) page.getNext();
        }
        return result;
    }

    private void doOverFlow(int index) throws IOException {
        HashPageInfo info = getRetrievePage(index);
        if (info.size() > maximumEntries) {
            // overflowed
            info.begin();
            HashEntry entry = info.removeHashEntry(info.size() - 1);
            doOverFlow(getNextPage(info), entry);
        }
    }

    private void doOverFlow(HashPageInfo next, HashEntry entry) throws IOException {
        HashPageInfo info = null;
        if (next == null) {
            HashPage page = hashIndex.createPage(id);
            info = addHashPageInfo(page.getId(), 0);
            info.setPage(page);
        } else {
            info = next;
        }
        info.begin();
        info.addHashEntry(0, entry);
        if (info.size() > maximumEntries) {
            // overflowed
            HashEntry overflowed = info.removeHashEntry(info.size() - 1);
            doOverFlow(getNextPage(info), overflowed);
        }
    }
    
    private HashPageInfo getNextPage(HashPageInfo start) {
        return (HashPageInfo) start.getNext();
    }

    private void doUnderFlow(int index) {
    }

    String dump() throws IOException {
        String str = "[" + this.numberOfPages+"]";
        HashPageInfo page = root;
        while (page != null) {
            page.begin();
            str +=page.dump();
            page.end();
            page = (HashPageInfo) page.getNext();
        }
        return str;
    }
    private void end() throws IOException {
        HashPageInfo page = root;
        while (page != null) {
            page.end();
            page = (HashPageInfo) page.getNext();
        }
    }
}
