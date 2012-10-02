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
package org.apache.kahadb.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.Marshaller;
import org.apache.kahadb.util.VariableMarshaller;

/**
 * BTree implementation
 * 
 * 
 */
public class HashIndex<Key,Value> implements Index<Key,Value> {

    public static final int CLOSED_STATE = 1;
    public static final int OPEN_STATE = 2;


    private static final Logger LOG = LoggerFactory.getLogger(HashIndex.class);

    public static final int DEFAULT_BIN_CAPACITY;
    public static final int DEFAULT_MAXIMUM_BIN_CAPACITY;
    public static final int DEFAULT_MINIMUM_BIN_CAPACITY;
    public static final int DEFAULT_LOAD_FACTOR;

    static {
        DEFAULT_BIN_CAPACITY = Integer.parseInt(System.getProperty("defaultBinSize", "1024"));
        DEFAULT_MAXIMUM_BIN_CAPACITY = Integer.parseInt(System.getProperty("maximumCapacity", "16384"));
        DEFAULT_MINIMUM_BIN_CAPACITY = Integer.parseInt(System.getProperty("minimumCapacity", "16"));
        DEFAULT_LOAD_FACTOR = Integer.parseInt(System.getProperty("defaultLoadFactor", "75"));
    }

    private AtomicBoolean loaded = new AtomicBoolean();


    private int increaseThreshold;
    private int decreaseThreshold;

    // Where the bin page array starts at.
    private int maximumBinCapacity = DEFAULT_MAXIMUM_BIN_CAPACITY;
    private int minimumBinCapacity = DEFAULT_MINIMUM_BIN_CAPACITY;



    // Once binsActive/binCapacity reaches the loadFactor, then we need to
    // increase the capacity
    private int loadFactor = DEFAULT_LOAD_FACTOR;

    private PageFile pageFile;
    // This page holds the index metadata.
    private long pageId;

    static class Metadata {
        
        private Page<Metadata> page;
        
        // When the index is initializing or resizing.. state changes so that
        // on failure it can be properly recovered.
        private int state;
        private long binPageId;
        private int binCapacity = DEFAULT_BIN_CAPACITY;
        private int binsActive;
        private int size;

        
        public void read(DataInput is) throws IOException {
            state = is.readInt();
            binPageId = is.readLong();
            binCapacity = is.readInt();
            size = is.readInt();
            binsActive = is.readInt();
        }
        public void write(DataOutput os) throws IOException {
            os.writeInt(state);
            os.writeLong(binPageId);
            os.writeInt(binCapacity);
            os.writeInt(size);
            os.writeInt(binsActive);
        }
        
        static class Marshaller extends VariableMarshaller<Metadata> {
            public Metadata readPayload(DataInput dataIn) throws IOException {
                Metadata rc = new Metadata();
                rc.read(dataIn);
                return rc;
            }

            public void writePayload(Metadata object, DataOutput dataOut) throws IOException {
                object.write(dataOut);
            }
        }
    }
    
    private Metadata metadata = new Metadata();
    
    private Metadata.Marshaller metadataMarshaller = new Metadata.Marshaller();
    private HashBin.Marshaller<Key,Value> hashBinMarshaller = new HashBin.Marshaller<Key,Value>(this);
    private Marshaller<Key> keyMarshaller;
    private Marshaller<Value> valueMarshaller;

    
    /**
     * Constructor
     * 
     * @param directory
     * @param name
     * @param indexManager
     * @param numberOfBins
     * @throws IOException
     */
    public HashIndex(PageFile pageFile, long pageId) throws IOException {
        this.pageFile = pageFile;
        this.pageId = pageId;
    }

    public synchronized void load(Transaction tx) throws IOException {
        if (loaded.compareAndSet(false, true)) {
            final Page<Metadata> metadataPage = tx.load(pageId, metadataMarshaller);
            // Is this a brand new index?
            if (metadataPage.getType() == Page.PAGE_FREE_TYPE) {
                // We need to create the pages for the bins
                Page binPage = tx.allocate(metadata.binCapacity);
                metadata.binPageId = binPage.getPageId();
                metadata.page = metadataPage;
                metadataPage.set(metadata);
                clear(tx);

                // If failure happens now we can continue initializing the
                // the hash bins...
            } else {

                metadata = metadataPage.get();
                metadata.page = metadataPage;
                
                // If we did not have a clean shutdown...
                if (metadata.state == OPEN_STATE ) {
                    // Figure out the size and the # of bins that are
                    // active. Yeah This loads the first page of every bin. :(
                    // We might want to put this in the metadata page, but
                    // then that page would be getting updated on every write.
                    metadata.size = 0;
                    for (int i = 0; i < metadata.binCapacity; i++) {
                        int t = sizeOfBin(tx, i);
                        if (t > 0) {
                            metadata.binsActive++;
                        }
                        metadata.size += t;
                    }
                }
            }

            calcThresholds();

            metadata.state = OPEN_STATE;
            tx.store(metadataPage, metadataMarshaller, true);
            
            LOG.debug("HashIndex loaded. Using "+metadata.binCapacity+" bins starting at page "+metadata.binPageId);
        }
    }

    public synchronized void unload(Transaction tx) throws IOException {
        if (loaded.compareAndSet(true, false)) {
            metadata.state = CLOSED_STATE;
            tx.store(metadata.page, metadataMarshaller, true);
        }
    }

    private int sizeOfBin(Transaction tx, int index) throws IOException {
        return getBin(tx, index).size();
    }

    public synchronized Value get(Transaction tx, Key key) throws IOException {
        assertLoaded();
        return getBin(tx, key).get(key);
    }
    
    public synchronized boolean containsKey(Transaction tx, Key key) throws IOException {
        assertLoaded();
        return getBin(tx, key).containsKey(key);
    }

    synchronized public Value put(Transaction tx, Key key, Value value) throws IOException {
        assertLoaded();
        HashBin<Key,Value> bin = getBin(tx, key);

        int originalSize = bin.size();
        Value result = bin.put(key,value);
        store(tx, bin);

        int newSize = bin.size();

        if (newSize != originalSize) {
            metadata.size++;
            if (newSize == 1) {
                metadata.binsActive++;
            }
        }

        if (metadata.binsActive >= this.increaseThreshold) {
            newSize = Math.min(maximumBinCapacity, metadata.binCapacity*2);
            if(metadata.binCapacity!=newSize) {
                resize(tx, newSize);
            }
        }
        return result;
    }
    
    synchronized public Value remove(Transaction tx, Key key) throws IOException {
        assertLoaded();

        HashBin<Key,Value> bin = getBin(tx, key);
        int originalSize = bin.size();
        Value result = bin.remove(key);
        int newSize = bin.size();
        
        if (newSize != originalSize) {
            store(tx, bin);

            metadata.size--;
            if (newSize == 0) {
                metadata.binsActive--;
            }
        }

        if (metadata.binsActive <= this.decreaseThreshold) {
            newSize = Math.max(minimumBinCapacity, metadata.binCapacity/2);
            if(metadata.binCapacity!=newSize) {
                resize(tx, newSize);
            }
        }
        return result;
    }
    

    public synchronized void clear(Transaction tx) throws IOException {
        assertLoaded();
        for (int i = 0; i < metadata.binCapacity; i++) {
            long pageId = metadata.binPageId + i;
            clearBinAtPage(tx, pageId);
        }
        metadata.size = 0;
        metadata.binsActive = 0;
    }
    
    public Iterator<Entry<Key, Value>> iterator(Transaction tx) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }


    /**
     * @param tx
     * @param pageId
     * @throws IOException
     */
    private void clearBinAtPage(Transaction tx, long pageId) throws IOException {
        Page<HashBin<Key,Value>> page = tx.load(pageId, null);
        HashBin<Key, Value> bin = new HashBin<Key,Value>();
        bin.setPage(page);
        page.set(bin);
        store(tx, bin);
    }

    public String toString() {
        String str = "HashIndex" + System.identityHashCode(this) + ": " + pageFile;
        return str;
    }

    // /////////////////////////////////////////////////////////////////
    // Implementation Methods
    // /////////////////////////////////////////////////////////////////

    private void assertLoaded() throws IllegalStateException {
        if( !loaded.get() ) {
            throw new IllegalStateException("The HashIndex is not loaded");
        }
    }

    public synchronized void store(Transaction tx, HashBin<Key,Value> bin) throws IOException {
        tx.store(bin.getPage(), hashBinMarshaller, true);
    }

    // While resizing, the following contains the new resize data.
    
    private void resize(Transaction tx, final int newSize) throws IOException {
        LOG.debug("Resizing to: "+newSize);
        
        int resizeCapacity = newSize;
        long resizePageId = tx.allocate(resizeCapacity).getPageId();

        // In Phase 1 we copy the data to the new bins..
        // Initialize the bins..
        for (int i = 0; i < resizeCapacity; i++) {
            long pageId = resizePageId + i;
            clearBinAtPage(tx, pageId);
        }

        metadata.binsActive = 0;
        // Copy the data from the old bins to the new bins.
        for (int i = 0; i < metadata.binCapacity; i++) {
            
            HashBin<Key,Value> bin = getBin(tx, i);
            for (Map.Entry<Key, Value> entry : bin.getAll(tx).entrySet()) {
                HashBin<Key,Value> resizeBin = getBin(tx, entry.getKey(), resizePageId, resizeCapacity);
                resizeBin.put(entry.getKey(), entry.getValue());
                store(tx, resizeBin);
                if( resizeBin.size() == 1) {
                    metadata.binsActive++;
                }
            }
        }
        
        // In phase 2 we free the old bins and switch the the new bins.
        tx.free(metadata.binPageId, metadata.binCapacity);
        
        metadata.binCapacity = resizeCapacity;
        metadata.binPageId = resizePageId;
        metadata.state = OPEN_STATE;
        tx.store(metadata.page, metadataMarshaller, true);
        calcThresholds();

        LOG.debug("Resizing done.  New bins start at: "+metadata.binPageId);
        resizeCapacity=0;
        resizePageId=0;
    }

    private void calcThresholds() {
        increaseThreshold = (metadata.binCapacity * loadFactor)/100;
        decreaseThreshold = (metadata.binCapacity * loadFactor * loadFactor ) / 20000;
    }
    
    private HashBin<Key,Value> getBin(Transaction tx, Key key) throws IOException {
        return getBin(tx, key, metadata.binPageId, metadata.binCapacity);
    }

    private HashBin<Key,Value> getBin(Transaction tx, int i) throws IOException {
        return getBin(tx, i, metadata.binPageId);
    }
    
    private HashBin<Key,Value> getBin(Transaction tx, Key key, long basePage, int capacity) throws IOException {
        int i = indexFor(key, capacity);
        return getBin(tx, i, basePage);
    }

    private HashBin<Key,Value> getBin(Transaction tx, int i, long basePage) throws IOException {
        Page<HashBin<Key, Value>> page = tx.load(basePage + i, hashBinMarshaller);
        HashBin<Key, Value> rc = page.get();
        rc.setPage(page);
        return rc;
    }

    int indexFor(Key x, int length) {
        return Math.abs(x.hashCode()%length);
    }

    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////

    public Marshaller<Key> getKeyMarshaller() {
        return keyMarshaller;
    }

    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    public synchronized void setKeyMarshaller(Marshaller<Key> marshaller) {
        this.keyMarshaller = marshaller;
    }

    public Marshaller<Value> getValueMarshaller() {
        return valueMarshaller;
    }
    /**
     * Set the marshaller for value objects
     * 
     * @param marshaller
     */
    public void setValueMarshaller(Marshaller<Value> valueMarshaller) {
        this.valueMarshaller = valueMarshaller;
    }
    
    /**
     * @return number of bins in the index
     */
    public int getBinCapacity() {
        return metadata.binCapacity;
    }

    /**
     * @param binCapacity
     */
    public void setBinCapacity(int binCapacity) {
        if (loaded.get() && binCapacity != metadata.binCapacity) {
            throw new RuntimeException("Pages already loaded - can't reset bin capacity");
        }
        metadata.binCapacity = binCapacity;
    }

    public boolean isTransient() {
        return false;
    }

    /**
     * @return the loadFactor
     */
    public int getLoadFactor() {
        return loadFactor;
    }

    /**
     * @param loadFactor the loadFactor to set
     */
    public void setLoadFactor(int loadFactor) {
        this.loadFactor = loadFactor;
    }

    /**
     * @return the maximumCapacity
     */
    public int setMaximumBinCapacity() {
        return maximumBinCapacity;
    }

    /**
     * @param maximumCapacity the maximumCapacity to set
     */
    public void setMaximumBinCapacity(int maximumCapacity) {
        this.maximumBinCapacity = maximumCapacity;
    }

    public synchronized int size(Transaction tx) {
        return metadata.size;
    }

    public synchronized int getActiveBins() {
        return metadata.binsActive;
    }

    public long getBinPageId() {
        return metadata.binPageId;
    }

    public PageFile getPageFile() {
        return pageFile;
    }

    public int getBinsActive() {
        return metadata.binsActive;
    }

}
