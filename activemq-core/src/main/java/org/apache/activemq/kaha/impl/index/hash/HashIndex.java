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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.index.Index;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.LRUCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BTree implementation
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class HashIndex implements Index, HashIndexMBean {
    public static final int DEFAULT_PAGE_SIZE;
    public static final int DEFAULT_KEY_SIZE;
    public static final int DEFAULT_BIN_SIZE;
    public static final int MAXIMUM_CAPACITY = 16384;
    /**
     * The load factor used when none specified in constructor.
     **/
    static final float DEFAULT_LOAD_FACTOR;
    private static final String NAME_PREFIX = "hash-index-";
    private static final Log LOG = LogFactory.getLog(HashIndex.class);
    private final String name;
    private File directory;
    private File file;
    private RandomAccessFile indexFile;
    private IndexManager indexManager;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private int keySize = DEFAULT_KEY_SIZE;
    private int numberOfBins = DEFAULT_BIN_SIZE;
    private int keysPerPage = this.pageSize /this.keySize;
    private DataByteArrayInputStream dataIn;
    private DataByteArrayOutputStream dataOut;
    private byte[] readBuffer;
    private HashBin[] bins;
    private Marshaller keyMarshaller;
    private long length;
    private LinkedList<HashPage> freeList = new LinkedList<HashPage>();
    private AtomicBoolean loaded = new AtomicBoolean();
    private LRUCache<Long, HashPage> pageCache;
    private boolean enablePageCaching=false;//this is off by default - see AMQ-1667
    private int pageCacheSize = 10;
    private int size;
    private int activeBins;
    private int threshold;
    private int maximumCapacity=MAXIMUM_CAPACITY;
    private float loadFactor=0.75f;
    
    
    /**
     * Constructor
     * 
     * @param directory
     * @param name
     * @param indexManager
     * @param numberOfBins
     * @throws IOException
     */
    public HashIndex(File directory, String name, IndexManager indexManager) throws IOException {
        this.directory = directory;
        this.name = name;
        this.indexManager = indexManager;
        openIndexFile();
        pageCache = new LRUCache<Long, HashPage>(pageCacheSize, pageCacheSize, 0.75f, true);
    }

    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    public synchronized void setKeyMarshaller(Marshaller marshaller) {
        this.keyMarshaller = marshaller;
    }

    /**
     * @return the keySize
     */
    public synchronized int getKeySize() {
        return this.keySize;
    }

    /**
     * @param keySize the keySize to set
     */
    public synchronized void setKeySize(int keySize) {
        this.keySize = keySize;
        if (loaded.get()) {
            throw new RuntimeException("Pages already loaded - can't reset key size");
        }
    }

    /**
     * @return the pageSize
     */
    public synchronized int getPageSize() {
        return this.pageSize;
    }

    /**
     * @param pageSize the pageSize to set
     */
    public synchronized void setPageSize(int pageSize) {
        if (loaded.get() && pageSize != this.pageSize) {
            throw new RuntimeException("Pages already loaded - can't reset page size");
        }
        this.pageSize = pageSize;
    }
    
    /**
     * @return number of bins
     */
    public int getNumberOfBins() {
        return this.numberOfBins;
    }

    /**
     * @param numberOfBins
     */
    public void setNumberOfBins(int numberOfBins) {
        if (loaded.get() && numberOfBins != this.numberOfBins) {
            throw new RuntimeException("Pages already loaded - can't reset bin size");
        }
        this.numberOfBins = numberOfBins;
    }

    /**
     * @return the enablePageCaching
     */
    public synchronized boolean isEnablePageCaching() {
        return this.enablePageCaching;
    }

    /**
     * @param enablePageCaching the enablePageCaching to set
     */
    public synchronized void setEnablePageCaching(boolean enablePageCaching) {
        this.enablePageCaching = enablePageCaching;
    }

    /**
     * @return the pageCacheSize
     */
    public synchronized int getPageCacheSize() {
        return this.pageCacheSize;
    }

    /**
     * @param pageCacheSize the pageCacheSize to set
     */
    public synchronized void setPageCacheSize(int pageCacheSize) {
        this.pageCacheSize = pageCacheSize;
        pageCache.setMaxCacheSize(pageCacheSize);
    }

    public synchronized boolean isTransient() {
        return false;
    }
    
    /**
     * @return the threshold
     */
    public int getThreshold() {
        return threshold;
    }

    /**
     * @param threshold the threshold to set
     */
    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    /**
     * @return the loadFactor
     */
    public float getLoadFactor() {
        return loadFactor;
    }

    /**
     * @param loadFactor the loadFactor to set
     */
    public void setLoadFactor(float loadFactor) {
        this.loadFactor = loadFactor;
    }
    
    /**
     * @return the maximumCapacity
     */
    public int getMaximumCapacity() {
        return maximumCapacity;
    }

    /**
     * @param maximumCapacity the maximumCapacity to set
     */
    public void setMaximumCapacity(int maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
    }
    
    public synchronized int getSize() {
        return size;
    }
    
    public synchronized int getActiveBins(){
        return activeBins;
    }

    public synchronized void load() {
        if (loaded.compareAndSet(false, true)) {
            int capacity = 1;
            while (capacity < numberOfBins) {
                capacity <<= 1;
            }
            this.bins = new HashBin[capacity];
            threshold = calculateThreashold();
            keysPerPage = pageSize / keySize;
            dataIn = new DataByteArrayInputStream();
            dataOut = new DataByteArrayOutputStream(pageSize);
            readBuffer = new byte[pageSize];
            try {
                openIndexFile();
                if (indexFile.length() > 0) {
                    doCompress();
                }
            } catch (IOException e) {
                LOG.error("Failed to load index ", e);
                throw new RuntimeException(e);
            }
        }
    }    

    public synchronized void unload() throws IOException {
        if (loaded.compareAndSet(true, false)) {
            if (indexFile != null) {
                indexFile.close();
                indexFile = null;
                freeList.clear();
                pageCache.clear();
                bins = new HashBin[bins.length];
            }
        }
    }

    public synchronized void store(Object key, StoreEntry value) throws IOException {
        load();
        HashEntry entry = new HashEntry();
        entry.setKey((Comparable)key);
        entry.setIndexOffset(value.getOffset());
        if (!getBin(key).put(entry)) {
            size++;
        }
        if (size >= threshold) {
            resize(2*bins.length);
        }
    }

    public synchronized StoreEntry get(Object key) throws IOException {
        load();
        HashEntry entry = new HashEntry();
        entry.setKey((Comparable)key);
        HashEntry result = getBin(key).find(entry);
        return result != null ? indexManager.getIndex(result.getIndexOffset()) : null;
    }

    public synchronized StoreEntry remove(Object key) throws IOException {
        load();
        HashEntry entry = new HashEntry();
        entry.setKey((Comparable)key);
        HashEntry result = getBin(key).remove(entry);
        if (result != null) {
            size--;
            return indexManager.getIndex(result.getIndexOffset());
        }
        return null;
    }

    public synchronized boolean containsKey(Object key) throws IOException {
        return get(key) != null;
    }

    public synchronized void clear() throws IOException {
        unload();
        delete();
        openIndexFile();
        load();
    }

    public synchronized void delete() throws IOException {
        unload();
        if (file.exists()) {
            file.delete();
        }
        length = 0;
    }

    HashPage lookupPage(long pageId) throws IOException {
        HashPage result = null;
        if (pageId >= 0) {
            result = getFromCache(pageId);
            if (result == null) {
                result = getFullPage(pageId);
                if (result != null) {
                    if (result.isActive()) {
                        addToCache(result);
                    } else {
                        throw new IllegalStateException("Trying to access an inactive page: " + pageId);
                    }
                }
            }
        }
        return result;
    }

    HashPage createPage(int binId) throws IOException {
        HashPage result = getNextFreePage();
        if (result == null) {  
            // allocate one
            result = new HashPage(keysPerPage);
            result.setId(length);
            result.setBinId(binId);
            writePageHeader(result);
            length += pageSize;
            indexFile.seek(length);
            indexFile.write(HashEntry.NOT_SET);
        }
        addToCache(result);
        return result;
    }

    void releasePage(HashPage page) throws IOException {
        removeFromCache(page);
        page.reset();
        page.setActive(false);
        writePageHeader(page);
        freeList.add(page);
    }

    private HashPage getNextFreePage() throws IOException {
        HashPage result = null;
        if(!freeList.isEmpty()) {
            result = freeList.removeFirst();
            result.setActive(true);
            result.reset();
            writePageHeader(result);
        }
        return result;
    }

    void writeFullPage(HashPage page) throws IOException {
        dataOut.reset();
        page.write(keyMarshaller, dataOut);
        if (dataOut.size() > pageSize) {
            throw new IOException("Page Size overflow: pageSize is " + pageSize + " trying to write " + dataOut.size());
        }
        indexFile.seek(page.getId());
        indexFile.write(dataOut.getData(), 0, dataOut.size());
    }

    void writePageHeader(HashPage page) throws IOException {
        dataOut.reset();
        page.writeHeader(dataOut);
        indexFile.seek(page.getId());
        indexFile.write(dataOut.getData(), 0, HashPage.PAGE_HEADER_SIZE);
    }

    HashPage getFullPage(long id) throws IOException {
        indexFile.seek(id);
        indexFile.readFully(readBuffer, 0, pageSize);
        dataIn.restart(readBuffer);
        HashPage page = new HashPage(keysPerPage);
        page.setId(id);
        page.read(keyMarshaller, dataIn);
        return page;
    }

    HashPage getPageHeader(long id) throws IOException {
        indexFile.seek(id);
        indexFile.readFully(readBuffer, 0, HashPage.PAGE_HEADER_SIZE);
        dataIn.restart(readBuffer);
        HashPage page = new HashPage(keysPerPage);
        page.setId(id);
        page.readHeader(dataIn);
        return page;
    }

    void addToBin(HashPage page) throws IOException {
        int index = page.getBinId();
        if (index >= numberOfBins) {
            HashBin[] newBins = new HashBin[index+1];
            System.arraycopy(this.bins, 0, newBins, 0, this.bins.length);
            this.bins=newBins;
        }
        HashBin bin = getBin(index);
        bin.addHashPageInfo(page.getId(), page.getPersistedSize());
    }

    private HashBin getBin(int index) {
        
        HashBin result = bins[index];
        if (result == null) {
            result = new HashBin(this, index, pageSize / keySize);
            bins[index] = result;
            activeBins++;
        }
        return result;
    }

    private void openIndexFile() throws IOException {
        if (indexFile == null) {
            file = new File(directory, NAME_PREFIX + IOHelper.toFileSystemSafeName(name));
            file.getParentFile().mkdirs();
            indexFile = new RandomAccessFile(file, "rw");
        }
    }
    
    private HashBin getBin(Object key) {
        int hash = hash(key);
        int i = indexFor(hash, bins.length);
        return getBin(i);
    }

    private HashPage getFromCache(long pageId) {
        HashPage result = null;
        if (enablePageCaching) {
            result = pageCache.get(pageId);
        }
        return result;
    }

    private void addToCache(HashPage page) {
        if (enablePageCaching) {
            pageCache.put(page.getId(), page);
        }
    }

    private void removeFromCache(HashPage page) {
        if (enablePageCaching) {
            pageCache.remove(page.getId());
        }
    }
    
    private void doLoad() throws IOException {
        long offset = 0;
        if (loaded.compareAndSet(false, true)) {
            while ((offset + pageSize) <= indexFile.length()) {
                indexFile.seek(offset);
                indexFile.readFully(readBuffer, 0, HashPage.PAGE_HEADER_SIZE);
                dataIn.restart(readBuffer);
                HashPage page = new HashPage(keysPerPage);
                page.setId(offset);
                page.readHeader(dataIn);
                if (!page.isActive()) {
                    page.reset();
                    freeList.add(page);
                } else {
                    addToBin(page);
                    size+=page.size();
                }
                offset += pageSize;
            }
            length=offset;
        }
    }
    
    private void doCompress() throws IOException {
        String backFileName = name + "-COMPRESS";
        HashIndex backIndex = new HashIndex(directory,backFileName,indexManager);
        backIndex.setKeyMarshaller(keyMarshaller);
        backIndex.setKeySize(getKeySize());
        backIndex.setNumberOfBins(getNumberOfBins());
        backIndex.setPageSize(getPageSize());
        backIndex.load();
        File backFile = backIndex.file;
        long offset = 0;
        while ((offset + pageSize) <= indexFile.length()) {
            indexFile.seek(offset);
            HashPage page = getFullPage(offset);
            if (page.isActive()) {
                for (HashEntry entry : page.getEntries()) {
                    backIndex.getBin(entry.getKey()).put(entry);
                    backIndex.size++;
                }
            }
            page=null;
            offset += pageSize;
        }
        backIndex.unload();
      
        unload();
        IOHelper.deleteFile(file);
        IOHelper.copyFile(backFile, file);
        IOHelper.deleteFile(backFile);
        openIndexFile();
        doLoad();
    }
    
    private void resize(int newCapacity) throws IOException {
        if (bins.length == getMaximumCapacity()) {
            threshold = Integer.MAX_VALUE;
            return;
        }
        String backFileName = name + "-REISZE";
        HashIndex backIndex = new HashIndex(directory,backFileName,indexManager);
        backIndex.setKeyMarshaller(keyMarshaller);
        backIndex.setKeySize(getKeySize());
        backIndex.setNumberOfBins(newCapacity);
        backIndex.setPageSize(getPageSize());
        backIndex.load();
        File backFile = backIndex.file;
        long offset = 0;
        while ((offset + pageSize) <= indexFile.length()) {
            indexFile.seek(offset);
            HashPage page = getFullPage(offset);
            if (page.isActive()) {
                for (HashEntry entry : page.getEntries()) {
                    backIndex.getBin(entry.getKey()).put(entry);
                    backIndex.size++;
                }
            }
            page=null;
            offset += pageSize;
        }
        backIndex.unload();
      
        unload();
        IOHelper.deleteFile(file);
        IOHelper.copyFile(backFile, file);
        IOHelper.deleteFile(backFile);
        setNumberOfBins(newCapacity);
        bins = new HashBin[newCapacity];
        threshold = calculateThreashold();
        openIndexFile();
        doLoad();
    }
    
    private int calculateThreashold() {
        return (int)(bins.length * 100 * loadFactor);
    }
    
    
    public String toString() {
        String str = "HashIndex"+System.identityHashCode(this)+": "+file.getName();
        return str;
    }
      

    static int hash(Object x) {
        int h = x.hashCode();
        h += ~(h << 9);
        h ^= h >>> 14;
        h += h << 4;
        h ^= h >>> 10;
        return h;
    }

    static int indexFor(int h, int length) {
        return h & (length - 1);
    }

    static {
        DEFAULT_PAGE_SIZE = Integer.parseInt(System.getProperty("defaultPageSize", "16384"));
        DEFAULT_KEY_SIZE = Integer.parseInt(System.getProperty("defaultKeySize", "96"));
        DEFAULT_BIN_SIZE= Integer.parseInt(System.getProperty("defaultBinSize", "1024"));
        DEFAULT_LOAD_FACTOR=Float.parseFloat(System.getProperty("defaultLoadFactor","1.5f"));
    }
}
