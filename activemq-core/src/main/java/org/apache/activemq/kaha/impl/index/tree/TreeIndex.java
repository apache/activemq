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
package org.apache.activemq.kaha.impl.index.tree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.index.Index;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.LRUCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * BTree implementation
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class TreeIndex implements Index {

    private static final String NAME_PREFIX = "tree-index-";
    private static final int DEFAULT_PAGE_SIZE;
    private static final int DEFAULT_KEY_SIZE;
    private static final Log LOG = LogFactory.getLog(TreeIndex.class);
    private final String name;
    private File directory;
    private File file;
    private RandomAccessFile indexFile;
    private IndexManager indexManager;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private int keySize = DEFAULT_KEY_SIZE;
    private int keysPerPage = pageSize / keySize;
    private TreePage root;
    private LRUCache<Long, TreePage> pageCache;
    private DataByteArrayInputStream dataIn;
    private DataByteArrayOutputStream dataOut;
    private byte[] readBuffer;
    private Marshaller keyMarshaller;
    private long length;
    private TreePage firstFree;
    private TreePage lastFree;
    private AtomicBoolean loaded = new AtomicBoolean();
    private boolean enablePageCaching = true;
    private int pageCacheSize = 10;

    /**
     * Constructor
     * 
     * @param directory
     * @param name
     * @param indexManager
     * @throws IOException
     */
    public TreeIndex(File directory, String name, IndexManager indexManager) throws IOException {
        this.directory = directory;
        this.name = name;
        this.indexManager = indexManager;
        pageCache = new LRUCache<Long, TreePage>(pageCacheSize, pageCacheSize, 0.75f, true);
        openIndexFile();
    }

    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    public void setKeyMarshaller(Marshaller marshaller) {
        if (loaded.get()) {
            throw new RuntimeException("Pages already loaded - can't set marshaller now");
        }
        this.keyMarshaller = marshaller;
    }

    /**
     * @return the keySize
     */
    public int getKeySize() {
        return this.keySize;
    }

    /**
     * @param keySize the keySize to set
     */
    public void setKeySize(int keySize) {
        this.keySize = keySize;
        if (loaded.get()) {
            throw new RuntimeException("Pages already loaded - can't reset key size");
        }
    }

    /**
     * @return the pageSize
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * @param pageSize the pageSize to set
     */
    public void setPageSize(int pageSize) {
        if (loaded.get() && pageSize != this.pageSize) {
            throw new RuntimeException("Pages already loaded - can't reset page size");
        }
        this.pageSize = pageSize;
    }

    public boolean isTransient() {
        return false;
    }

    /**
     * @return the enablePageCaching
     */
    public boolean isEnablePageCaching() {
        return this.enablePageCaching;
    }

    /**
     * @param enablePageCaching the enablePageCaching to set
     */
    public void setEnablePageCaching(boolean enablePageCaching) {
        this.enablePageCaching = enablePageCaching;
    }

    /**
     * @return the pageCacheSize
     */
    public int getPageCacheSize() {
        return this.pageCacheSize;
    }

    /**
     * @param pageCacheSize the pageCacheSize to set
     */
    public void setPageCacheSize(int pageCacheSize) {
        this.pageCacheSize = pageCacheSize;
        pageCache.setMaxCacheSize(pageCacheSize);
    }

    public void load() {
        if (loaded.compareAndSet(false, true)) {
            keysPerPage = pageSize / keySize;
            dataIn = new DataByteArrayInputStream();
            dataOut = new DataByteArrayOutputStream(pageSize);
            readBuffer = new byte[pageSize];
            try {
                openIndexFile();
                long offset = 0;
                while ((offset + pageSize) <= indexFile.length()) {
                    indexFile.seek(offset);
                    indexFile.readFully(readBuffer, 0, TreePage.PAGE_HEADER_SIZE);
                    dataIn.restart(readBuffer);
                    TreePage page = new TreePage(keysPerPage);
                    page.setTree(this);
                    page.setId(offset);
                    page.readHeader(dataIn);
                    if (!page.isActive()) {
                        if (lastFree != null) {
                            lastFree.setNextFreePageId(offset);
                            indexFile.seek(lastFree.getId());
                            dataOut.reset();
                            lastFree.writeHeader(dataOut);
                            indexFile.write(dataOut.getData(), 0, TreePage.PAGE_HEADER_SIZE);
                            lastFree = page;
                        } else {
                            lastFree = firstFree = page;
                        }
                    } else if (root == null && page.isRoot()) {
                        root = getFullPage(offset);
                    }
                    offset += pageSize;
                }
                length = offset;
                if (root == null) {
                    root = createRoot();
                }
            } catch (IOException e) {
                LOG.error("Failed to load index ", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void unload() throws IOException {
        if (loaded.compareAndSet(true, false)) {
            if (indexFile != null) {
                indexFile.close();
                indexFile = null;
                pageCache.clear();
                root = null;
                firstFree = lastFree = null;
            }
        }
    }

    public void store(Object key, StoreEntry value) throws IOException {
        TreeEntry entry = new TreeEntry();
        entry.setKey((Comparable)key);
        entry.setIndexOffset(value.getOffset());
        root.put(entry);
    }

    public StoreEntry get(Object key) throws IOException {
        TreeEntry entry = new TreeEntry();
        entry.setKey((Comparable)key);
        TreeEntry result = root.find(entry);
        return result != null ? indexManager.getIndex(result.getIndexOffset()) : null;
    }

    public StoreEntry remove(Object key) throws IOException {
        TreeEntry entry = new TreeEntry();
        entry.setKey((Comparable)key);
        TreeEntry result = root.remove(entry);
        return result != null ? indexManager.getIndex(result.getIndexOffset()) : null;
    }

    public boolean containsKey(Object key) throws IOException {
        TreeEntry entry = new TreeEntry();
        entry.setKey((Comparable)key);
        return root.find(entry) != null;
    }

    public void clear() throws IOException {
        unload();
        delete();
        openIndexFile();
        load();
    }

    public void delete() throws IOException {
        unload();
        if (file.exists()) {
            boolean result = file.delete();
        }
        length = 0;
    }

    /**
     * @return the root
     */
    TreePage getRoot() {
        return this.root;
    }

    TreePage lookupPage(long pageId) throws IOException {
        TreePage result = null;
        if (pageId >= 0) {
            if (root != null && root.getId() == pageId) {
                result = root;
            } else {
                result = getFromCache(pageId);
            }
            if (result == null) {
                result = getFullPage(pageId);
                if (result != null) {
                    if (result.isActive()) {
                        addToCache(result);
                    } else {
                        throw new IllegalStateException("Trying to access an inactive page: " + pageId + " root is " + root);
                    }
                }
            }
        }
        return result;
    }

    TreePage createRoot() throws IOException {
        TreePage result = createPage(-1);
        root = result;
        return result;
    }

    TreePage createPage(long parentId) throws IOException {
        TreePage result = getNextFreePage();
        if (result == null) {
            // allocate one
            result = new TreePage(keysPerPage);
            result.setId(length);
            result.setTree(this);
            result.setParentId(parentId);
            writePage(result);
            length += pageSize;
            indexFile.seek(length);
            indexFile.write(TreeEntry.NOT_SET);
        }
        addToCache(result);
        return result;
    }

    void releasePage(TreePage page) throws IOException {
        removeFromCache(page);
        page.reset();
        page.setActive(false);
        if (lastFree == null) {
            firstFree = lastFree = page;
        } else {
            lastFree.setNextFreePageId(page.getId());
            writePage(lastFree);
        }
        writePage(page);
    }

    private TreePage getNextFreePage() throws IOException {
        TreePage result = null;
        if (firstFree != null) {
            if (firstFree.equals(lastFree)) {
                result = firstFree;
                firstFree = lastFree = null;
            } else {
                result = firstFree;
                firstFree = getPage(firstFree.getNextFreePageId());
                if (firstFree == null) {
                    lastFree = null;
                }
            }
            result.setActive(true);
            result.reset();
            result.saveHeader();
        }
        return result;
    }

    void writeFullPage(TreePage page) throws IOException {
        dataOut.reset();
        page.write(keyMarshaller, dataOut);
        if (dataOut.size() > pageSize) {
            throw new IOException("Page Size overflow: pageSize is " + pageSize + " trying to write " + dataOut.size());
        }
        indexFile.seek(page.getId());
        indexFile.write(dataOut.getData(), 0, dataOut.size());
    }

    void writePage(TreePage page) throws IOException {
        dataOut.reset();
        page.writeHeader(dataOut);
        indexFile.seek(page.getId());
        indexFile.write(dataOut.getData(), 0, TreePage.PAGE_HEADER_SIZE);
    }

    TreePage getFullPage(long id) throws IOException {
        indexFile.seek(id);
        indexFile.readFully(readBuffer, 0, pageSize);
        dataIn.restart(readBuffer);
        TreePage page = new TreePage(keysPerPage);
        page.setId(id);
        page.setTree(this);
        page.read(keyMarshaller, dataIn);
        return page;
    }

    TreePage getPage(long id) throws IOException {
        indexFile.seek(id);
        indexFile.readFully(readBuffer, 0, TreePage.PAGE_HEADER_SIZE);
        dataIn.restart(readBuffer);
        TreePage page = new TreePage(keysPerPage);
        page.setId(id);
        page.setTree(this);
        page.readHeader(dataIn);
        return page;
    }

    private TreePage getFromCache(long pageId) {
        TreePage result = null;
        if (enablePageCaching) {
            result = pageCache.get(pageId);
        }
        return result;
    }

    private void addToCache(TreePage page) {
        if (enablePageCaching) {
            pageCache.put(page.getId(), page);
        }
    }

    private void removeFromCache(TreePage page) {
        if (enablePageCaching) {
            pageCache.remove(page.getId());
        }
    }

    protected void openIndexFile() throws IOException {
        if (indexFile == null) {
            file = new File(directory, NAME_PREFIX + name);
            indexFile = new RandomAccessFile(file, "rw");
        }
    }

    static {
        DEFAULT_PAGE_SIZE = Integer.parseInt(System.getProperty("defaultPageSize", "16384"));
        DEFAULT_KEY_SIZE = Integer.parseInt(System.getProperty("defaultKeySize", "96"));
    }
}
