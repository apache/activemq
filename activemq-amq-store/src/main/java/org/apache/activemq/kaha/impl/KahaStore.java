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
package org.apache.activemq.kaha.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.async.AsyncDataManager;
import org.apache.activemq.kaha.impl.async.DataManagerFacade;
import org.apache.activemq.kaha.impl.container.ListContainerImpl;
import org.apache.activemq.kaha.impl.container.MapContainerImpl;
import org.apache.activemq.kaha.impl.data.DataManagerImpl;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.data.RedoListener;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.kaha.impl.index.RedoStoreIndexItem;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store Implementation
 * 
 * 
 */
public class KahaStore implements Store {

    private static final String PROPERTY_PREFIX = "org.apache.activemq.kaha.Store";
    private static final boolean BROKEN_FILE_LOCK = "true".equals(System.getProperty(PROPERTY_PREFIX
                                                                                     + ".FileLockBroken",
                                                                                     "false"));
    private static final boolean DISABLE_LOCKING = "true".equals(System.getProperty(PROPERTY_PREFIX
                                                                                    + ".DisableLocking",
                                                                                    "false"));
    //according to the String javadoc, all constant strings are interned so this will be the same object throughout the vm
    //and we can use it as a monitor for the lockset.
    private final static String LOCKSET_MONITOR = PROPERTY_PREFIX + ".Lock.Monitor";
    private static final Logger LOG = LoggerFactory.getLogger(KahaStore.class);

    private final File directory;
    private final String mode;
    private IndexRootContainer mapsContainer;
    private IndexRootContainer listsContainer;
    private final Map<ContainerId, ListContainerImpl> lists = new ConcurrentHashMap<ContainerId, ListContainerImpl>();
    private final Map<ContainerId, MapContainerImpl> maps = new ConcurrentHashMap<ContainerId, MapContainerImpl>();
    private final Map<String, DataManager> dataManagers = new ConcurrentHashMap<String, DataManager>();
    private final Map<String, IndexManager> indexManagers = new ConcurrentHashMap<String, IndexManager>();
    private boolean closed;
    private boolean initialized;
    private boolean logIndexChanges;
    private boolean useAsyncDataManager;
    private long maxDataFileLength = 1024 * 1024 * 32;
    private FileLock lock;
    private boolean persistentIndex = true;
    private RandomAccessFile lockFile;
    private final AtomicLong storeSize;
    private String defaultContainerName = DEFAULT_CONTAINER_NAME;

    
    public KahaStore(String name, String mode) throws IOException {
    	this(new File(IOHelper.toFileSystemDirectorySafeName(name)), mode, new AtomicLong());
    }

    public KahaStore(File directory, String mode) throws IOException {
    	this(directory, mode, new AtomicLong());
    }

    public KahaStore(String name, String mode,AtomicLong storeSize) throws IOException {
    	this(new File(IOHelper.toFileSystemDirectorySafeName(name)), mode, storeSize);
    }
    
    public KahaStore(File directory, String mode, AtomicLong storeSize) throws IOException {
        this.mode = mode;
        this.storeSize = storeSize;
        this.directory = directory;
        IOHelper.mkdirs(this.directory);
    }

    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            if (initialized) {
                unlock();
                for (ListContainerImpl container : lists.values()) {
                    container.close();
                }
                lists.clear();
                for (MapContainerImpl container : maps.values()) {
                    container.close();
                }
                maps.clear();
                for (Iterator<IndexManager> iter = indexManagers.values().iterator(); iter.hasNext();) {
                    IndexManager im = iter.next();
                    im.close();
                    iter.remove();
                }
                for (Iterator<DataManager> iter = dataManagers.values().iterator(); iter.hasNext();) {
                    DataManager dm = iter.next();
                    dm.close();
                    iter.remove();
                }
            }
            if (lockFile!=null) {
                lockFile.close();
                lockFile=null;
            }
        }
    }

    public synchronized void force() throws IOException {
        if (initialized) {
            for (Iterator<IndexManager> iter = indexManagers.values().iterator(); iter.hasNext();) {
                IndexManager im = iter.next();
                im.force();
            }
            for (Iterator<DataManager> iter = dataManagers.values().iterator(); iter.hasNext();) {
                DataManager dm = iter.next();
                dm.force();
            }
        }
    }

    public synchronized void clear() throws IOException {
        initialize();
        for (Iterator i = mapsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            MapContainer container = getMapContainer(id.getKey(), id.getDataContainerName());
            container.clear();
        }
        for (Iterator i = listsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            ListContainer container = getListContainer(id.getKey(), id.getDataContainerName());
            container.clear();
        }

    }

    public synchronized boolean delete() throws IOException {
        boolean result = true;
        if (initialized) {
            clear();
            for (Iterator<IndexManager> iter = indexManagers.values().iterator(); iter.hasNext();) {
                IndexManager im = iter.next();
                result &= im.delete();
                iter.remove();
            }
            for (Iterator<DataManager> iter = dataManagers.values().iterator(); iter.hasNext();) {
                DataManager dm = iter.next();
                result &= dm.delete();
                iter.remove();
            }
        }
        if (directory != null && directory.isDirectory()) {
            result =IOHelper.deleteChildren(directory);
            String str = result ? "successfully deleted" : "failed to delete";
            LOG.info("Kaha Store " + str + " data directory " + directory);
        }
        return result;
    }

    public synchronized boolean isInitialized() {
        return initialized;
    }

    public boolean doesMapContainerExist(Object id) throws IOException {
        return doesMapContainerExist(id, defaultContainerName);
    }

    public synchronized boolean doesMapContainerExist(Object id, String containerName) throws IOException {
        initialize();
        ContainerId containerId = new ContainerId(id, containerName);
        return maps.containsKey(containerId) || mapsContainer.doesRootExist(containerId);
    }

    public MapContainer getMapContainer(Object id) throws IOException {
        return getMapContainer(id, defaultContainerName);
    }

    public MapContainer getMapContainer(Object id, String containerName) throws IOException {
        return getMapContainer(id, containerName, persistentIndex);
    }

    public synchronized MapContainer getMapContainer(Object id, String containerName, boolean persistentIndex)
        throws IOException {
        initialize();
        ContainerId containerId = new ContainerId(id, containerName);
        MapContainerImpl result = maps.get(containerId);
        if (result == null) {
            DataManager dm = getDataManager(containerName);
            IndexManager im = getIndexManager(dm, containerName);

            IndexItem root = mapsContainer.getRoot(im, containerId);
            if (root == null) {
                root = mapsContainer.addRoot(im, containerId);
            }
            result = new MapContainerImpl(directory, containerId, root, im, dm, persistentIndex);
            maps.put(containerId, result);
        }
        return result;
    }

    public void deleteMapContainer(Object id) throws IOException {
        deleteMapContainer(id, defaultContainerName);
    }

    public void deleteMapContainer(Object id, String containerName) throws IOException {
        ContainerId containerId = new ContainerId(id, containerName);
        deleteMapContainer(containerId);
    }

    public synchronized void deleteMapContainer(ContainerId containerId) throws IOException {
        initialize();
        MapContainerImpl container = maps.remove(containerId);
        if (container != null) {
            container.clear();
            mapsContainer.removeRoot(container.getIndexManager(), containerId);
            container.close();
        }
    }

    public synchronized Set<ContainerId> getMapContainerIds() throws IOException {
        initialize();
        Set<ContainerId> set = new HashSet<ContainerId>();
        for (Iterator i = mapsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            set.add(id);
        }
        return set;
    }

    public boolean doesListContainerExist(Object id) throws IOException {
        return doesListContainerExist(id, defaultContainerName);
    }

    public synchronized boolean doesListContainerExist(Object id, String containerName) throws IOException {
        initialize();
        ContainerId containerId = new ContainerId(id, containerName);
        return lists.containsKey(containerId) || listsContainer.doesRootExist(containerId);
    }

    public ListContainer getListContainer(Object id) throws IOException {
        return getListContainer(id, defaultContainerName);
    }

    public ListContainer getListContainer(Object id, String containerName) throws IOException {
        return getListContainer(id, containerName, persistentIndex);
    }

    public synchronized ListContainer getListContainer(Object id, String containerName,
                                                       boolean persistentIndex) throws IOException {
        initialize();
        ContainerId containerId = new ContainerId(id, containerName);
        ListContainerImpl result = lists.get(containerId);
        if (result == null) {
            DataManager dm = getDataManager(containerName);
            IndexManager im = getIndexManager(dm, containerName);

            IndexItem root = listsContainer.getRoot(im, containerId);
            if (root == null) {
                root = listsContainer.addRoot(im, containerId);
            }
            result = new ListContainerImpl(containerId, root, im, dm, persistentIndex);
            lists.put(containerId, result);
        }
        return result;
    }

    public void deleteListContainer(Object id) throws IOException {
        deleteListContainer(id, defaultContainerName);
    }

    public synchronized void deleteListContainer(Object id, String containerName) throws IOException {
        ContainerId containerId = new ContainerId(id, containerName);
        deleteListContainer(containerId);
    }

    public synchronized void deleteListContainer(ContainerId containerId) throws IOException {
        initialize();
        ListContainerImpl container = lists.remove(containerId);
        if (container != null) {
            listsContainer.removeRoot(container.getIndexManager(), containerId);
            container.clear();
            container.close();
        }
    }

    public synchronized Set<ContainerId> getListContainerIds() throws IOException {
        initialize();
        Set<ContainerId> set = new HashSet<ContainerId>();
        for (Iterator i = listsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            set.add(id);
        }
        return set;
    }

    /**
     * @return the listsContainer
     */
    public IndexRootContainer getListsContainer() {
        return this.listsContainer;
    }

    /**
     * @return the mapsContainer
     */
    public IndexRootContainer getMapsContainer() {
        return this.mapsContainer;
    }

    public synchronized DataManager getDataManager(String name) throws IOException {
        DataManager dm = dataManagers.get(name);
        if (dm == null) {
            if (isUseAsyncDataManager()) {
                AsyncDataManager t = new AsyncDataManager(storeSize);
                t.setDirectory(directory);
                t.setFilePrefix("async-data-" + name + "-");
                t.setMaxFileLength((int)maxDataFileLength);
                t.start();
                dm = new DataManagerFacade(t, name);
            } else {
                DataManagerImpl t = new DataManagerImpl(directory, name,storeSize);
                t.setMaxFileLength(maxDataFileLength);
                dm = t;
            }
            if (logIndexChanges) {
                recover(dm);
            }
            dataManagers.put(name, dm);
        }
        return dm;
    }

    public synchronized IndexManager getIndexManager(DataManager dm, String name) throws IOException {
        IndexManager im = indexManagers.get(name);
        if (im == null) {
            im = new IndexManager(directory, name, mode, logIndexChanges ? dm : null,storeSize);
            indexManagers.put(name, im);
        }
        return im;
    }

    private void recover(final DataManager dm) throws IOException {
        dm.recoverRedoItems(new RedoListener() {
            public void onRedoItem(StoreLocation item, Object o) throws Exception {
                RedoStoreIndexItem redo = (RedoStoreIndexItem)o;
                // IndexManager im = getIndexManager(dm, redo.getIndexName());
                IndexManager im = getIndexManager(dm, dm.getName());
                im.redo(redo);
            }
        });
    }

    public synchronized boolean isLogIndexChanges() {
        return logIndexChanges;
    }

    public synchronized void setLogIndexChanges(boolean logIndexChanges) {
        this.logIndexChanges = logIndexChanges;
    }

    /**
     * @return the maxDataFileLength
     */
    public synchronized long getMaxDataFileLength() {
        return maxDataFileLength;
    }

    /**
     * @param maxDataFileLength the maxDataFileLength to set
     */
    public synchronized void setMaxDataFileLength(long maxDataFileLength) {
        this.maxDataFileLength = maxDataFileLength;
    }

    /**
     * @return the default index type
     */
    public synchronized String getIndexTypeAsString() {
        return persistentIndex ? "PERSISTENT" : "VM";
    }

    /**
     * Set the default index type
     * 
     * @param type "PERSISTENT" or "VM"
     */
    public synchronized void setIndexTypeAsString(String type) {
        if (type.equalsIgnoreCase("VM")) {
            persistentIndex = false;
        } else {
            persistentIndex = true;
        }
    }
    
    public boolean isPersistentIndex() {
		return persistentIndex;
	}

	public void setPersistentIndex(boolean persistentIndex) {
		this.persistentIndex = persistentIndex;
	}
	

    public synchronized boolean isUseAsyncDataManager() {
        return useAsyncDataManager;
    }

    public synchronized void setUseAsyncDataManager(boolean useAsyncWriter) {
        this.useAsyncDataManager = useAsyncWriter;
    }

    /**
     * @return size of store
     * @see org.apache.activemq.kaha.Store#size()
     */
    public long size(){
        return storeSize.get();
    }

    public String getDefaultContainerName() {
        return defaultContainerName;
    }

    public void setDefaultContainerName(String defaultContainerName) {
        this.defaultContainerName = defaultContainerName;
    }

    public synchronized void initialize() throws IOException {
        if (closed) {
            throw new IOException("Store has been closed.");
        }
        if (!initialized) {       
            LOG.info("Kaha Store using data directory " + directory);
            lockFile = new RandomAccessFile(new File(directory, "lock"), "rw");
            lock();
            DataManager defaultDM = getDataManager(defaultContainerName);
            IndexManager rootIndexManager = getIndexManager(defaultDM, defaultContainerName);
            IndexItem mapRoot = new IndexItem();
            IndexItem listRoot = new IndexItem();
            if (rootIndexManager.isEmpty()) {
                mapRoot.setOffset(0);
                rootIndexManager.storeIndex(mapRoot);
                listRoot.setOffset(IndexItem.INDEX_SIZE);
                rootIndexManager.storeIndex(listRoot);
                rootIndexManager.setLength(IndexItem.INDEX_SIZE * 2);
            } else {
                mapRoot = rootIndexManager.getIndex(0);
                listRoot = rootIndexManager.getIndex(IndexItem.INDEX_SIZE);
            }
            initialized = true;
            mapsContainer = new IndexRootContainer(mapRoot, rootIndexManager, defaultDM);
            listsContainer = new IndexRootContainer(listRoot, rootIndexManager, defaultDM);
            /**
             * Add interest in data files - then consolidate them
             */
            generateInterestInMapDataFiles();
            generateInterestInListDataFiles();
            for (Iterator<DataManager> i = dataManagers.values().iterator(); i.hasNext();) {
                DataManager dm = i.next();
                dm.consolidateDataFiles();
            }
        }
    }

    private void lock() throws IOException {
        synchronized (LOCKSET_MONITOR) {
            if (!DISABLE_LOCKING && directory != null && lock == null) {
                String key = getPropertyKey();
                String property = System.getProperty(key);
                if (null == property) {
                    if (!BROKEN_FILE_LOCK) {
                        lock = lockFile.getChannel().tryLock(0, Math.max(1, lockFile.getChannel().size()), false);
                        if (lock == null) {
                            throw new StoreLockedExcpetion("Kaha Store " + directory.getName() + "  is already opened by another application");
                        } else
                            System.setProperty(key, new Date().toString());
                    }
                } else { //already locked
                    throw new StoreLockedExcpetion("Kaha Store " + directory.getName() + " is already opened by this application.");
                }
            }
        }
    }

    private void unlock() throws IOException {
        synchronized (LOCKSET_MONITOR) {
            if (!DISABLE_LOCKING && (null != directory) && (null != lock)) {
                System.getProperties().remove(getPropertyKey());
                if (lock.isValid()) {
                    lock.release();
                }
                lock = null;
            }
        }
    }


    private String getPropertyKey() throws IOException {
        return getClass().getName() + ".lock." + directory.getCanonicalPath();
    }

    /**
     * scans the directory and builds up the IndexManager and DataManager
     * 
     * @throws IOException if there is a problem accessing an index or data file
     */
    private void generateInterestInListDataFiles() throws IOException {
        for (Iterator i = listsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            DataManager dm = getDataManager(id.getDataContainerName());
            IndexManager im = getIndexManager(dm, id.getDataContainerName());
            IndexItem theRoot = listsContainer.getRoot(im, id);
            long nextItem = theRoot.getNextItem();
            while (nextItem != Item.POSITION_NOT_SET) {
                IndexItem item = im.getIndex(nextItem);
                item.setOffset(nextItem);
                dm.addInterestInFile(item.getKeyFile());
                dm.addInterestInFile(item.getValueFile());
                nextItem = item.getNextItem();
            }
        }
    }

    /**
     * scans the directory and builds up the IndexManager and DataManager
     * 
     * @throws IOException if there is a problem accessing an index or data file
     */
    private void generateInterestInMapDataFiles() throws IOException {
        for (Iterator i = mapsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            DataManager dm = getDataManager(id.getDataContainerName());
            IndexManager im = getIndexManager(dm, id.getDataContainerName());
            IndexItem theRoot = mapsContainer.getRoot(im, id);
            long nextItem = theRoot.getNextItem();
            while (nextItem != Item.POSITION_NOT_SET) {
                IndexItem item = im.getIndex(nextItem);
                item.setOffset(nextItem);
                dm.addInterestInFile(item.getKeyFile());
                dm.addInterestInFile(item.getValueFile());
                nextItem = item.getNextItem();
            }

        }
    }
}
