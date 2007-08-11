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
package org.apache.activemq.kaha.impl.container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.index.DiskIndexLinkedList;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.kaha.impl.index.VMIndexLinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of a ListContainer
 * 
 * @version $Revision: 1.2 $
 */
public abstract class BaseContainerImpl {

    private static final Log LOG = LogFactory.getLog(BaseContainerImpl.class);
    protected IndexItem root;
    protected IndexLinkedList indexList;
    protected IndexManager indexManager;
    protected DataManager dataManager;
    protected ContainerId containerId;
    protected boolean loaded;
    protected boolean closed;
    protected boolean initialized;
    protected boolean persistentIndex;

    protected BaseContainerImpl(ContainerId id, IndexItem root, IndexManager indexManager, DataManager dataManager, boolean persistentIndex) {
        this.containerId = id;
        this.root = root;
        this.indexManager = indexManager;
        this.dataManager = dataManager;
        this.persistentIndex = persistentIndex;
    }

    public ContainerId getContainerId() {
        return containerId;
    }

    public synchronized void init() {
        if (!initialized) {
            if (!initialized) {
                initialized = true;
                if (this.indexList == null) {
                    if (persistentIndex) {
                        this.indexList = new DiskIndexLinkedList(indexManager, root);
                    } else {
                        this.indexList = new VMIndexLinkedList(root);
                    }
                }
            }
        }
    }

    public synchronized void clear() {
        if (indexList != null) {
            indexList.clear();
        }
    }

    /**
     * @return the indexList
     */
    public IndexLinkedList getList() {
        return indexList;
    }

    /**
     * @param indexList the indexList to set
     */
    public void setList(IndexLinkedList indexList) {
        this.indexList = indexList;
    }

    public abstract void unload();

    public abstract void load();

    public abstract int size();

    protected abstract Object getValue(StoreEntry currentItem);

    protected abstract void remove(IndexItem currentItem);

    protected final synchronized IndexLinkedList getInternalList() {
        return indexList;
    }

    public final synchronized void close() {
        unload();
        closed = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#isLoaded()
     */
    public final synchronized boolean isLoaded() {
        checkClosed();
        return loaded;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#getId()
     */
    public final Object getId() {
        checkClosed();
        return containerId.getKey();
    }

    public DataManager getDataManager() {
        return dataManager;
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public final synchronized void expressDataInterest() throws IOException {
        long nextItem = root.getNextItem();
        while (nextItem != Item.POSITION_NOT_SET) {
            IndexItem item = indexManager.getIndex(nextItem);
            item.setOffset(nextItem);
            dataManager.addInterestInFile(item.getKeyFile());
            dataManager.addInterestInFile(item.getValueFile());
            nextItem = item.getNextItem();
        }
    }

    protected final void doClear() {
        checkClosed();
        loaded = true;
        List<IndexItem> indexList = new ArrayList<IndexItem>();
        try {
            init();
            long nextItem = root.getNextItem();
            while (nextItem != Item.POSITION_NOT_SET) {
                IndexItem item = new IndexItem();
                item.setOffset(nextItem);
                indexList.add(item);
                nextItem = item.getNextItem();
            }
            root.setNextItem(Item.POSITION_NOT_SET);
            storeIndex(root);
            for (int i = 0; i < indexList.size(); i++) {
                IndexItem item = indexList.get(i);
                dataManager.removeInterestInFile(item.getKeyFile());
                dataManager.removeInterestInFile(item.getValueFile());
                indexManager.freeIndex(item);
            }
            indexList.clear();
        } catch (IOException e) {
            LOG.error("Failed to clear Container " + getId(), e);
            throw new RuntimeStoreException(e);
        }
    }

    protected final void delete(final IndexItem keyItem, final IndexItem prevItem, final IndexItem nextItem) {
        if (keyItem != null) {
            try {
                IndexItem prev = prevItem == null ? root : prevItem;
                IndexItem next = nextItem != root ? nextItem : null;
                dataManager.removeInterestInFile(keyItem.getKeyFile());
                dataManager.removeInterestInFile(keyItem.getValueFile());
                if (next != null) {
                    prev.setNextItem(next.getOffset());
                    next.setPreviousItem(prev.getOffset());
                    updateIndexes(next);
                } else {
                    prev.setNextItem(Item.POSITION_NOT_SET);
                }
                updateIndexes(prev);
                indexManager.freeIndex(keyItem);
            } catch (IOException e) {
                LOG.error("Failed to delete " + keyItem, e);
                throw new RuntimeStoreException(e);
            }
        }
    }

    protected final void checkClosed() {
        if (closed) {
            throw new RuntimeStoreException("The store is closed");
        }
    }

    protected void storeIndex(IndexItem item) throws IOException {
        indexManager.storeIndex(item);
    }

    protected void updateIndexes(IndexItem item) throws IOException {
        indexManager.updateIndexes(item);
    }

    protected final boolean isRoot(StoreEntry item) {
        return item != null && root != null && (root == item || root.getOffset() == item.getOffset());
        // return item != null && indexRoot != null && indexRoot == item;
    }

}
