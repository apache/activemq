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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container of roots for other Containers
 * 
 * @version $Revision: 1.2 $
 */

class IndexRootContainer {
    
    protected static final Marshaller ROOT_MARSHALLER = Store.OBJECT_MARSHALLER;
    private static final Logger LOG = LoggerFactory.getLogger(IndexRootContainer.class);

    protected IndexItem root;
    protected IndexManager indexManager;
    protected DataManager dataManager;
    protected Map<Object, StoreEntry> map = new ConcurrentHashMap<Object, StoreEntry>();
    protected LinkedList<StoreEntry> list = new LinkedList<StoreEntry>();

    IndexRootContainer(IndexItem root, IndexManager im, DataManager dfm) throws IOException {
        this.root = root;
        this.indexManager = im;
        this.dataManager = dfm;
        long nextItem = root.getNextItem();
        while (nextItem != Item.POSITION_NOT_SET) {
            StoreEntry item = indexManager.getIndex(nextItem);
            StoreLocation data = item.getKeyDataItem();
            Object key = dataManager.readItem(ROOT_MARSHALLER, data);
            map.put(key, item);
            list.add(item);
            nextItem = item.getNextItem();
            dataManager.addInterestInFile(item.getKeyFile());
        }
    }

    Set<Object> getKeys() {
        return map.keySet();
    }

    IndexItem addRoot(IndexManager containerIndexManager, ContainerId key) throws IOException {
        if (map.containsKey(key)) {
            removeRoot(containerIndexManager, key);
        }

        StoreLocation data = dataManager.storeDataItem(ROOT_MARSHALLER, key);
        IndexItem newRoot = indexManager.createNewIndex();
        newRoot.setKeyData(data);
        IndexItem containerRoot = containerIndexManager.createNewIndex();
        containerIndexManager.storeIndex(containerRoot);
        newRoot.setValueOffset(containerRoot.getOffset());

        IndexItem last = list.isEmpty() ? null : (IndexItem)list.getLast();
        last = last == null ? root : last;
        long prev = last.getOffset();
        newRoot.setPreviousItem(prev);
        indexManager.storeIndex(newRoot);
        last.setNextItem(newRoot.getOffset());
        indexManager.storeIndex(last);
        map.put(key, newRoot);
        list.add(newRoot);
        return containerRoot;
    }

    void removeRoot(IndexManager containerIndexManager, ContainerId key) throws IOException {
        StoreEntry oldRoot = map.remove(key);
        if (oldRoot != null) {
            dataManager.removeInterestInFile(oldRoot.getKeyFile());
            // get the container root
            IndexItem containerRoot = containerIndexManager.getIndex(oldRoot.getValueOffset());
            if (containerRoot != null) {
                containerIndexManager.freeIndex(containerRoot);
            }
            int index = list.indexOf(oldRoot);
            IndexItem prev = index > 0 ? (IndexItem)list.get(index - 1) : root;
            prev = prev == null ? root : prev;
            IndexItem next = index < (list.size() - 1) ? (IndexItem)list.get(index + 1) : null;
            if (next != null) {
                prev.setNextItem(next.getOffset());
                next.setPreviousItem(prev.getOffset());
                indexManager.updateIndexes(next);
            } else {
                prev.setNextItem(Item.POSITION_NOT_SET);
            }
            indexManager.updateIndexes(prev);
            list.remove(oldRoot);
            indexManager.freeIndex((IndexItem)oldRoot);
        }
    }

    IndexItem getRoot(IndexManager containerIndexManager, ContainerId key) throws IOException {
        StoreEntry index = map.get(key);
        if (index != null) {
            return containerIndexManager.getIndex(index.getValueOffset());
        }
        return null;
    }

    boolean doesRootExist(Object key) {
        return map.containsKey(key);
    }

}
