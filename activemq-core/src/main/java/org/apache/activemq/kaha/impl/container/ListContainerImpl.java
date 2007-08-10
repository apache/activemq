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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of a ListContainer
 * 
 * @version $Revision: 1.2 $
 */
public class ListContainerImpl extends BaseContainerImpl implements ListContainer {

    private static final Log LOG = LogFactory.getLog(ListContainerImpl.class);
    protected Marshaller marshaller = Store.OBJECT_MARSHALLER;

    public ListContainerImpl(ContainerId id, IndexItem root, IndexManager indexManager,
                             DataManager dataManager, boolean persistentIndex) throws IOException {
        super(id, root, indexManager, dataManager, persistentIndex);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#load()
     */
    public synchronized void load() {
        checkClosed();
        if (!loaded) {
            if (!loaded) {
                loaded = true;
                try {
                    init();
                    long nextItem = root.getNextItem();
                    while (nextItem != Item.POSITION_NOT_SET) {
                        IndexItem item = indexManager.getIndex(nextItem);
                        indexList.add(item);
                        itemAdded(item, indexList.size() - 1, getValue(item));
                        nextItem = item.getNextItem();
                    }
                } catch (IOException e) {
                    LOG.error("Failed to load container " + getId(), e);
                    throw new RuntimeStoreException(e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#unload()
     */
    public synchronized void unload() {
        checkClosed();
        if (loaded) {
            loaded = false;
            indexList.clear();

        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#setKeyMarshaller(org.apache.activemq.kaha.Marshaller)
     */
    public synchronized void setMarshaller(Marshaller marshaller) {
        checkClosed();
        this.marshaller = marshaller;
    }

    public synchronized boolean equals(Object obj) {
        load();
        boolean result = false;
        if (obj != null && obj instanceof List) {
            List other = (List)obj;
            result = other.size() == size();
            if (result) {
                for (int i = 0; i < indexList.size(); i++) {
                    Object o1 = other.get(i);
                    Object o2 = get(i);
                    result = o1 == o2 || (o1 != null && o2 != null && o1.equals(o2));
                    if (!result) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    public int hashCode() {
        return super.hashCode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#size()
     */
    public synchronized int size() {
        load();
        return indexList.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addFirst(java.lang.Object)
     */
    public synchronized void addFirst(Object o) {
        internalAddFirst(o);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addLast(java.lang.Object)
     */
    public synchronized void addLast(Object o) {
        internalAddLast(o);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#removeFirst()
     */
    public synchronized Object removeFirst() {
        load();
        Object result = null;
        IndexItem item = indexList.getFirst();
        if (item != null) {
            itemRemoved(0);
            result = getValue(item);
            IndexItem prev = root;
            IndexItem next = indexList.size() > 1 ? (IndexItem)indexList.get(1) : null;
            indexList.removeFirst();

            delete(item, prev, next);
            item = null;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#removeLast()
     */
    public synchronized Object removeLast() {
        load();
        Object result = null;
        IndexItem last = indexList.getLast();
        if (last != null) {
            itemRemoved(indexList.size() - 1);
            result = getValue(last);
            IndexItem prev = indexList.getPrevEntry(last);
            IndexItem next = null;
            indexList.removeLast();
            delete(last, prev, next);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#isEmpty()
     */
    public synchronized boolean isEmpty() {
        load();
        return indexList.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#contains(java.lang.Object)
     */
    public synchronized boolean contains(Object o) {
        load();
        boolean result = false;
        if (o != null) {
            IndexItem next = indexList.getFirst();
            while (next != null) {
                Object value = getValue(next);
                if (value != null && value.equals(o)) {
                    result = true;
                    break;
                }
                next = indexList.getNextEntry(next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#iterator()
     */
    public synchronized Iterator iterator() {
        load();
        return listIterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#toArray()
     */
    public synchronized Object[] toArray() {
        load();
        List tmp = new ArrayList(indexList.size());
        IndexItem next = indexList.getFirst();
        while (next != null) {
            Object value = getValue(next);
            tmp.add(value);
            next = indexList.getNextEntry(next);
        }
        return tmp.toArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#toArray(T[])
     */
    public synchronized Object[] toArray(Object[] a) {
        load();
        List tmp = new ArrayList(indexList.size());
        IndexItem next = indexList.getFirst();
        while (next != null) {
            Object value = getValue(next);
            tmp.add(value);
            next = indexList.getNextEntry(next);
        }
        return tmp.toArray(a);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#add(E)
     */
    public synchronized boolean add(Object o) {
        load();
        addLast(o);
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#remove(java.lang.Object)
     */
    public synchronized boolean remove(Object o) {
        load();
        boolean result = false;
        int pos = 0;
        IndexItem next = indexList.getFirst();
        while (next != null) {
            Object value = getValue(next);
            if (value != null && value.equals(o)) {
                remove(next);
                itemRemoved(pos);
                result = true;
                break;
            }
            next = indexList.getNextEntry(next);
            pos++;
        }
        return result;
    }

    protected synchronized void remove(IndexItem item) {
        IndexItem prev = indexList.getPrevEntry(item);
        IndexItem next = indexList.getNextEntry(item);
        indexList.remove(item);

        delete(item, prev, next);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#containsAll(java.util.Collection)
     */
    public synchronized boolean containsAll(Collection c) {
        load();
        for (Iterator i = c.iterator(); i.hasNext();) {
            Object obj = i.next();
            if (!contains(obj)) {
                return false;
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#addAll(java.util.Collection)
     */
    public synchronized boolean addAll(Collection c) {
        load();
        for (Iterator i = c.iterator(); i.hasNext();) {
            add(i.next());
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#addAll(int, java.util.Collection)
     */
    public synchronized boolean addAll(int index, Collection c) {
        load();
        boolean result = false;
        ListIterator e1 = listIterator(index);
        Iterator e2 = c.iterator();
        while (e2.hasNext()) {
            e1.add(e2.next());
            result = true;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#removeAll(java.util.Collection)
     */
    public synchronized boolean removeAll(Collection c) {
        load();
        boolean result = true;
        for (Iterator i = c.iterator(); i.hasNext();) {
            Object obj = i.next();
            result &= remove(obj);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#retainAll(java.util.Collection)
     */
    public synchronized boolean retainAll(Collection c) {
        load();
        List tmpList = new ArrayList();
        IndexItem next = indexList.getFirst();
        while (next != null) {
            Object o = getValue(next);
            if (!c.contains(o)) {
                tmpList.add(o);
            }
            next = indexList.getNextEntry(next);
        }
        for (Iterator i = tmpList.iterator(); i.hasNext();) {
            remove(i.next());
        }
        return !tmpList.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#clear()
     */
    public synchronized void clear() {
        checkClosed();
        super.clear();
        doClear();

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#get(int)
     */
    public synchronized Object get(int index) {
        load();
        Object result = null;
        IndexItem item = indexList.get(index);
        if (item != null) {
            result = getValue(item);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#set(int, E)
     */
    public synchronized Object set(int index, Object element) {
        load();
        Object result = null;
        IndexItem replace = indexList.isEmpty() ? null : (IndexItem)indexList.get(index);
        IndexItem prev = (indexList.isEmpty() || (index - 1) < 0) ? null : (IndexItem)indexList
            .get(index - 1);
        IndexItem next = (indexList.isEmpty() || (index + 1) >= size()) ? null : (IndexItem)indexList
            .get(index + 1);
        result = getValue(replace);
        indexList.remove(index);
        delete(replace, prev, next);
        itemRemoved(index);
        add(index, element);
        return result;
    }

    protected synchronized IndexItem internalSet(int index, Object element) {
        IndexItem replace = indexList.isEmpty() ? null : (IndexItem)indexList.get(index);
        IndexItem prev = (indexList.isEmpty() || (index - 1) < 0) ? null : (IndexItem)indexList
            .get(index - 1);
        IndexItem next = (indexList.isEmpty() || (index + 1) >= size()) ? null : (IndexItem)indexList
            .get(index + 1);
        indexList.remove(index);
        delete(replace, prev, next);
        itemRemoved(index);
        return internalAdd(index, element);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#add(int, E)
     */
    public synchronized void add(int index, Object element) {
        load();
        IndexItem item = insert(index, element);
        indexList.add(index, item);
        itemAdded(item, index, element);
    }

    protected synchronized StoreEntry internalAddLast(Object o) {
        load();
        IndexItem item = writeLast(o);
        indexList.addLast(item);
        itemAdded(item, indexList.size() - 1, o);
        return item;
    }

    protected synchronized StoreEntry internalAddFirst(Object o) {
        load();
        IndexItem item = writeFirst(o);
        indexList.addFirst(item);
        itemAdded(item, 0, o);
        return item;
    }

    protected synchronized IndexItem internalAdd(int index, Object element) {
        load();
        IndexItem item = insert(index, element);
        indexList.add(index, item);
        itemAdded(item, index, element);
        return item;
    }

    protected synchronized StoreEntry internalGet(int index) {
        load();
        if (index >= 0 && index < indexList.size()) {
            return indexList.get(index);
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#doRemove(int)
     */
    public synchronized boolean doRemove(int index) {
        load();
        boolean result = false;
        IndexItem item = indexList.get(index);
        if (item != null) {
            result = true;
            IndexItem prev = indexList.getPrevEntry(item);
            prev = prev != null ? prev : root;
            IndexItem next = indexList.getNextEntry(prev);
            indexList.remove(index);
            itemRemoved(index);
            delete(item, prev, next);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#remove(int)
     */
    public synchronized Object remove(int index) {
        load();
        Object result = null;
        IndexItem item = indexList.get(index);
        if (item != null) {
            itemRemoved(index);
            result = getValue(item);
            IndexItem prev = indexList.getPrevEntry(item);
            prev = prev != null ? prev : root;
            IndexItem next = indexList.getNextEntry(item);
            indexList.remove(index);
            delete(item, prev, next);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#indexOf(java.lang.Object)
     */
    public synchronized int indexOf(Object o) {
        load();
        int result = -1;
        if (o != null) {
            int count = 0;
            IndexItem next = indexList.getFirst();
            while (next != null) {
                Object value = getValue(next);
                if (value != null && value.equals(o)) {
                    result = count;
                    break;
                }
                count++;
                next = indexList.getNextEntry(next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#lastIndexOf(java.lang.Object)
     */
    public synchronized int lastIndexOf(Object o) {
        load();
        int result = -1;
        if (o != null) {
            int count = indexList.size() - 1;
            IndexItem next = indexList.getLast();
            while (next != null) {
                Object value = getValue(next);
                if (value != null && value.equals(o)) {
                    result = count;
                    break;
                }
                count--;
                next = indexList.getPrevEntry(next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator()
     */
    public synchronized ListIterator listIterator() {
        load();
        return new ContainerListIterator(this, indexList, indexList.getRoot());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator(int)
     */
    public synchronized ListIterator listIterator(int index) {
        load();
        IndexItem start = (index - 1) > 0 ? indexList.get(index - 1) : indexList.getRoot();
        return new ContainerListIterator(this, indexList, start);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#subList(int, int)
     */
    public synchronized List subList(int fromIndex, int toIndex) {
        load();
        List result = new ArrayList();
        int count = fromIndex;
        IndexItem next = indexList.get(fromIndex);
        while (next != null && count++ < toIndex) {
            result.add(getValue(next));
            next = indexList.getNextEntry(next);
        }
        return result;
    }

    /**
     * add an Object to the list but get a StoreEntry of its position
     * 
     * @param object
     * @return the entry in the Store
     */
    public synchronized StoreEntry placeLast(Object object) {
        StoreEntry item = internalAddLast(object);
        return item;
    }

    /**
     * insert an Object in first position int the list but get a StoreEntry of
     * its position
     * 
     * @param object
     * @return the location in the Store
     */
    public synchronized StoreEntry placeFirst(Object object) {
        StoreEntry item = internalAddFirst(object);
        return item;
    }

    /**
     * @param entry
     * @param object
     * @see org.apache.activemq.kaha.ListContainer#update(org.apache.activemq.kaha.StoreEntry,
     *      java.lang.Object)
     */
    public synchronized void update(StoreEntry entry, Object object) {
        try {
            dataManager.updateItem(entry.getValueDataItem(), marshaller, object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve an Object from the Store by its location
     * 
     * @param entry
     * @return the Object at that entry
     */
    public synchronized Object get(final StoreEntry entry) {
        load();
        StoreEntry entryToUse = refresh(entry);
        return getValue(entryToUse);
    }

    /**
     * remove the Object at the StoreEntry
     * 
     * @param entry
     * @return true if successful
     */
    public synchronized boolean remove(StoreEntry entry) {
        IndexItem item = (IndexItem)entry;
        load();
        boolean result = false;
        if (item != null) {

            remove(item);
            result = true;
        }
        return result;
    }

    /**
     * Get the StoreEntry for the first item of the list
     * 
     * @return the first StoreEntry or null if the list is empty
     */
    public synchronized StoreEntry getFirst() {
        load();
        return indexList.getFirst();
    }

    /**
     * Get the StoreEntry for the last item of the list
     * 
     * @return the last StoreEntry or null if the list is empty
     */
    public synchronized StoreEntry getLast() {
        load();
        return indexList.getLast();
    }

    /**
     * Get the next StoreEntry from the list
     * 
     * @param entry
     * @return the next StoreEntry or null
     */
    public synchronized StoreEntry getNext(StoreEntry entry) {
        load();
        IndexItem item = (IndexItem)entry;
        return indexList.getNextEntry(item);
    }

    /**
     * Get the previous StoreEntry from the list
     * 
     * @param entry
     * @return the previous store entry or null
     */
    public synchronized StoreEntry getPrevious(StoreEntry entry) {
        load();
        IndexItem item = (IndexItem)entry;
        return indexList.getPrevEntry(item);
    }

    /**
     * It's possible that a StoreEntry could be come stale this will return an
     * upto date entry for the StoreEntry position
     * 
     * @param entry old entry
     * @return a refreshed StoreEntry
     */
    public synchronized StoreEntry refresh(StoreEntry entry) {
        load();
        return indexList.getEntry(entry);
    }

    protected synchronized IndexItem writeLast(Object value) {
        IndexItem index = null;
        try {
            if (value != null) {
                StoreLocation data = dataManager.storeDataItem(marshaller, value);
                index = indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev = indexList.getLast();
                prev = prev != null ? prev : root;
                IndexItem next = indexList.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndexes(prev);
                if (next != null) {
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndexes(next);
                }
                storeIndex(index);
            }
        } catch (IOException e) {
            LOG.error("Failed to write " + value, e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected synchronized IndexItem writeFirst(Object value) {
        IndexItem index = null;
        try {
            if (value != null) {
                StoreLocation data = dataManager.storeDataItem(marshaller, value);
                index = indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev = root;
                IndexItem next = indexList.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndexes(prev);
                if (next != null) {
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndexes(next);
                }
                storeIndex(index);
            }
        } catch (IOException e) {
            LOG.error("Failed to write " + value, e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected synchronized IndexItem insert(int insertPos, Object value) {
        IndexItem index = null;
        try {
            if (value != null) {
                StoreLocation data = dataManager.storeDataItem(marshaller, value);
                index = indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev = null;
                IndexItem next = null;
                if (insertPos <= 0) {
                    prev = root;
                    next = indexList.getNextEntry(root);
                } else if (insertPos >= indexList.size()) {
                    prev = indexList.getLast();
                    next = null;
                } else {
                    prev = indexList.get(insertPos);
                    prev = prev != null ? prev : root;
                    next = indexList.getNextEntry(prev);
                }
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndexes(prev);
                if (next != null) {
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndexes(next);
                }
                storeIndex(index);
            }
        } catch (IOException e) {
            LOG.error("Failed to insert " + value, e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected synchronized Object getValue(StoreEntry item) {
        Object result = null;
        if (item != null) {
            try {
                StoreLocation data = item.getValueDataItem();
                result = dataManager.readItem(marshaller, data);
            } catch (IOException e) {
                LOG.error("Failed to get value for " + item, e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    /**
     * @return a string representation of this collection.
     */
    public synchronized String toString() {
        StringBuffer result = new StringBuffer();
        result.append("[");
        Iterator i = iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
            Object o = i.next();
            result.append(String.valueOf(o));
            hasNext = i.hasNext();
            if (hasNext) {
                result.append(", ");
            }
        }
        result.append("]");
        return result.toString();
    }

    protected synchronized void itemAdded(IndexItem item, int pos, Object value) {

    }

    protected synchronized void itemRemoved(int pos) {

    }
}
