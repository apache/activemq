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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kahadb.index.ListNode.ListIterator;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.PageFile;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.Marshaller;

public class ListIndex<Key,Value> implements Index<Key,Value> {

    private static final Logger LOG = LoggerFactory.getLogger(ListIndex.class);
    public  final static long NOT_SET = -1;
    protected PageFile pageFile;
    protected long headPageId;
    protected long tailPageId;
    private AtomicLong size = new AtomicLong(0);

    protected AtomicBoolean loaded = new AtomicBoolean();

    private ListNode.NodeMarshaller<Key, Value> marshaller;
    private Marshaller<Key> keyMarshaller;
    private Marshaller<Value> valueMarshaller;

    public ListIndex() {
    }

    public ListIndex(PageFile pageFile, long headPageId) {
        this.pageFile = pageFile;
        setHeadPageId(headPageId);
    }

    @SuppressWarnings("rawtypes")
    public ListIndex(PageFile pageFile, Page page) {
        this(pageFile, page.getPageId());
    }

    synchronized public void load(Transaction tx) throws IOException {
        if (loaded.compareAndSet(false, true)) {
            LOG.debug("loading");
            if( keyMarshaller == null ) {
                throw new IllegalArgumentException("The key marshaller must be set before loading the ListIndex");
            }
            if( valueMarshaller == null ) {
                throw new IllegalArgumentException("The value marshaller must be set before loading the ListIndex");
            }

            marshaller = new ListNode.NodeMarshaller<Key, Value>(keyMarshaller, valueMarshaller);
            final Page<ListNode<Key,Value>> p = tx.load(getHeadPageId(), null);
            if( p.getType() == Page.PAGE_FREE_TYPE ) {
                 // Need to initialize it..
                ListNode<Key, Value> root = createNode(p);
                storeNode(tx, root, true);
                setHeadPageId(p.getPageId());
                setTailPageId(getHeadPageId());
            } else {
                ListNode<Key, Value> node = loadNode(tx, getHeadPageId());
                setTailPageId(getHeadPageId());
                size.addAndGet(node.size(tx));
                while (node.getNext() != NOT_SET ) {
                    node = loadNode(tx, node.getNext());
                    size.addAndGet(node.size(tx));
                    setTailPageId(node.getPageId());
                }
            }
        }
    }

    synchronized public void unload(Transaction tx) {
        if (loaded.compareAndSet(true, false)) {
        }
    }

    protected ListNode<Key,Value> getHead(Transaction tx) throws IOException {
        return loadNode(tx, getHeadPageId());
    }

    protected ListNode<Key,Value> getTail(Transaction tx) throws IOException {
        return loadNode(tx, getTailPageId());
    }

    synchronized public boolean containsKey(Transaction tx, Key key) throws IOException {
        assertLoaded();

        if (size.get() == 0) {
            return false;
        }

        for (Iterator<Map.Entry<Key,Value>> iterator = iterator(tx); iterator.hasNext(); ) {
            Map.Entry<Key,Value> candidate = iterator.next();
            if (key.equals(candidate.getKey())) {
                return true;
            }
        }
        return false;
    }

    private ListNode<Key, Value> lastGetNodeCache = null;
    private Map.Entry<Key, Value> lastGetEntryCache = null;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    synchronized public Value get(Transaction tx, Key key) throws IOException {
        assertLoaded();
        for (Iterator<Map.Entry<Key,Value>> iterator = iterator(tx); iterator.hasNext(); ) {
            Map.Entry<Key,Value> candidate = iterator.next();
            if (key.equals(candidate.getKey())) {
                this.lastGetNodeCache = ((ListIterator) iterator).getCurrent();
                this.lastGetEntryCache = candidate;
                return candidate.getValue();
            }
        }
        return null;
    }

    /**
     * Update the value of the item with the given key in the list if ot exists, otherwise
     * it appends the value to the end of the list.
     *
     * @return the old value contained in the list if one exists or null.
     */
    @SuppressWarnings({ "rawtypes" })
    synchronized public Value put(Transaction tx, Key key, Value value) throws IOException {

        Value oldValue = null;

        if (lastGetNodeCache != null) {

            if(lastGetEntryCache.getKey().equals(key)) {
                oldValue = lastGetEntryCache.setValue(value);
                lastGetEntryCache.setValue(value);
                lastGetNodeCache.storeUpdate(tx);
                return oldValue;
            }

            // This searches from the last location of a call to get for the element to replace
            // all the way to the end of the ListIndex.
            Iterator<Map.Entry<Key, Value>> iterator = lastGetNodeCache.iterator(tx);
            while (iterator.hasNext()) {
                Map.Entry<Key, Value> entry = iterator.next();
                if (entry.getKey().equals(key)) {
                    oldValue = entry.setValue(value);
                    ((ListIterator) iterator).getCurrent().storeUpdate(tx);
                    return oldValue;
                }
            }
        }

        // Not found because the cache wasn't set or its not at the end of the list so we
        // start from the beginning and go to the cached location or the end, then we do
        // an add if its not found.
        Iterator<Map.Entry<Key, Value>> iterator = iterator(tx);
        while (iterator.hasNext() && ((ListIterator) iterator).getCurrent() != lastGetNodeCache) {
            Map.Entry<Key, Value> entry = iterator.next();
            if (entry.getKey().equals(key)) {
                oldValue = entry.setValue(value);
                ((ListIterator) iterator).getCurrent().storeUpdate(tx);
                return oldValue;
            }
        }

        // Not found so add it last.
        return add(tx, key, value);
    }

    synchronized public Value add(Transaction tx, Key key, Value value) throws IOException {
        assertLoaded();
        getTail(tx).put(tx, key, value);
        size.incrementAndGet();
        return null;
    }

    synchronized public Value addFirst(Transaction tx, Key key, Value value) throws IOException {
        assertLoaded();
        getHead(tx).addFirst(tx, key, value);
        size.incrementAndGet();
        return null;
    }

    @SuppressWarnings("rawtypes")
    synchronized public Value remove(Transaction tx, Key key) throws IOException {
        assertLoaded();

        if (size.get() == 0) {
            return null;
        }

        if (lastGetNodeCache != null) {

            // This searches from the last location of a call to get for the element to remove
            // all the way to the end of the ListIndex.
            Iterator<Map.Entry<Key, Value>> iterator = lastGetNodeCache.iterator(tx);
            while (iterator.hasNext()) {
                Map.Entry<Key, Value> entry = iterator.next();
                if (entry.getKey().equals(key)) {
                    iterator.remove();
                    return entry.getValue();
                }
            }
        }

        // Not found because the cache wasn't set or its not at the end of the list so we
        // start from the beginning and go to the cached location or the end to find the
        // element to remove.
        Iterator<Map.Entry<Key, Value>> iterator = iterator(tx);
        while (iterator.hasNext() && ((ListIterator) iterator).getCurrent() != lastGetNodeCache) {
            Map.Entry<Key, Value> entry = iterator.next();
            if (entry.getKey().equals(key)) {
                iterator.remove();
                return entry.getValue();
            }
        }

        return null;
    }

    public void onRemove() {
        size.decrementAndGet();
    }

    public boolean isTransient() {
        return false;
    }

    synchronized public void clear(Transaction tx) throws IOException {
        for (Iterator<ListNode<Key,Value>> iterator = listNodeIterator(tx); iterator.hasNext(); ) {
            ListNode<Key,Value>candidate = iterator.next();
            candidate.clear(tx);
            // break up the transaction
            tx.commit();
        }
        size.set(0);
    }

    synchronized public Iterator<ListNode<Key, Value>> listNodeIterator(Transaction tx) throws IOException {
        return getHead(tx).listNodeIterator(tx);
    }

    synchronized public boolean isEmpty(final Transaction tx) throws IOException {
        return getHead(tx).isEmpty(tx);
    }

    synchronized public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx) throws IOException {
        return getHead(tx).iterator(tx);
    }

    synchronized public Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx, long initialPosition) throws IOException {
        return getHead(tx).iterator(tx, initialPosition);
    }

    synchronized public Map.Entry<Key,Value> getFirst(Transaction tx) throws IOException {
        return getHead(tx).getFirst(tx);
    }

    synchronized public Map.Entry<Key,Value> getLast(Transaction tx) throws IOException {
        return getTail(tx).getLast(tx);
    }

    private void assertLoaded() throws IllegalStateException {
        if( !loaded.get() ) {
            throw new IllegalStateException("TheListIndex is not loaded");
        }
    }

    ListNode<Key,Value> loadNode(Transaction tx, long pageId) throws IOException {
        Page<ListNode<Key,Value>> page = tx.load(pageId, marshaller);
        ListNode<Key, Value> node = page.get();
        node.setPage(page);
        node.setContainingList(this);
        return node;
    }

    ListNode<Key,Value> createNode(Page<ListNode<Key,Value>> page) throws IOException {
        ListNode<Key,Value> node = new ListNode<Key,Value>();
        node.setPage(page);
        page.set(node);
        node.setContainingList(this);
        return node;
    }

    public ListNode<Key,Value> createNode(Transaction tx) throws IOException {
        return createNode(tx.<ListNode<Key,Value>>load(tx.<ListNode<Key,Value>>allocate().getPageId(), null));
    }

    public void storeNode(Transaction tx, ListNode<Key,Value> node, boolean overflow) throws IOException {
        tx.store(node.getPage(), marshaller, overflow);
        flushCache();
    }

    public PageFile getPageFile() {
        return pageFile;
    }

    public void setPageFile(PageFile pageFile) {
        this.pageFile = pageFile;
    }

    public long getHeadPageId() {
        return headPageId;
    }

    public void setHeadPageId(long headPageId) {
        this.headPageId = headPageId;
    }

    public Marshaller<Key> getKeyMarshaller() {
        return keyMarshaller;
    }
    public void setKeyMarshaller(Marshaller<Key> keyMarshaller) {
        this.keyMarshaller = keyMarshaller;
    }

    public Marshaller<Value> getValueMarshaller() {
        return valueMarshaller;
    }
    public void setValueMarshaller(Marshaller<Value> valueMarshaller) {
        this.valueMarshaller = valueMarshaller;
    }

    public void setTailPageId(long tailPageId) {
        this.tailPageId = tailPageId;
    }

    public long getTailPageId() {
       return tailPageId;
    }

    public long size() {
        return size.get();
    }

    private void flushCache() {
        this.lastGetEntryCache = null;
        this.lastGetNodeCache = null;
    }
}
