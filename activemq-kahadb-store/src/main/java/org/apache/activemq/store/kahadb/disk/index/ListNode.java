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
package org.apache.activemq.store.kahadb.disk.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LinkedNode;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.store.kahadb.disk.util.Marshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;

/**
 * The ListNode class represents a node in the List object graph. It is stored
 * in one overflowing Page of a PageFile.
 */
public final class ListNode<Key, Value> {

    private final static boolean ADD_FIRST = true;
    private final static boolean ADD_LAST = false;

    // The index that this node is part of.
    private ListIndex<Key, Value> containingList;

    // The page associated with this node
    private Page<ListNode<Key, Value>> page;

    private LinkedNodeList<KeyValueEntry<Key, Value>> entries = new LinkedNodeList<KeyValueEntry<Key, Value>>() {

        @Override
        public String toString() {
            return "PageId:" + page.getPageId() + ", index:" + containingList + super.toString();
        }
    };

    // The next page after this one.
    private long next = ListIndex.NOT_SET;

    static final class KeyValueEntry<Key, Value> extends LinkedNode<KeyValueEntry<Key, Value>> implements Entry<Key, Value> {

        private final Key key;
        private Value value;

        public KeyValueEntry(Key key, Value value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Key getKey() {
            return key;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public Value setValue(Value value) {
            Value oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public String toString() {
            return "{" + key + ":" + value + "}";
        }
    }

    private final class ListNodeIterator implements Iterator<ListNode<Key, Value>> {

        private final Transaction tx;
        private final ListIndex<Key, Value> index;
        ListNode<Key, Value> nextEntry;

        private ListNodeIterator(Transaction tx, ListNode<Key, Value> current) {
            this.tx = tx;
            nextEntry = current;
            index = current.getContainingList();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public ListNode<Key, Value> next() {
            ListNode<Key, Value> current = nextEntry;
            if (current != null) {
                if (current.next != ListIndex.NOT_SET) {
                    try {
                        nextEntry = index.loadNode(tx, current.next);
                    } catch (IOException unexpected) {
                        IllegalStateException e = new IllegalStateException("failed to load next: " + current.next + ", reason: "
                                + unexpected.getLocalizedMessage());
                        e.initCause(unexpected);
                        throw e;
                    }
                } else {
                    nextEntry = null;
                }
            }
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    final class ListIterator implements Iterator<Entry<Key, Value>> {

        private final Transaction tx;
        private final ListIndex<Key, Value> targetList;
        ListNode<Key, Value> currentNode, previousNode;
        KeyValueEntry<Key, Value> nextEntry;
        KeyValueEntry<Key, Value> entryToRemove;

        private ListIterator(Transaction tx, ListNode<Key, Value> current, long start) {
            this.tx = tx;
            this.currentNode = current;
            this.targetList = current.getContainingList();
            nextEntry = current.entries.getHead();
            if (start > 0) {
                moveToRequestedStart(start);
            }
        }

        private void moveToRequestedStart(final long start) {
            long count = 0;
            while (hasNext() && count < start) {
                next();
                count++;
            }
            if (!hasNext()) {
                throw new NoSuchElementException("Index " + start + " out of current range: " + count);
            }
        }

        private KeyValueEntry<Key, Value> getFromNextNode() {
            KeyValueEntry<Key, Value> result = null;
            if (currentNode.getNext() != ListIndex.NOT_SET) {
                try {
                    previousNode = currentNode;
                    currentNode = targetList.loadNode(tx, currentNode.getNext());
                } catch (IOException unexpected) {
                    NoSuchElementException e = new NoSuchElementException(unexpected.getLocalizedMessage());
                    e.initCause(unexpected);
                    throw e;
                }
                result = currentNode.entries.getHead();
            }
            return result;
        }

        @Override
        public boolean hasNext() {
            if (nextEntry == null) {
                nextEntry = getFromNextNode();
            }
            return nextEntry != null;
        }

        @Override
        public Entry<Key, Value> next() {
            if (nextEntry != null) {
                entryToRemove = nextEntry;
                nextEntry = entryToRemove.getNext();
                return entryToRemove;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            if (entryToRemove == null) {
                throw new IllegalStateException("can only remove once, call hasNext();next() again");
            }
            try {
                entryToRemove.unlink();

                ListNode<Key, Value> toRemoveNode = null;
                if (currentNode.entries.isEmpty()) {
                    // may need to free this node
                    if (currentNode.isHead() && currentNode.isTail()) {
                        // store empty list
                    } else if (currentNode.isHead()) {
                        // merge next node into existing headNode as we don't want to
                        // change our headPageId b/c that is our identity
                        ListNode<Key, Value> headNode = currentNode;
                        nextEntry = getFromNextNode(); // will move currentNode

                        if (currentNode.isTail()) {
                            targetList.setTailPageId(headNode.getPageId());
                        }
                        // copy next/currentNode into head
                        headNode.setEntries(currentNode.entries);
                        headNode.setNext(currentNode.getNext());
                        headNode.store(tx);
                        toRemoveNode = currentNode;
                        currentNode = headNode;

                    } else if (currentNode.isTail()) {
                        toRemoveNode = currentNode;
                        previousNode.setNext(ListIndex.NOT_SET);
                        previousNode.store(tx);
                        targetList.setTailPageId(previousNode.getPageId());
                    } else {
                        toRemoveNode = currentNode;
                        previousNode.setNext(toRemoveNode.getNext());
                        previousNode.store(tx);
                        currentNode = previousNode;
                    }
                }

                targetList.onRemove(entryToRemove);
                entryToRemove = null;

                if (toRemoveNode != null) {
                    tx.free(toRemoveNode.getPage());
                } else {
                    currentNode.store(tx);
                }
            } catch (IOException unexpected) {
                IllegalStateException e = new IllegalStateException(unexpected.getLocalizedMessage());
                e.initCause(unexpected);
                throw e;
            }
        }

        ListNode<Key, Value> getCurrent() {
            return this.currentNode;
        }
    }

    /**
     * The Marshaller is used to store and load the data in the ListNode into a Page.
     *
     * @param <Key>
     * @param <Value>
     */
    static public final class NodeMarshaller<Key, Value> extends VariableMarshaller<ListNode<Key, Value>> {
        private final Marshaller<Key> keyMarshaller;
        private final Marshaller<Value> valueMarshaller;

        public NodeMarshaller(Marshaller<Key> keyMarshaller, Marshaller<Value> valueMarshaller) {
            this.keyMarshaller = keyMarshaller;
            this.valueMarshaller = valueMarshaller;
        }

        @Override
        public void writePayload(ListNode<Key, Value> node, DataOutput os) throws IOException {
            os.writeLong(node.next);
            short count = (short) node.entries.size(); // cast may truncate
                                                       // value...
            if (count != node.entries.size()) {
                throw new IOException("short over flow, too many entries in list: " + node.entries.size());
            }

            os.writeShort(count);
            KeyValueEntry<Key, Value> entry = node.entries.getHead();
            while (entry != null) {
                keyMarshaller.writePayload((Key) entry.getKey(), os);
                valueMarshaller.writePayload((Value) entry.getValue(), os);
                entry = entry.getNext();
            }
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public ListNode<Key, Value> readPayload(DataInput is) throws IOException {
            ListNode<Key, Value> node = new ListNode<Key, Value>();
            node.setNext(is.readLong());
            final short size = is.readShort();
            for (short i = 0; i < size; i++) {
                node.entries.addLast(new KeyValueEntry(keyMarshaller.readPayload(is), valueMarshaller.readPayload(is)));
            }
            return node;
        }
    }

    public Value put(Transaction tx, Key key, Value value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        entries.addLast(new KeyValueEntry<Key, Value>(key, value));
        store(tx, ADD_LAST);
        return null;
    }

    public Value addFirst(Transaction tx, Key key, Value value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        entries.addFirst(new KeyValueEntry<Key, Value>(key, value));
        store(tx, ADD_FIRST);
        return null;
    }

    public void storeUpdate(Transaction tx) throws IOException {
        store(tx, ADD_LAST);
    }

    private void store(Transaction tx, boolean addFirst) throws IOException {
        try {
            // keeping splitting till we get down to a single entry
            // then we need to overflow the value
            getContainingList().storeNode(tx, this, entries.size() == 1);

            if (this.next == -1) {
                getContainingList().setTailPageId(getPageId());
            }

        } catch (Transaction.PageOverflowIOException e) {
            // If we get an overflow
            split(tx, addFirst);
        }
    }

    private void store(Transaction tx) throws IOException {
        getContainingList().storeNode(tx, this, true);
    }

    private void split(Transaction tx, boolean isAddFirst) throws IOException {
        ListNode<Key, Value> extension = getContainingList().createNode(tx);
        if (isAddFirst) {
            // head keeps the first entry, insert extension with the rest
            extension.setEntries(entries.getHead().splitAfter());
            extension.setNext(this.getNext());
            extension.store(tx, isAddFirst);
            this.setNext(extension.getPageId());
        } else {
            extension.setEntries(entries.getTail().getPrevious().splitAfter());
            extension.setNext(this.getNext());
            extension.store(tx, isAddFirst);
            getContainingList().setTailPageId(extension.getPageId());
            this.setNext(extension.getPageId());
        }
        store(tx, true);
    }

    // called after a split
    private void setEntries(LinkedNodeList<KeyValueEntry<Key, Value>> list) {
        this.entries = list;
    }

    public Value get(Transaction tx, Key key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        Value result = null;
        KeyValueEntry<Key, Value> nextEntry = entries.getTail();
        while (nextEntry != null) {
            if (nextEntry.getKey().equals(key)) {
                result = nextEntry.getValue();
                break;
            }
            nextEntry = nextEntry.getPrevious();
        }
        return result;
    }

    public boolean isEmpty(final Transaction tx) {
        return entries.isEmpty();
    }

    public Entry<Key, Value> getFirst(Transaction tx) {
        return entries.getHead();
    }

    public Entry<Key, Value> getLast(Transaction tx) {
        return entries.getTail();
    }

    public Iterator<Entry<Key, Value>> iterator(final Transaction tx, long pos) throws IOException {
        return new ListIterator(tx, this, pos);
    }

    public Iterator<Entry<Key, Value>> iterator(final Transaction tx) throws IOException {
        return new ListIterator(tx, this, 0);
    }

    Iterator<ListNode<Key, Value>> listNodeIterator(final Transaction tx) throws IOException {
        return new ListNodeIterator(tx, this);
    }

    public void clear(Transaction tx) throws IOException {
        entries.clear();
        tx.free(this.getPageId());
    }

    public boolean contains(Transaction tx, Key key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        boolean found = false;
        KeyValueEntry<Key, Value> nextEntry = entries.getTail();
        while (nextEntry != null) {
            if (nextEntry.getKey().equals(key)) {
                found = true;
                break;
            }
            nextEntry = nextEntry.getPrevious();
        }
        return found;
    }

    // /////////////////////////////////////////////////////////////////
    // Implementation methods
    // /////////////////////////////////////////////////////////////////

    public long getPageId() {
        return page.getPageId();
    }

    public Page<ListNode<Key, Value>> getPage() {
        return page;
    }

    public void setPage(Page<ListNode<Key, Value>> page) {
        this.page = page;
    }

    public long getNext() {
        return next;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public void setContainingList(ListIndex<Key, Value> list) {
        this.containingList = list;
    }

    public ListIndex<Key, Value> getContainingList() {
        return containingList;
    }

    public boolean isHead() {
        return getPageId() == containingList.getHeadPageId();
    }

    public boolean isTail() {
        return getPageId() == containingList.getTailPageId();
    }

    public int size(Transaction tx) {
        return entries.size();
    }

    @Override
    public String toString() {
        return "[ListNode(" + (page != null ? page.getPageId() + "->" + next : "null") + ")[" + entries.size() + "]]";
    }
}
