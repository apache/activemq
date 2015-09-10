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
package org.apache.activemq.store.kahadb.plist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.management.SizeStatisticImpl;
import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListEntry;
import org.apache.activemq.store.kahadb.disk.index.ListIndex;
import org.apache.activemq.store.kahadb.disk.index.ListNode;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LocationMarshaller;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.util.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PListImpl extends ListIndex<String, Location> implements PList {
    static final Logger LOG = LoggerFactory.getLogger(PListImpl.class);
    final PListStoreImpl store;
    private String name;
    Object indexLock;
    private final SizeStatisticImpl messageSize;

    PListImpl(PListStoreImpl store) {
        this.store = store;
        this.indexLock = store.getIndexLock();
        setPageFile(store.getPageFile());
        setKeyMarshaller(StringMarshaller.INSTANCE);
        setValueMarshaller(LocationMarshaller.INSTANCE);

        messageSize = new SizeStatisticImpl("messageSize", "The size in bytes of the pending messages");
        messageSize.setEnabled(true);
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    void read(DataInput in) throws IOException {
        setHeadPageId(in.readLong());
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(getHeadPageId());
    }

    @Override
    public synchronized void destroy() throws IOException {
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    clear(tx);
                    unload(tx);
                }
            });
        }
    }

    class Locator {
        final String id;

        Locator(String id) {
            this.id = id;
        }

        PListImpl plist() {
            return PListImpl.this;
        }
    }

    @Override
    public Object addLast(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    add(tx, id, location);
                }
            });
        }
        return new Locator(id);
    }

    @Override
    public Object addFirst(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    addFirst(tx, id, location);
                }
            });
        }
        return new Locator(id);
    }

    @Override
    public boolean remove(final Object l) throws IOException {
        Locator locator = (Locator) l;
        assert locator!=null;
        assert locator.plist()==this;
        return remove(locator.id);
    }

    public boolean remove(final String id) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    result.set(remove(tx, id) != null);
                }
            });
        }
        return result.get();
    }

    public boolean remove(final long position) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    Iterator<Map.Entry<String, Location>> iterator = iterator(tx, position);
                    if (iterator.hasNext()) {
                        iterator.next();
                        iterator.remove();
                        result.set(true);
                    } else {
                        result.set(false);
                    }
                }
            });
        }
        return result.get();
    }

    public PListEntry get(final long position) throws IOException {
        PListEntry result = null;
        final AtomicReference<Map.Entry<String, Location>> ref = new AtomicReference<Map.Entry<String, Location>>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    Iterator<Map.Entry<String, Location>> iterator = iterator(tx, position);
                    ref.set(iterator.next());
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs, new Locator(ref.get().getKey()));
        }
        return result;
    }

    public PListEntry getFirst() throws IOException {
        PListEntry result = null;
        final AtomicReference<Map.Entry<String, Location>> ref = new AtomicReference<Map.Entry<String, Location>>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    ref.set(getFirst(tx));
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs, new Locator(ref.get().getKey()));
        }
        return result;
    }

    public PListEntry getLast() throws IOException {
        PListEntry result = null;
        final AtomicReference<Map.Entry<String, Location>> ref = new AtomicReference<Map.Entry<String, Location>>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    ref.set(getLast(tx));
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs, new Locator(ref.get().getKey()));
        }
        return result;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public PListIterator iterator() throws IOException {
        return new PListIteratorImpl();
    }

    final class PListIteratorImpl implements PListIterator {
        final Iterator<Map.Entry<String, Location>> iterator;
        final Transaction tx;

        PListIteratorImpl() throws IOException {
            tx = store.pageFile.tx();
            synchronized (indexLock) {
                this.iterator = iterator(tx);
            }
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public PListEntry next() {
            Map.Entry<String, Location> entry = iterator.next();
            ByteSequence bs = null;
            try {
                bs = store.getPayload(entry.getValue());
            } catch (IOException unexpected) {
                NoSuchElementException e = new NoSuchElementException(unexpected.getLocalizedMessage());
                e.initCause(unexpected);
                throw e;
            }
            return new PListEntry(entry.getKey(), bs, new Locator(entry.getKey()));
        }

        @Override
        public void remove() {
            try {
                synchronized (indexLock) {
                    tx.execute(new Transaction.Closure<IOException>() {
                        @Override
                        public void execute(Transaction tx) throws IOException {
                            iterator.remove();
                        }
                    });
                }
            } catch (IOException unexpected) {
                IllegalStateException e = new IllegalStateException(unexpected);
                e.initCause(unexpected);
                throw e;
            }
        }

        @Override
        public void release() {
            try {
                tx.rollback();
            } catch (IOException unexpected) {
                IllegalStateException e = new IllegalStateException(unexpected);
                e.initCause(unexpected);
                throw e;
            }
        }
    }

    public void claimFileLocations(final Set<Integer> candidates) throws IOException {
        synchronized (indexLock) {
            if (loaded.get()) {
                this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                    @Override
                    public void execute(Transaction tx) throws IOException {
                        Iterator<Map.Entry<String,Location>> iterator = iterator(tx);
                        while (iterator.hasNext()) {
                            Location location = iterator.next().getValue();
                            candidates.remove(location.getDataFileId());
                        }
                    }
                });
            }
        }
    }

    @Override
    public long messageSize() {
        return messageSize.getTotalSize();
    }

    @Override
    public synchronized Location add(Transaction tx, String key, Location value)
            throws IOException {
        Location location = super.add(tx, key, value);
        messageSize.addSize(value.getSize());
        return location;
    }

    @Override
    public synchronized Location addFirst(Transaction tx, String key,
            Location value) throws IOException {
        Location location = super.addFirst(tx, key, value);
        messageSize.addSize(value.getSize());
        return location;
    }

    @Override
    public synchronized void clear(Transaction tx) throws IOException {
        messageSize.reset();
        super.clear(tx);
    }

    @Override
    protected synchronized void onLoad(ListNode<String, Location> node, Transaction tx) {
        try {
            Iterator<Entry<String, Location>> i = node.iterator(tx);
            while (i.hasNext()) {
                messageSize.addSize(i.next().getValue().getSize());
            }
        } catch (IOException e) {
            LOG.warn("could not increment message size", e);
        }
    }

    @Override
    public void onRemove(Entry<String, Location> removed) {
        super.onRemove(removed);
        if (removed != null) {
            messageSize.addSize(-removed.getValue().getSize());
        }
    }

    @Override
    public String toString() {
        return name + "[headPageId=" + getHeadPageId()  + ",tailPageId=" + getTailPageId() + ", size=" + size() + "]";
    }
}
