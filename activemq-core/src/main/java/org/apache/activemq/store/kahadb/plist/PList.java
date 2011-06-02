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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kahadb.index.ListIndex;
import org.apache.kahadb.index.ListNode;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.LocationMarshaller;
import org.apache.kahadb.util.StringMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PList extends ListIndex<String, Location> {
    static final Logger LOG = LoggerFactory.getLogger(PList.class);
    final PListStore store;
    private String name;
    Object indexLock;

    PList(PListStore store) {
        this.store = store;
        this.indexLock = store.getIndexLock();
        setPageFile(store.getPageFile());
        setKeyMarshaller(StringMarshaller.INSTANCE);
        setValueMarshaller(LocationMarshaller.INSTANCE);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    void read(DataInput in) throws IOException {
        this.headPageId = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.headPageId);
    }

    public synchronized void destroy() throws IOException {
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    clear(tx);
                    unload(tx);
                }
            });
        }
    }

    public void addLast(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    add(tx, id, location);
                }
            });
        }
    }

    public void addFirst(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    addFirst(tx, id, location);
                }
            });
        }
    }

    public boolean remove(final String id) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
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
                public void execute(Transaction tx) throws IOException {
                    Iterator<Map.Entry<String, Location>> iterator = iterator(tx, position);
                    ref.set(iterator.next());
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs);
        }
        return result;
    }

    public PListEntry getFirst() throws IOException {
        PListEntry result = null;
        final AtomicReference<Map.Entry<String, Location>> ref = new AtomicReference<Map.Entry<String, Location>>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(getFirst(tx));
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs);
        }
        return result;
    }

    public PListEntry getLast() throws IOException {
        PListEntry result = null;
        final AtomicReference<Map.Entry<String, Location>> ref = new AtomicReference<Map.Entry<String, Location>>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(getLast(tx));
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getValue());
            result = new PListEntry(ref.get().getKey(), bs);
        }
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    synchronized public Iterator<PListEntry> iterator() throws IOException {
        return new PListIterator();
    }

    private final class PListIterator implements Iterator<PListEntry> {
        final Iterator<Map.Entry<String, Location>> iterator;
        final Transaction tx;

        PListIterator() throws IOException {
            tx = store.pageFile.tx();
            this.iterator = iterator(tx);
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
            return new PListEntry(entry.getKey(), bs);
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
    }

    public void claimFileLocations(final Set<Integer> candidates) throws IOException {
        synchronized (indexLock) {
            if (loaded.get()) {
                this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
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
    public String toString() {
        return "" + name + ",[headPageId=" + headPageId  + ",tailPageId=" + tailPageId + ", size=" + size() + "]";
    }
}
