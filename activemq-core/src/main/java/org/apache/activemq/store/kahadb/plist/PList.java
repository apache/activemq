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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.store.kahadb.plist.EntryLocation.EntryLocationMarshaller;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;

public class PList {
    final PListStore store;
    private String name;
    private long rootId = EntryLocation.NOT_SET;
    private long lastId = EntryLocation.NOT_SET;
    private final AtomicBoolean loaded = new AtomicBoolean();
    private int size = 0;
    Object indexLock;

    PList(PListStore store) {
        this.store = store;
        this.indexLock = store.getIndexLock();
    }

    public void setName(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#getName()
     */
    public String getName() {
        return this.name;
    }

    public synchronized int size() {
        return this.size;
    }

    public synchronized boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return the rootId
     */
    public long getRootId() {
        return this.rootId;
    }

    /**
     * @param rootId
     *            the rootId to set
     */
    public void setRootId(long rootId) {
        this.rootId = rootId;
    }

    /**
     * @return the lastId
     */
    public long getLastId() {
        return this.lastId;
    }

    /**
     * @param lastId
     *            the lastId to set
     */
    public void setLastId(long lastId) {
        this.lastId = lastId;
    }

    /**
     * @return the loaded
     */
    public boolean isLoaded() {
        return this.loaded.get();
    }

    void read(DataInput in) throws IOException {
        this.rootId = in.readLong();
        this.name = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.rootId);
        out.writeUTF(name);
    }

    public synchronized void destroy() throws IOException {
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    destroy(tx);
                }
            });
        }
    }

    void destroy(Transaction tx) throws IOException {
        // start from the first
        EntryLocation entry = getFirst(tx);
        while (entry != null) {
            EntryLocation toRemove = entry.copy();
            entry = getNext(tx, entry.getNext());
            doRemove(tx, toRemove);
        }
    }

    synchronized void load(Transaction tx) throws IOException {
        if (loaded.compareAndSet(false, true)) {
            final Page<EntryLocation> p = tx.load(this.rootId, null);
            if (p.getType() == Page.PAGE_FREE_TYPE) {
                // Need to initialize it..
                EntryLocation root = createEntry(p, "root", EntryLocation.NOT_SET, EntryLocation.NOT_SET);

                storeEntry(tx, root);
                this.lastId = root.getPage().getPageId();
            } else {
                // find last id
                long nextId = this.rootId;
                while (nextId != EntryLocation.NOT_SET) {
                    EntryLocation next = getNext(tx, nextId);
                    if (next != null) {
                        this.lastId = next.getPage().getPageId();
                        nextId = next.getNext();
                        this.size++;
                    }
                }
            }
        }
    }

    synchronized public void unload() {
        if (loaded.compareAndSet(true, false)) {
            this.rootId = EntryLocation.NOT_SET;
            this.lastId = EntryLocation.NOT_SET;
            this.size=0;
        }
    }

    synchronized public void addLast(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    addLast(tx, id, bs, location);
                }
            });
        }
    }

    private void addLast(Transaction tx, String id, ByteSequence bs, Location location) throws IOException {
        EntryLocation entry = createEntry(tx, id, this.lastId, EntryLocation.NOT_SET);
        entry.setLocation(location);
        storeEntry(tx, entry);
        EntryLocation last = loadEntry(tx, this.lastId);
        last.setNext(entry.getPage().getPageId());
        storeEntry(tx, last);
        this.lastId = entry.getPage().getPageId();
        this.size++;
    }

    synchronized public void addFirst(final String id, final ByteSequence bs) throws IOException {
        final Location location = this.store.write(bs, false);
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    addFirst(tx, id, bs, location);
                }
            });
        }
    }

    private void addFirst(Transaction tx, String id, ByteSequence bs, Location location) throws IOException {
        EntryLocation entry = createEntry(tx, id, EntryLocation.NOT_SET, EntryLocation.NOT_SET);
        entry.setLocation(location);
        EntryLocation oldFirst = getFirst(tx);
        if (oldFirst != null) {
            oldFirst.setPrev(entry.getPage().getPageId());
            storeEntry(tx, oldFirst);
            entry.setNext(oldFirst.getPage().getPageId());

        }
        EntryLocation root = getRoot(tx);
        root.setNext(entry.getPage().getPageId());
        storeEntry(tx, root);
        storeEntry(tx, entry);

        this.size++;
    }

    synchronized public boolean remove(final String id) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    result.set(remove(tx, id));
                }
            });
        }
        return result.get();
    }

    synchronized public boolean remove(final int position) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    result.set(remove(tx, position));
                }
            });
        }
        return result.get();
    }

    synchronized public boolean remove(final PListEntry entry) throws IOException {
        final AtomicBoolean result = new AtomicBoolean();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    result.set(doRemove(tx, entry.getEntry()));
                }
            });
        }
        return result.get();
    }

    synchronized public PListEntry get(final int position) throws IOException {
        PListEntry result = null;
        final AtomicReference<EntryLocation> ref = new AtomicReference<EntryLocation>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(get(tx, position));
                }
            });
        }
        if (ref.get() != null) {
            ByteSequence bs = this.store.getPayload(ref.get().getLocation());
            result = new PListEntry(ref.get(), bs);
        }
        return result;
    }

    synchronized public PListEntry getFirst() throws IOException {
        PListEntry result = null;
        final AtomicReference<EntryLocation> ref = new AtomicReference<EntryLocation>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(getFirst(tx));
                }
            });
            if (ref.get() != null) {
                ByteSequence bs = this.store.getPayload(ref.get().getLocation());
                result = new PListEntry(ref.get(), bs);
            }
        }
        return result;
    }

    synchronized public PListEntry getLast() throws IOException {
        PListEntry result = null;
        final AtomicReference<EntryLocation> ref = new AtomicReference<EntryLocation>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(getLast(tx));
                }
            });
            if (ref.get() != null) {
                ByteSequence bs = this.store.getPayload(ref.get().getLocation());
                result = new PListEntry(ref.get(), bs);
            }
        }
        return result;
    }

    synchronized public PListEntry getNext(PListEntry entry) throws IOException {
        PListEntry result = null;
        final long nextId = entry != null ? entry.getEntry().getNext() : this.rootId;
        if (nextId != EntryLocation.NOT_SET) {
            final AtomicReference<EntryLocation> ref = new AtomicReference<EntryLocation>();
            synchronized (indexLock) {
                this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                    public void execute(Transaction tx) throws IOException {
                        ref.set(getNext(tx, nextId));
                    }
                });
                if (ref.get() != null) {
                    ByteSequence bs = this.store.getPayload(ref.get().getLocation());
                    result = new PListEntry(ref.get(), bs);
                }
            }
        }
        return result;
    }

    synchronized public PListEntry refresh(final PListEntry entry) throws IOException {
        PListEntry result = null;
        final AtomicReference<EntryLocation> ref = new AtomicReference<EntryLocation>();
        synchronized (indexLock) {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                public void execute(Transaction tx) throws IOException {
                    ref.set(loadEntry(tx, entry.getEntry().getPage().getPageId()));
                }
            });
            if (ref.get() != null) {
                result = new PListEntry(ref.get(), entry.getByteSequence());
            }
        }
        return result;
    }

    boolean remove(Transaction tx, String id) throws IOException {
        boolean result = false;
        long nextId = this.rootId;
        while (nextId != EntryLocation.NOT_SET) {
            EntryLocation entry = getNext(tx, nextId);
            if (entry != null) {
                if (entry.getId().equals(id)) {
                    result = doRemove(tx, entry);
                    break;
                }
                nextId = entry.getNext();
            } else {
                // not found
                break;
            }
        }
        return result;
    }

    boolean remove(Transaction tx, int position) throws IOException {
        boolean result = false;
        long nextId = this.rootId;
        int count = 0;
        while (nextId != EntryLocation.NOT_SET) {
            EntryLocation entry = getNext(tx, nextId);
            if (entry != null) {
                if (count == position) {
                    result = doRemove(tx, entry);
                    break;
                }
                nextId = entry.getNext();
            } else {
                // not found
                break;
            }
            count++;
        }
        return result;
    }

    EntryLocation get(Transaction tx, int position) throws IOException {
        EntryLocation result = null;
        long nextId = this.rootId;
        int count = -1;
        while (nextId != EntryLocation.NOT_SET) {
            EntryLocation entry = getNext(tx, nextId);
            if (entry != null) {
                if (count == position) {
                    result = entry;
                    break;
                }
                nextId = entry.getNext();
            } else {
                break;
            }
            count++;
        }
        return result;
    }

    EntryLocation getFirst(Transaction tx) throws IOException {
        long offset = getRoot(tx).getNext();
        if (offset != EntryLocation.NOT_SET) {
            return loadEntry(tx, offset);
        }
        return null;
    }

    EntryLocation getLast(Transaction tx) throws IOException {
        if (this.lastId != EntryLocation.NOT_SET) {
            return loadEntry(tx, this.lastId);
        }
        return null;
    }

    private boolean doRemove(Transaction tx, EntryLocation entry) throws IOException {
        boolean result = false;
        if (entry != null) {

            EntryLocation prev = getPrevious(tx, entry.getPrev());
            EntryLocation next = getNext(tx, entry.getNext());
            long prevId = prev != null ? prev.getPage().getPageId() : this.rootId;
            long nextId = next != null ? next.getPage().getPageId() : EntryLocation.NOT_SET;

            if (next != null) {
                next.setPrev(prevId);
                storeEntry(tx, next);
            } else {
                // we are deleting the last one in the list
                this.lastId = prevId;
            }
            if (prev != null) {
                prev.setNext(nextId);
                storeEntry(tx, prev);
            }

            entry.reset();
            storeEntry(tx, entry);
            tx.free(entry.getPage().getPageId());
            result = true;
            this.size--;
        }
        return result;
    }

    private EntryLocation createEntry(Transaction tx, String id, long previous, long next) throws IOException {
        Page<EntryLocation> p = tx.allocate();
        EntryLocation result = new EntryLocation();
        result.setPage(p);
        p.set(result);
        result.setId(id);
        result.setPrev(previous);
        result.setNext(next);
        return result;
    }

    private EntryLocation createEntry(Page<EntryLocation> p, String id, long previous, long next) throws IOException {
        EntryLocation result = new EntryLocation();
        result.setPage(p);
        p.set(result);
        result.setId(id);
        result.setPrev(previous);
        result.setNext(next);
        return result;
    }

    EntryLocation loadEntry(Transaction tx, long pageId) throws IOException {
        Page<EntryLocation> page = tx.load(pageId, EntryLocationMarshaller.INSTANCE);
        EntryLocation entry = page.get();
        if (entry != null) {
            entry.setPage(page);
        }
        return entry;
    }
    
    private void storeEntry(Transaction tx, EntryLocation entry) throws IOException {
        tx.store(entry.getPage(), EntryLocationMarshaller.INSTANCE, true);
    }

    EntryLocation getNext(Transaction tx, long next) throws IOException {
        EntryLocation result = null;
        if (next != EntryLocation.NOT_SET) {
            result = loadEntry(tx, next);
        }
        return result;
    }

    private EntryLocation getPrevious(Transaction tx, long previous) throws IOException {
        EntryLocation result = null;
        if (previous != EntryLocation.NOT_SET) {
            result = loadEntry(tx, previous);
        }
        return result;
    }

    private EntryLocation getRoot(Transaction tx) throws IOException {
        EntryLocation result = loadEntry(tx, this.rootId);
        return result;
    }

    ByteSequence getPayload(EntryLocation entry) throws IOException {
        return this.store.getPayload(entry.getLocation());
    }
}
