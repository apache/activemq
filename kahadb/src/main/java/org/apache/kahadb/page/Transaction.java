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
package org.apache.kahadb.page;

import java.io.*;
import java.util.*;

import org.apache.kahadb.page.PageFile.PageWrite;
import org.apache.kahadb.util.*;

/**
 * The class used to read/update a PageFile object.  Using a transaction allows you to
 * do multiple update operations in a single unit of work.
 */
public class Transaction implements Iterable<Page> {

    private RandomAccessFile tmpFile;
    private File txFile;
    private int nextLocation = 0;

    /**
     * The PageOverflowIOException occurs when a page write is requested
     * and it's data is larger than what would fit into a single page.
     */
    public class PageOverflowIOException extends IOException {
        public PageOverflowIOException(String message) {
            super(message);
        }
    }

    /**
     * The InvalidPageIOException is thrown if try to load/store a a page
     * with an invalid page id.
     */
    public class InvalidPageIOException extends IOException {
        private final long page;

        public InvalidPageIOException(String message, long page) {
            super(message);
            this.page = page;
        }

        public long getPage() {
            return page;
        }
    }

    /**
     * This closure interface is intended for the end user implement callbacks for the Transaction.exectue() method.
     *
     * @param <T> The type of exceptions that operation will throw.
     */
    public interface Closure <T extends Throwable> {
        public void execute(Transaction tx) throws T;
    }

    /**
     * This closure interface is intended for the end user implement callbacks for the Transaction.exectue() method.
     *
     * @param <R> The type of result that the closure produces.
     * @param <T> The type of exceptions that operation will throw.
     */
    public interface CallableClosure<R, T extends Throwable> {
        public R execute(Transaction tx) throws T;
    }


    // The page file that this Transaction operates against.
    private final PageFile pageFile;
    // If this transaction is updating stuff.. this is the tx of
    private long writeTransactionId=-1;
    // List of pages that this transaction has modified.
    private TreeMap<Long, PageWrite> writes=new TreeMap<Long, PageWrite>();
    // List of pages allocated in this transaction
    private final SequenceSet allocateList = new SequenceSet();
    // List of pages freed in this transaction
    private final SequenceSet freeList = new SequenceSet();

    private long maxTransactionSize = Long.parseLong(System.getProperty("maxKahaDBTxSize", "" + 10485760));

    private long size = 0;

    Transaction(PageFile pageFile) {
        this.pageFile = pageFile;
    }

    /**
     * @return the page file that created this Transaction
     */
    public PageFile getPageFile() {
        return this.pageFile;
    }

    /**
     * Allocates a free page that you can write data to.
     *
     * @return a newly allocated page.
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> Page<T> allocate() throws IOException {
        return allocate(1);
    }

    /**
     * Allocates a block of free pages that you can write data to.
     *
     * @param count the number of sequential pages to allocate
     * @return the first page of the sequential set.
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> Page<T> allocate(int count) throws IOException {
        Page<T> rc = pageFile.allocate(count);
        allocateList.add(new Sequence(rc.getPageId(), rc.getPageId()+count-1));
        return rc;
    }

    /**
     * Frees up a previously allocated page so that it can be re-allocated again.
     *
     * @param pageId the page to free up
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public void free(long pageId) throws IOException {
        free(load(pageId, null));
    }

    /**
     * Frees up a previously allocated sequence of pages so that it can be re-allocated again.
     *
     * @param pageId the initial page of the sequence that will be getting freed
     * @param count the number of pages in the sequence
     *
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public void free(long pageId, int count) throws IOException {
        free(load(pageId, null), count);
    }

    /**
     * Frees up a previously allocated sequence of pages so that it can be re-allocated again.
     *
     * @param page the initial page of the sequence that will be getting freed
     * @param count the number of pages in the sequence
     *
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> void free(Page<T> page, int count) throws IOException {
        pageFile.assertLoaded();
        long offsetPage = page.getPageId();
        for (int i = 0; i < count; i++) {
            if (page == null) {
                page = load(offsetPage + i, null);
            }
            free(page);
            page = null;
        }
    }

    /**
     * Frees up a previously allocated page so that it can be re-allocated again.
     *
     * @param page the page to free up
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> void free(Page<T> page) throws IOException {
        pageFile.assertLoaded();

        // We may need loop to free up a page chain.
        while (page != null) {

            // Is it already free??
            if (page.getType() == Page.PAGE_FREE_TYPE) {
                return;
            }

            Page<T> next = null;
            if (page.getType() == Page.PAGE_PART_TYPE) {
                next = load(page.getNext(), null);
            }

            page.makeFree(getWriteTransactionId());
            // ensure free page is visible while write is pending
            pageFile.addToCache(page.copy());

            DataByteArrayOutputStream out = new DataByteArrayOutputStream(pageFile.getPageSize());
            page.write(out);
            write(page, out.getData());

            freeList.add(page.getPageId());
            page = next;
        }
    }

    /**
     *
     * @param page
     *        the page to write. The Page object must be fully populated with a valid pageId, type, and data.
     * @param marshaller
     *        the marshaler to use to load the data portion of the Page, may be null if you do not wish to write the data.
     * @param overflow
     *        If true, then if the page data marshalls to a bigger size than can fit in one page, then additional
     *        overflow pages are automatically allocated and chained to this page to store all the data.  If false,
     *        and the overflow condition would occur, then the PageOverflowIOException is thrown.
     * @throws IOException
     *         If an disk error occurred.
     * @throws PageOverflowIOException
     *         If the page data marshalls to size larger than maximum page size and overflow was false.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> void store(Page<T> page, Marshaller<T> marshaller, final boolean overflow) throws IOException {
        DataByteArrayOutputStream out = (DataByteArrayOutputStream)openOutputStream(page, overflow);
        if (marshaller != null) {
            marshaller.writePayload(page.get(), out);
        }
        out.close();
    }

    /**
     * @throws IOException
     */
    public OutputStream openOutputStream(Page page, final boolean overflow) throws IOException {
        pageFile.assertLoaded();

        // Copy to protect against the end user changing
        // the page instance while we are doing a write.
        final Page copy = page.copy();
        pageFile.addToCache(copy);

        //
        // To support writing VERY large data, we override the output stream so
        // that we
        // we do the page writes incrementally while the data is being
        // marshalled.
        DataByteArrayOutputStream out = new DataByteArrayOutputStream(pageFile.getPageSize() * 2) {
            Page current = copy;

            @SuppressWarnings("unchecked")
            @Override
            protected void onWrite() throws IOException {

                // Are we at an overflow condition?
                final int pageSize = pageFile.getPageSize();
                if (pos >= pageSize) {
                    // If overflow is allowed
                    if (overflow) {

                        Page next;
                        if (current.getType() == Page.PAGE_PART_TYPE) {
                            next = load(current.getNext(), null);
                        } else {
                            next = allocate();
                        }

                        next.txId = current.txId;

                        // Write the page header
                        int oldPos = pos;
                        pos = 0;

                        current.makePagePart(next.getPageId(), getWriteTransactionId());
                        current.write(this);

                        // Do the page write..
                        byte[] data = new byte[pageSize];
                        System.arraycopy(buf, 0, data, 0, pageSize);
                        Transaction.this.write(current, data);

                        // Reset for the next page chunk
                        pos = 0;
                        // The page header marshalled after the data is written.
                        skip(Page.PAGE_HEADER_SIZE);
                        // Move the overflow data after the header.
                        System.arraycopy(buf, pageSize, buf, pos, oldPos - pageSize);
                        pos += oldPos - pageSize;
                        current = next;

                    } else {
                        throw new PageOverflowIOException("Page overflow.");
                    }
                }

            }

            @SuppressWarnings("unchecked")
            @Override
            public void close() throws IOException {
                super.close();

                // We need to free up the rest of the page chain..
                if (current.getType() == Page.PAGE_PART_TYPE) {
                    free(current.getNext());
                }

                current.makePageEnd(pos, getWriteTransactionId());

                // Write the header..
                pos = 0;
                current.write(this);

                Transaction.this.write(current, buf);
            }
        };

        // The page header marshaled after the data is written.
        out.skip(Page.PAGE_HEADER_SIZE);
        return out;
    }

    /**
     * Loads a page from disk.
     *
     * @param pageId
     *        the id of the page to load
     * @param marshaller
     *        the marshaler to use to load the data portion of the Page, may be null if you do not wish to load the data.
     * @return The page with the given id
     * @throws IOException
     *         If an disk error occurred.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public <T> Page<T> load(long pageId, Marshaller<T> marshaller) throws IOException {
        pageFile.assertLoaded();
        Page<T> page = new Page<T>(pageId);
        load(page, marshaller);
        return page;
    }

    /**
     * Loads a page from disk.
     *
     * @param page - The pageId field must be properly set
     * @param marshaller
     *        the marshaler to use to load the data portion of the Page, may be null if you do not wish to load the data.
     * @throws IOException
     *         If an disk error occurred.
     * @throws InvalidPageIOException
     *         If the page is is not valid.
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    @SuppressWarnings("unchecked")
    public <T> void load(Page<T> page, Marshaller<T> marshaller) throws IOException {
        pageFile.assertLoaded();

        // Can't load invalid offsets...
        long pageId = page.getPageId();
        if (pageId < 0) {
            throw new InvalidPageIOException("Page id is not valid", pageId);
        }

        // It might be a page this transaction has modified...
        PageWrite update = writes.get(pageId);
        if (update != null) {
            page.copy(update.getPage());
            return;
        }

        // We may be able to get it from the cache...
        Page<T> t = pageFile.getFromCache(pageId);
        if (t != null) {
            page.copy(t);
            return;
        }

        if (marshaller != null) {
            // Full page read..
            InputStream is = openInputStream(page);
            DataInputStream dataIn = new DataInputStream(is);
            page.set(marshaller.readPayload(dataIn));
            is.close();
        } else {
            // Page header read.
            DataByteArrayInputStream in = new DataByteArrayInputStream(new byte[Page.PAGE_HEADER_SIZE]);
            pageFile.readPage(pageId, in.getRawData());
            page.read(in);
            page.set(null);
        }

        // Cache it.
        if (marshaller != null) {
            pageFile.addToCache(page);
        }
    }

    /**
     * @see org.apache.kahadb.page.Transaction#load(org.apache.kahadb.page.Page,
     *      org.apache.kahadb.util.Marshaller)
     */
    public InputStream openInputStream(final Page p) throws IOException {

        return new InputStream() {

            private ByteSequence chunk = new ByteSequence(new byte[pageFile.getPageSize()]);
            private Page page = readPage(p);
            private int pageCount = 1;

            private Page markPage;
            private ByteSequence markChunk;

            private Page readPage(Page page) throws IOException {
                // Read the page data

                pageFile.readPage(page.getPageId(), chunk.getData());

                chunk.setOffset(0);
                chunk.setLength(pageFile.getPageSize());

                DataByteArrayInputStream in = new DataByteArrayInputStream(chunk);
                page.read(in);

                chunk.setOffset(Page.PAGE_HEADER_SIZE);
                if (page.getType() == Page.PAGE_END_TYPE) {
                    chunk.setLength((int)(page.getNext()));
                }

                if (page.getType() == Page.PAGE_FREE_TYPE) {
                    throw new EOFException("Chunk stream does not exist, page: " + page.getPageId() + " is marked free");
                }

                return page;
            }

            public int read() throws IOException {
                if (!atEOF()) {
                    return chunk.data[chunk.offset++] & 0xff;
                } else {
                    return -1;
                }
            }

            private boolean atEOF() throws IOException {
                if (chunk.offset < chunk.length) {
                    return false;
                }
                if (page.getType() == Page.PAGE_END_TYPE) {
                    return true;
                }
                fill();
                return chunk.offset >= chunk.length;
            }

            private void fill() throws IOException {
                page = readPage(new Page(page.getNext()));
                pageCount++;
            }

            public int read(byte[] b) throws IOException {
                return read(b, 0, b.length);
            }

            public int read(byte b[], int off, int len) throws IOException {
                if (!atEOF()) {
                    int rc = 0;
                    while (!atEOF() && rc < len) {
                        len = Math.min(len, chunk.length - chunk.offset);
                        if (len > 0) {
                            System.arraycopy(chunk.data, chunk.offset, b, off, len);
                            chunk.offset += len;
                        }
                        rc += len;
                    }
                    return rc;
                } else {
                    return -1;
                }
            }

            public long skip(long len) throws IOException {
                if (atEOF()) {
                    int rc = 0;
                    while (!atEOF() && rc < len) {
                        len = Math.min(len, chunk.length - chunk.offset);
                        if (len > 0) {
                            chunk.offset += len;
                        }
                        rc += len;
                    }
                    return rc;
                } else {
                    return -1;
                }
            }

            public int available() {
                return chunk.length - chunk.offset;
            }

            public boolean markSupported() {
                return true;
            }

            public void mark(int markpos) {
                markPage = page;
                byte data[] = new byte[pageFile.getPageSize()];
                System.arraycopy(chunk.getData(), 0, data, 0, pageFile.getPageSize());
                markChunk = new ByteSequence(data, chunk.getOffset(), chunk.getLength());
            }

            public void reset() {
                page = markPage;
                chunk = markChunk;
            }

        };
    }

    /**
     * Allows you to iterate through all active Pages in this object.  Pages with type Page.FREE_TYPE are
     * not included in this iteration.
     *
     * Pages removed with Iterator.remove() will not actually get removed until the transaction commits.
     *
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    @SuppressWarnings("unchecked")
    public Iterator<Page> iterator() {
        return (Iterator<Page>)iterator(false);
    }

    /**
     * Allows you to iterate through all active Pages in this object.  You can optionally include free pages in the pages
     * iterated.
     *
     * @param includeFreePages - if true, free pages are included in the iteration
     * @throws IllegalStateException
     *         if the PageFile is not loaded
     */
    public Iterator<Page> iterator(final boolean includeFreePages) {

        pageFile.assertLoaded();

        return new Iterator<Page>() {
            long nextId;
            Page nextPage;
            Page lastPage;

            private void findNextPage() {
                if (!pageFile.isLoaded()) {
                    throw new IllegalStateException("Cannot iterate the pages when the page file is not loaded");
                }

                if (nextPage != null) {
                    return;
                }

                try {
                    while (nextId < pageFile.getPageCount()) {

                        Page page = load(nextId, null);

                        if (includeFreePages || page.getType() != Page.PAGE_FREE_TYPE) {
                            nextPage = page;
                            return;
                        } else {
                            nextId++;
                        }
                    }
                } catch (IOException e) {
                }
            }

            public boolean hasNext() {
                findNextPage();
                return nextPage != null;
            }

            public Page next() {
                findNextPage();
                if (nextPage != null) {
                    lastPage = nextPage;
                    nextPage = null;
                    nextId++;
                    return lastPage;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @SuppressWarnings("unchecked")
            public void remove() {
                if (lastPage == null) {
                    throw new IllegalStateException();
                }
                try {
                    free(lastPage);
                    lastPage = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    ///////////////////////////////////////////////////////////////////
    // Commit / Rollback related methods..
    ///////////////////////////////////////////////////////////////////

    /**
     * Commits the transaction to the PageFile as a single 'Unit of Work'. Either all page updates associated
     * with the transaction are written to disk or none will.
     */
    public void commit() throws IOException {
        if( writeTransactionId!=-1 ) {
            // Actually do the page writes...
            pageFile.write(writes.entrySet());
            // Release the pages that were freed up in the transaction..
            freePages(freeList);

            freeList.clear();
            allocateList.clear();
            writes.clear();
            writeTransactionId = -1;
            if (tmpFile != null) {
                tmpFile.close();
                if (!getTempFile().delete()) {
                    throw new IOException("Can't delete temporary KahaDB transaction file:"  + getTempFile());
                }
                tmpFile = null;
                txFile = null;
            }
        }
        size = 0;
    }

    /**
     * Rolls back the transaction.
     */
    public void rollback() throws IOException {
        if( writeTransactionId!=-1 ) {
            // Release the pages that were allocated in the transaction...
            freePages(allocateList);

            freeList.clear();
            allocateList.clear();
            writes.clear();
            writeTransactionId = -1;
            if (tmpFile != null) {
                tmpFile.close();
                if (getTempFile().delete()) {
                    throw new IOException("Can't delete temporary KahaDB transaction file:"  + getTempFile());
                }
                tmpFile = null;
                txFile = null;
            }
        }
        size = 0;
    }

    private long getWriteTransactionId() {
        if( writeTransactionId==-1 ) {
            writeTransactionId = pageFile.getNextWriteTransactionId();
        }
        return writeTransactionId;
    }


    protected File getTempFile() {
        if (txFile == null) {
            txFile = new File(getPageFile().getDirectory(), IOHelper.toFileSystemSafeName("tx-"+ Long.toString(getWriteTransactionId()) + "-" + Long.toString(System.currentTimeMillis()) + ".tmp"));
        }
       return txFile;
    }

    /**
     * Queues up a page write that should get done when commit() gets called.
     */
    @SuppressWarnings("unchecked")
    private void write(final Page page, byte[] data) throws IOException {
        Long key = page.getPageId();
        size += data.length;

        PageWrite write;
        if (size > maxTransactionSize) {
            if (tmpFile == null) {
                tmpFile = new RandomAccessFile(getTempFile(), "rw");
            }
            int location = nextLocation;
            tmpFile.seek(nextLocation);
            tmpFile.write(data);
            nextLocation = location + data.length;
            write = new PageWrite(page, location, data.length, getTempFile());
        } else {
            write = new PageWrite(page, data);
        }
        writes.put(key, write);
    }

    /**
     * @param list
     * @throws RuntimeException
     */
    private void freePages(SequenceSet list) throws RuntimeException {
        Sequence seq = list.getHead();
        while( seq!=null ) {
            seq.each(new Sequence.Closure<RuntimeException>(){
                public void execute(long value) {
                    pageFile.freePage(value);
                }
            });
            seq = seq.getNext();
        }
    }

    /**
     * @return true if there are no uncommitted page file updates associated with this transaction.
     */
    public boolean isReadOnly() {
        return writeTransactionId==-1;
    }

    ///////////////////////////////////////////////////////////////////
    // Transaction closure helpers...
    ///////////////////////////////////////////////////////////////////

    /**
     * Executes a closure and if it does not throw any exceptions, then it commits the transaction.
     * If the closure throws an Exception, then the transaction is rolled back.
     *
     * @param <T>
     * @param closure - the work to get exectued.
     * @throws T if the closure throws it
     * @throws IOException If the commit fails.
     */
    public <T extends Throwable> void execute(Closure<T> closure) throws T, IOException {
        boolean success = false;
        try {
            closure.execute(this);
            success = true;
        } finally {
            if (success) {
                commit();
            } else {
                rollback();
            }
        }
    }

    /**
     * Executes a closure and if it does not throw any exceptions, then it commits the transaction.
     * If the closure throws an Exception, then the transaction is rolled back.
     *
     * @param <T>
     * @param closure - the work to get exectued.
     * @throws T if the closure throws it
     * @throws IOException If the commit fails.
     */
    public <R, T extends Throwable> R execute(CallableClosure<R, T> closure) throws T, IOException {
        boolean success = false;
        try {
            R rc = closure.execute(this);
            success = true;
            return rc;
        } finally {
            if (success) {
                commit();
            } else {
                rollback();
            }
        }
    }

}
