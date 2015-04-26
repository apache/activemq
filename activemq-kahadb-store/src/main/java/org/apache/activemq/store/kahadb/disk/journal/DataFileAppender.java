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
package org.apache.activemq.store.kahadb.disk.journal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.util.DataByteArrayOutputStream;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 */
class DataFileAppender implements FileAppender {

    private static final Logger logger = LoggerFactory.getLogger(DataFileAppender.class);

    protected final Journal journal;
    protected final Map<Journal.WriteKey, Journal.WriteCommand> inflightWrites;
    protected final Object enqueueMutex = new Object();
    protected WriteBatch nextWriteBatch;

    protected boolean shutdown;
    protected IOException firstAsyncException;
    protected final CountDownLatch shutdownDone = new CountDownLatch(1);
    protected int maxWriteBatchSize;
    protected final boolean syncOnComplete;

    protected boolean running;
    private Thread thread;

    public static class WriteKey {
        private final int file;
        private final long offset;
        private final int hash;

        public WriteKey(Location item) {
            file = item.getDataFileId();
            offset = item.getOffset();
            // TODO: see if we can build a better hash
            hash = (int)(file ^ offset);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof WriteKey) {
                WriteKey di = (WriteKey)obj;
                return di.file == file && di.offset == offset;
            }
            return false;
        }
    }

    public class WriteBatch {

        public final DataFile dataFile;

        public final LinkedNodeList<Journal.WriteCommand> writes = new LinkedNodeList<Journal.WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
        protected final int offset;
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;
        public AtomicReference<IOException> exception = new AtomicReference<IOException>();

        public WriteBatch(DataFile dataFile,int offset) {
            this.dataFile = dataFile;
            this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size=Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
        }

        public WriteBatch(DataFile dataFile, int offset, Journal.WriteCommand write) throws IOException {
            this(dataFile, offset);
            append(write);
        }

        public boolean canAppend(Journal.WriteCommand write) {
            int newSize = size + write.location.getSize();
            if (newSize >= maxWriteBatchSize || offset+newSize > journal.getMaxFileLength() ) {
                return false;
            }
            return true;
        }

        public void append(Journal.WriteCommand write) throws IOException {
            this.writes.addLast(write);
            write.location.setDataFileId(dataFile.getDataFileId());
            write.location.setOffset(offset+size);
            int s = write.location.getSize();
            size += s;
            dataFile.incrementLength(s);
            journal.addToTotalLength(s);
        }
    }

    /**
     * Construct a Store writer
     */
    public DataFileAppender(Journal dataManager) {
        this.journal = dataManager;
        this.inflightWrites = this.journal.getInflightWrites();
        this.maxWriteBatchSize = this.journal.getWriteBatchSize();
        this.syncOnComplete = this.journal.isEnableAsyncDiskSync();
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException {

        // Write the packet our internal buffer.
        int size = data.getLength() + Journal.RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, sync);

        WriteBatch batch = enqueue(write);
        location.setLatch(batch.latch);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            IOException exception = batch.exception.get();
            if (exception != null) {
                throw exception;
            }
        }

        return location;
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException {
        // Write the packet our internal buffer.
        int size = data.getLength() + Journal.RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, onComplete);

        WriteBatch batch = enqueue(write);

        location.setLatch(batch.latch);
        return location;
    }

    private WriteBatch enqueue(Journal.WriteCommand write) throws IOException {
        synchronized (enqueueMutex) {
            if (shutdown) {
                throw new IOException("Async Writer Thread Shutdown");
            }

            if (!running) {
                running = true;
                thread = new Thread() {
                    @Override
                    public void run() {
                        processQueue();
                    }
                };
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setDaemon(true);
                thread.setName("ActiveMQ Data File Writer");
                thread.start();
                firstAsyncException = null;
            }

            if (firstAsyncException != null) {
                throw firstAsyncException;
            }

            while ( true ) {
                if (nextWriteBatch == null) {
                    DataFile file = journal.getCurrentWriteFile();
                    if( file.getLength() + write.location.getSize() >= journal.getMaxFileLength() ) {
                        file = journal.rotateWriteFile();
                    }


                    nextWriteBatch = newWriteBatch(write, file);
                    enqueueMutex.notifyAll();
                    break;
                } else {
                    // Append to current batch if possible..
                    if (nextWriteBatch.canAppend(write)) {
                        nextWriteBatch.append(write);
                        break;
                    } else {
                        // Otherwise wait for the queuedCommand to be null
                        try {
                            while (nextWriteBatch != null) {
                                final long start = System.currentTimeMillis();
                                enqueueMutex.wait();
                                if (maxStat > 0) {
                                    logger.info("Waiting for write to finish with full batch... millis: " +
                                                (System.currentTimeMillis() - start));
                               }
                            }
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException();
                        }
                        if (shutdown) {
                            throw new IOException("Async Writer Thread Shutdown");
                        }
                    }
                }
            }
            if (!write.sync) {
                inflightWrites.put(new Journal.WriteKey(write.location), write);
            }
            return nextWriteBatch;
        }
    }

    protected WriteBatch newWriteBatch(Journal.WriteCommand write, DataFile file) throws IOException {
        return new WriteBatch(file, file.getLength(), write);
    }

    @Override
    public void close() throws IOException {
        synchronized (enqueueMutex) {
            if (!shutdown) {
                shutdown = true;
                if (running) {
                    enqueueMutex.notifyAll();
                } else {
                    shutdownDone.countDown();
                }
            }
        }

        try {
            shutdownDone.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

    }

    int statIdx = 0;
    int[] stats = new int[maxStat];
    final byte[] end = new byte[]{0};
    /**
     * The async processing loop that writes to the data files and does the
     * force calls. Since the file sync() call is the slowest of all the
     * operations, this algorithm tries to 'batch' or group together several
     * file sync() requests into a single file sync() call. The batching is
     * accomplished attaching the same CountDownLatch instance to every force
     * request in a group.
     */
    protected void processQueue() {
        DataFile dataFile = null;
        RecoverableRandomAccessFile file = null;
        WriteBatch wb = null;
        try {

            DataByteArrayOutputStream buff = new DataByteArrayOutputStream(maxWriteBatchSize);
            while (true) {

                // Block till we get a command.
                synchronized (enqueueMutex) {
                    while (true) {
                        if (nextWriteBatch != null) {
                            wb = nextWriteBatch;
                            nextWriteBatch = null;
                            break;
                        }
                        if (shutdown) {
                            return;
                        }
                        enqueueMutex.wait();
                    }
                    enqueueMutex.notifyAll();
                }

                if (dataFile != wb.dataFile) {
                    if (file != null) {
                        dataFile.closeRandomAccessFile(file);
                    }
                    dataFile = wb.dataFile;
                    file = dataFile.openRandomAccessFile();
                    // pre allocate on first open of new file (length==0)
                    // note dataFile.length cannot be used because it is updated in enqueue
                    if (file.length() == 0l) {
                        journal.preallocateEntireJournalDataFile(file);
                    }
                }

                Journal.WriteCommand write = wb.writes.getHead();

                // Write an empty batch control record.
                buff.reset();
                buff.writeInt(Journal.BATCH_CONTROL_RECORD_SIZE);
                buff.writeByte(Journal.BATCH_CONTROL_RECORD_TYPE);
                buff.write(Journal.BATCH_CONTROL_RECORD_MAGIC);
                buff.writeInt(0);
                buff.writeLong(0);

                boolean forceToDisk = false;
                while (write != null) {
                    forceToDisk |= write.sync | (syncOnComplete && write.onComplete != null);
                    buff.writeInt(write.location.getSize());
                    buff.writeByte(write.location.getType());
                    buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                    write = write.getNext();
                }

                // append 'unset' next batch (5 bytes) so read can always find eof
                buff.writeInt(0);
                buff.writeByte(0);

                ByteSequence sequence = buff.toByteSequence();

                // Now we can fill in the batch control record properly.
                buff.reset();
                buff.skip(5+Journal.BATCH_CONTROL_RECORD_MAGIC.length);
                buff.writeInt(sequence.getLength()-Journal.BATCH_CONTROL_RECORD_SIZE - 5);
                if( journal.isChecksum() ) {
                    Checksum checksum = new Adler32();
                    checksum.update(sequence.getData(), sequence.getOffset()+Journal.BATCH_CONTROL_RECORD_SIZE, sequence.getLength()-Journal.BATCH_CONTROL_RECORD_SIZE - 5);
                    buff.writeLong(checksum.getValue());
                }

                // Now do the 1 big write.
                file.seek(wb.offset);
                if (maxStat > 0) {
                    if (statIdx < maxStat) {
                        stats[statIdx++] = sequence.getLength();
                    } else {
                        long all = 0;
                        for (;statIdx > 0;) {
                            all+= stats[--statIdx];
                        }
                        logger.info("Ave writeSize: " + all/maxStat);
                    }
                }
                file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());

                ReplicationTarget replicationTarget = journal.getReplicationTarget();
                if( replicationTarget!=null ) {
                    replicationTarget.replicate(wb.writes.getHead().location, sequence, forceToDisk);
                }

                if (forceToDisk) {
                    file.sync();
                }

                Journal.WriteCommand lastWrite = wb.writes.getTail();
                journal.setLastAppendLocation(lastWrite.location);

                signalDone(wb);
            }
        } catch (IOException e) {
            logger.info("Journal failed while writing at: " + wb.offset);
            synchronized (enqueueMutex) {
                firstAsyncException = e;
                if (wb != null) {
                    wb.exception.set(e);
                    wb.latch.countDown();
                }
                if (nextWriteBatch != null) {
                    nextWriteBatch.exception.set(e);
                    nextWriteBatch.latch.countDown();
                }
            }
        } catch (InterruptedException e) {
        } finally {
            try {
                if (file != null) {
                    dataFile.closeRandomAccessFile(file);
                }
            } catch (Throwable ignore) {
            }
            shutdownDone.countDown();
            running = false;
        }
    }

    protected void signalDone(WriteBatch wb) {
        // Now that the data is on disk, remove the writes from the in
        // flight
        // cache.
        Journal.WriteCommand write = wb.writes.getHead();
        while (write != null) {
            if (!write.sync) {
                inflightWrites.remove(new Journal.WriteKey(write.location));
            }
            if (write.onComplete != null) {
                try {
                    write.onComplete.run();
                } catch (Throwable e) {
                    logger.info("Add exception was raised while executing the run command for onComplete", e);
                }
            }
            write = write.getNext();
        }

        // Signal any waiting threads that the write is on disk.
        wb.latch.countDown();
    }
}
