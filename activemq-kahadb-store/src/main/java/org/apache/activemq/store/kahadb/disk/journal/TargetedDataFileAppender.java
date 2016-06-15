/*
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
import java.util.Map;
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
 * File Appender instance that performs batched writes in the thread where the write is
 * queued.  This appender does not honor the maxFileLength value in the journal as the
 * files created here are out-of-band logs used for other purposes such as journal level
 * compaction.
 */
public class TargetedDataFileAppender implements FileAppender {

    private static final Logger LOG = LoggerFactory.getLogger(TargetedDataFileAppender.class);

    private final Journal journal;
    private final DataFile target;
    private final Map<Journal.WriteKey, Journal.WriteCommand> inflightWrites;
    private final int maxWriteBatchSize;

    private boolean closed;
    private boolean preallocate;
    private WriteBatch nextWriteBatch;
    private int statIdx = 0;
    private int[] stats = new int[maxStat];

    public class WriteBatch {

        protected final int offset;

        public final DataFile dataFile;
        public final LinkedNodeList<Journal.WriteCommand> writes = new LinkedNodeList<Journal.WriteCommand>();
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;
        public AtomicReference<IOException> exception = new AtomicReference<IOException>();

        public WriteBatch(DataFile dataFile, int offset) {
            this.dataFile = dataFile;
            this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size = Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
        }

        public WriteBatch(DataFile dataFile, int offset, Journal.WriteCommand write) throws IOException {
            this(dataFile, offset);
            append(write);
        }

        public boolean canAppend(Journal.WriteCommand write) {
            int newSize = size + write.location.getSize();
            if (newSize >= maxWriteBatchSize) {
                return false;
            }
            return true;
        }

        public void append(Journal.WriteCommand write) throws IOException {
            this.writes.addLast(write);
            write.location.setDataFileId(dataFile.getDataFileId());
            write.location.setOffset(offset + size);
            int s = write.location.getSize();
            size += s;
            dataFile.incrementLength(s);
            journal.addToTotalLength(s);
        }
    }

    /**
     * Construct a Store writer
     */
    public TargetedDataFileAppender(Journal journal, DataFile target) {
        this.journal = journal;
        this.target = target;
        this.inflightWrites = this.journal.getInflightWrites();
        this.maxWriteBatchSize = this.journal.getWriteBatchSize();
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException {
        checkClosed();

        // Write the packet our internal buffer.
        int size = data.getLength() + Journal.RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, sync);

        enqueueWrite(write);

        if (sync) {
            writePendingBatch();
        }

        return location;
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException {
        checkClosed();

        // Write the packet our internal buffer.
        int size = data.getLength() + Journal.RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, onComplete);

        enqueueWrite(write);

        return location;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (nextWriteBatch != null) {
                // force sync of current in-progress batched write.
                LOG.debug("Close of targeted appender flushing last batch.");
                writePendingBatch();
            }

            closed = true;
        }
    }

    //----- Appender Configuration -------------------------------------------//

    public boolean isPreallocate() {
        return preallocate;
    }

    public void setPreallocate(boolean preallocate) {
        this.preallocate = preallocate;
    }

    //----- Internal Implementation ------------------------------------------//

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("The appender is clsoed");
        }
    }

    private WriteBatch enqueueWrite(Journal.WriteCommand write) throws IOException {
        while (true) {
            if (nextWriteBatch == null) {
                nextWriteBatch = new WriteBatch(target, target.getLength(), write);
                break;
            } else {
                // Append to current batch if possible..
                if (nextWriteBatch.canAppend(write)) {
                    nextWriteBatch.append(write);
                    break;
                } else {
                    // Flush current batch and start a new one.
                    writePendingBatch();
                    nextWriteBatch = null;
                }
            }
        }

        if (!write.sync) {
            inflightWrites.put(new Journal.WriteKey(write.location), write);
        }

        return nextWriteBatch;
    }

    private void writePendingBatch() throws IOException {
        DataFile dataFile = nextWriteBatch.dataFile;

        try (RecoverableRandomAccessFile file = dataFile.openRandomAccessFile();
             DataByteArrayOutputStream buff = new DataByteArrayOutputStream(maxWriteBatchSize);) {

            // preallocate on first open of new file (length == 0) if configured to do so.
            // NOTE: dataFile.length cannot be used because it is updated in enqueue
            if (file.length() == 0L && isPreallocate()) {
                journal.preallocateEntireJournalDataFile(file);
            }

            Journal.WriteCommand write = nextWriteBatch.writes.getHead();

            // Write an empty batch control record.
            buff.reset();
            buff.writeInt(Journal.BATCH_CONTROL_RECORD_SIZE);
            buff.writeByte(Journal.BATCH_CONTROL_RECORD_TYPE);
            buff.write(Journal.BATCH_CONTROL_RECORD_MAGIC);
            buff.writeInt(0);
            buff.writeLong(0);

            while (write != null) {
                buff.writeInt(write.location.getSize());
                buff.writeByte(write.location.getType());
                buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                write = write.getNext();
            }

            // append 'unset' next batch (5 bytes) so read can always find eof
            buff.write(Journal.EOF_RECORD);
            ByteSequence sequence = buff.toByteSequence();

            // Now we can fill in the batch control record properly.
            buff.reset();
            buff.skip(5 + Journal.BATCH_CONTROL_RECORD_MAGIC.length);
            buff.writeInt(sequence.getLength() - Journal.BATCH_CONTROL_RECORD_SIZE - Journal.EOF_RECORD.length);
            if (journal.isChecksum()) {
                Checksum checksum = new Adler32();
                checksum.update(sequence.getData(),
                                sequence.getOffset() + Journal.BATCH_CONTROL_RECORD_SIZE,
                                sequence.getLength() - Journal.BATCH_CONTROL_RECORD_SIZE - Journal.EOF_RECORD.length);
                buff.writeLong(checksum.getValue());
            }

            // Now do the 1 big write.
            file.seek(nextWriteBatch.offset);
            if (maxStat > 0) {
                if (statIdx < maxStat) {
                    stats[statIdx++] = sequence.getLength();
                } else {
                    long all = 0;
                    for (; statIdx > 0;) {
                        all += stats[--statIdx];
                    }
                    LOG.trace("Ave writeSize: {}", all / maxStat);
                }
            }

            file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());

            ReplicationTarget replicationTarget = journal.getReplicationTarget();
            if (replicationTarget != null) {
                replicationTarget.replicate(nextWriteBatch.writes.getHead().location, sequence, true);
            }

            file.sync();

            signalDone(nextWriteBatch);
        } catch (IOException e) {
            LOG.info("Journal failed while writing at: {}", nextWriteBatch.offset);
            throw e;
        }
    }

    private void signalDone(WriteBatch writeBatch) {
        // Now that the data is on disk, remove the writes from the in
        // flight cache and signal any onComplete requests.
        Journal.WriteCommand write = writeBatch.writes.getHead();
        while (write != null) {
            if (!write.sync) {
                inflightWrites.remove(new Journal.WriteKey(write.location));
            }

            if (write.onComplete != null) {
                try {
                    write.onComplete.run();
                } catch (Throwable e) {
                    LOG.info("Add exception was raised while executing the run command for onComplete", e);
                }
            }

            write = write.getNext();
        }
    }
}
