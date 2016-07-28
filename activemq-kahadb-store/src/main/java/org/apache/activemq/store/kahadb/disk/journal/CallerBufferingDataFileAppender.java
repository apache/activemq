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
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.util.DataByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 * The thread calling enqueue does the file open and buffering of the data, which
 * reduces the round trip of the write thread.
 *
 */
class CallerBufferingDataFileAppender extends DataFileAppender {

    private static final Logger logger = LoggerFactory.getLogger(CallerBufferingDataFileAppender.class);

    final DataByteArrayOutputStream cachedBuffers[] = new DataByteArrayOutputStream[] {
            new DataByteArrayOutputStream(maxWriteBatchSize),
            new DataByteArrayOutputStream(maxWriteBatchSize)
    };
    volatile byte flip = 0x1;
    public class WriteBatch extends DataFileAppender.WriteBatch {

        DataByteArrayOutputStream buff = cachedBuffers[flip ^= 1];
        private boolean forceToDisk;
        public WriteBatch(DataFile dataFile, int offset, Journal.WriteCommand write) throws IOException {
            super(dataFile, offset);
            initBuffer(buff);
            append(write);
        }

        @Override
        public void append(Journal.WriteCommand write) throws IOException {
            super.append(write);
            forceToDisk |= appendToBuffer(write, buff);
        }
    }

    @Override
    protected DataFileAppender.WriteBatch newWriteBatch(Journal.WriteCommand write, DataFile file) throws IOException {
        return new WriteBatch(file, file.getLength(), write);
    }

    private void initBuffer(DataByteArrayOutputStream buff) throws IOException {
        // Write an empty batch control record.
        buff.reset();
        buff.write(Journal.BATCH_CONTROL_RECORD_HEADER);
        buff.writeInt(0);
        buff.writeLong(0);
    }

    public CallerBufferingDataFileAppender(Journal dataManager) {
        super(dataManager);
    }

    /**
     * The async processing loop that writes to the data files and does the
     * force calls. Since the file sync() call is the slowest of all the
     * operations, this algorithm tries to 'batch' or group together several
     * file sync() requests into a single file sync() call. The batching is
     * accomplished attaching the same CountDownLatch instance to every force
     * request in a group.
     */
    @Override
    protected void processQueue() {
        DataFile dataFile = null;
        RecoverableRandomAccessFile file = null;
        WriteBatch wb = null;
        try {

            while (true) {

                Object o = null;

                // Block till we get a command.
                synchronized (enqueueMutex) {
                    while (true) {
                        if (nextWriteBatch != null) {
                            o = nextWriteBatch;
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

                wb = (WriteBatch)o;
                if (dataFile != wb.dataFile) {
                    if (file != null) {
                        if (periodicSync) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Syning file {} on rotate", dataFile.getFile().getName());
                            }
                            file.sync();
                        }
                        dataFile.closeRandomAccessFile(file);
                    }
                    dataFile = wb.dataFile;
                    file = dataFile.openRandomAccessFile();
                }

                final DataByteArrayOutputStream buff = wb.buff;
                final boolean forceToDisk = wb.forceToDisk;

                ByteSequence sequence = buff.toByteSequence();

                // Now we can fill in the batch control record properly.
                buff.reset();
                buff.skip(5+Journal.BATCH_CONTROL_RECORD_MAGIC.length);
                buff.writeInt(sequence.getLength()-Journal.BATCH_CONTROL_RECORD_SIZE);
                if( journal.isChecksum() ) {
                    Checksum checksum = new Adler32();
                    checksum.update(sequence.getData(), sequence.getOffset()+Journal.BATCH_CONTROL_RECORD_SIZE, sequence.getLength()-Journal.BATCH_CONTROL_RECORD_SIZE);
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
                        System.err.println("Ave writeSize: " + all/maxStat);
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
                    if (periodicSync) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Syning file {} on close", dataFile.getFile().getName());
                        }
                        file.sync();
                    }
                    dataFile.closeRandomAccessFile(file);
                }
            } catch (Throwable ignore) {
            }
            shutdownDone.countDown();
            running = false;
        }
    }

    private boolean appendToBuffer(Journal.WriteCommand write, DataByteArrayOutputStream buff) throws IOException {
        buff.writeInt(write.location.getSize());
        buff.writeByte(write.location.getType());
        buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
        return write.sync | (syncOnComplete && write.onComplete != null);
    }
}
