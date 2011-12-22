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
package org.apache.kahadb.journal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.LinkedNodeList;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 * The thread calling enqueue does the file open and buffering of the data, which
 * reduces the round trip of the write thread.
 * 
 */
class CallerBufferingDataFileAppender implements FileAppender {

    protected final Journal journal;
    protected final Map<Journal.WriteKey, Journal.WriteCommand> inflightWrites;
    protected final Object enqueueMutex = new Object() {
    };
    protected WriteBatch nextWriteBatch;

    protected boolean shutdown;
    protected IOException firstAsyncException;
    protected final CountDownLatch shutdownDone = new CountDownLatch(1);
    protected int maxWriteBatchSize;

    private boolean running;
    private Thread thread;

    final DataByteArrayOutputStream cachedBuffers[] = new DataByteArrayOutputStream[] {
            new DataByteArrayOutputStream(maxWriteBatchSize),
            new DataByteArrayOutputStream(maxWriteBatchSize)
    };
    AtomicInteger writeBatchInstanceCount = new AtomicInteger();
    public class WriteBatch {

        DataByteArrayOutputStream buff = cachedBuffers[writeBatchInstanceCount.getAndIncrement()%2];
        public final DataFile dataFile;

        public final LinkedNodeList<Journal.WriteCommand> writes = new LinkedNodeList<Journal.WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
		private final int offset;
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;
        public AtomicReference<IOException> exception = new AtomicReference<IOException>();
        public boolean forceToDisk;

        public WriteBatch(DataFile dataFile, int offset, Journal.WriteCommand write) throws IOException {
            this.dataFile = dataFile;
			this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size=Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            initBuffer(buff);
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
            forceToDisk |= appendToBuffer(write, buff);
        }
    }

    private void initBuffer(DataByteArrayOutputStream buff) throws IOException {
        // Write an empty batch control record.
        buff.reset();
        buff.write(Journal.BATCH_CONTROL_RECORD_HEADER);
        buff.writeInt(0);
        buff.writeLong(0);
    }

    /**
     * Construct a Store writer
     */
    public CallerBufferingDataFileAppender(Journal dataManager) {
        this.journal = dataManager;
        this.inflightWrites = this.journal.getInflightWrites();
        this.maxWriteBatchSize = this.journal.getWriteBatchSize();
    }

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
                throw new IOException("Async Writter Thread Shutdown");
            }
            
            if (!running) {
                running = true;
                thread = new Thread() {
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
	            	if( file.getLength() > journal.getMaxFileLength() ) {
	            		file = journal.rotateWriteFile();
	            	}

	                nextWriteBatch = new WriteBatch(file, file.getLength(), write);
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
	                                System.err.println("Watiting for write to finish with full batch... millis: " + (System.currentTimeMillis() - start));
	                            }
	                        }
	                    } catch (InterruptedException e) {
	                        throw new InterruptedIOException();
	                    }
	                    if (shutdown) {
	                        throw new IOException("Async Writter Thread Shutdown");
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

    public static final String PROPERTY_LOG_WRITE_STAT_WINDOW = "org.apache.kahadb.journal.appender.WRITE_STAT_WINDOW";
    public static final int maxStat = Integer.parseInt(System.getProperty(PROPERTY_LOG_WRITE_STAT_WINDOW, "0"));
    int statIdx = 0;
    int[] stats = new int[maxStat];
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
        RandomAccessFile file = null;
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
                        file.setLength(dataFile.getLength());
                        dataFile.closeRandomAccessFile(file);
                    }
                    dataFile = wb.dataFile;
                    file = dataFile.openRandomAccessFile();
                    if( file.length() < journal.preferedFileLength ) {
                        file.setLength(journal.preferedFileLength);
                    }
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
                    file.getFD().sync();
                }

                Journal.WriteCommand lastWrite = wb.writes.getTail();
                journal.setLastAppendLocation(lastWrite.location);

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
                            e.printStackTrace();
                        }
                    }
                    write = write.getNext();
                }

                // Signal any waiting threads that the write is on disk.
                wb.latch.countDown();
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
        return write.sync | write.onComplete != null;
    }
}
