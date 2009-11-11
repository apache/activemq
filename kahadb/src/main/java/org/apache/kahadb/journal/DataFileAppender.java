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
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayOutputStream;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 * 
 * @version $Revision$
 */
class DataFileAppender {

    protected final Journal journal;
    protected final Map<WriteKey, WriteCommand> inflightWrites;
    protected final Object enqueueMutex = new Object() {
    };
    protected WriteBatch nextWriteBatch;

    protected boolean shutdown;
    protected IOException firstAsyncException;
    protected final CountDownLatch shutdownDone = new CountDownLatch(1);
    protected int maxWriteBatchSize;

    private boolean running;
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

        public int hashCode() {
            return hash;
        }

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

        public final LinkedNodeList<WriteCommand> writes = new LinkedNodeList<WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
		private final int offset;
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;

        public WriteBatch(DataFile dataFile, int offset, WriteCommand write) throws IOException {
            this.dataFile = dataFile;
			this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size=Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            append(write);
        }

        public boolean canAppend(WriteCommand write) {
            int newSize = size + write.location.getSize();
			if (newSize >= maxWriteBatchSize || offset+newSize > journal.getMaxFileLength() ) {
                return false;
            }
            return true;
        }

        public void append(WriteCommand write) throws IOException {
            this.writes.addLast(write);
            write.location.setDataFileId(dataFile.getDataFileId());
            write.location.setOffset(offset+size);
            int s = write.location.getSize();
			size += s;
            dataFile.incrementLength(s);
            journal.addToTotalLength(s);
        }
    }

    public static class WriteCommand extends LinkedNode<WriteCommand> {
        public final Location location;
        public final ByteSequence data;
        final boolean sync;
        public final Runnable onComplete;

        public WriteCommand(Location location, ByteSequence data, boolean sync) {
            this.location = location;
            this.data = data;
            this.sync = sync;
            this.onComplete = null;
        }

        public WriteCommand(Location location, ByteSequence data, Runnable onComplete) {
            this.location = location;
            this.data = data;
            this.onComplete = onComplete;
            this.sync = false;
        }
    }

    /**
     * Construct a Store writer
     * 
     * @param fileId
     */
    public DataFileAppender(Journal dataManager) {
        this.journal = dataManager;
        this.inflightWrites = this.journal.getInflightWrites();
        this.maxWriteBatchSize = this.journal.getWriteBatchSize();
    }

    /**
     * @param type
     * @param marshaller
     * @param payload
     * @param type
     * @param sync
     * @return
     * @throws IOException
     * @throws
     * @throws
     */
    public Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException {
    	
        // Write the packet our internal buffer.
        int size = data.getLength() + Journal.RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteBatch batch;
        WriteCommand write = new WriteCommand(location, data, sync);

        // Locate datafile and enqueue into the executor in sychronized block so
        // that writes get equeued onto the executor in order that they were
        // assigned
        // by the data manager (which is basically just appending)

        synchronized (this) {
            batch = enqueue(write);
        }
        location.setLatch(batch.latch);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
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

        WriteBatch batch;
        WriteCommand write = new WriteCommand(location, data, onComplete);

        synchronized (this) {
            batch = enqueue(write);
        }
 
        location.setLatch(batch.latch);
        return location;
    }

    private WriteBatch enqueue(WriteCommand write) throws IOException {
        synchronized (enqueueMutex) {
            if (shutdown) {
                throw new IOException("Async Writter Thread Shutdown");
            }
            if (firstAsyncException != null) {
                throw firstAsyncException;
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
            }

            while ( true ) {
	            if (nextWriteBatch == null) {
	            	DataFile file = journal.getCurrentWriteFile();
	            	if( file.getLength() > journal.getMaxFileLength() ) {
	            		file = journal.rotateWriteFile();
	            	}
	            	
	                nextWriteBatch = new WriteBatch(file, file.getLength(), write);
	                enqueueMutex.notify();
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
	                            enqueueMutex.wait();
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
                inflightWrites.put(new WriteKey(write.location), write);
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

            DataByteArrayOutputStream buff = new DataByteArrayOutputStream(maxWriteBatchSize);
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
                    enqueueMutex.notify();
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

                WriteCommand write = wb.writes.getHead();

                // Write an empty batch control record.
                buff.reset();
                buff.writeInt(Journal.BATCH_CONTROL_RECORD_SIZE);
                buff.writeByte(Journal.BATCH_CONTROL_RECORD_TYPE);
                buff.write(Journal.BATCH_CONTROL_RECORD_MAGIC);
                buff.writeInt(0);
                buff.writeLong(0);
                
                boolean forceToDisk = false;
                while (write != null) {
                    forceToDisk |= write.sync | write.onComplete != null;
                    buff.writeInt(write.location.getSize());
                    buff.writeByte(write.location.getType());
                    buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                    write = write.getNext();
                }

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
                file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());
                
                ReplicationTarget replicationTarget = journal.getReplicationTarget();
                if( replicationTarget!=null ) {
                	replicationTarget.replicate(wb.writes.getHead().location, sequence, forceToDisk);
                }
                
                if (forceToDisk) {
                    file.getFD().sync();
                }

                WriteCommand lastWrite = wb.writes.getTail();
                journal.setLastAppendLocation(lastWrite.location);

                // Now that the data is on disk, remove the writes from the in
                // flight
                // cache.
                write = wb.writes.getHead();
                while (write != null) {
                    if (!write.sync) {
                        inflightWrites.remove(new WriteKey(write.location));
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
        	if (wb != null) {
        		wb.latch.countDown();
        	}
            synchronized (enqueueMutex) {
                firstAsyncException = e;
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
        }
    }

}
