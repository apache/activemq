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
package org.apache.activemq.kaha.impl.async;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.LinkedNode;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 * 
 * @version $Revision: 1.1.1.1 $
 */
class DataFileAppender {

    protected static final byte[] RESERVED_SPACE = new byte[AsyncDataManager.ITEM_HEAD_RESERVED_SPACE];
    protected static final String SHUTDOWN_COMMAND = "SHUTDOWN";

    protected final AsyncDataManager dataManager;
    protected final Map<WriteKey, WriteCommand> inflightWrites;
    protected final Object enqueueMutex = new Object(){};
    protected WriteBatch nextWriteBatch;

    protected boolean shutdown;
    protected IOException firstAsyncException;
    protected final CountDownLatch shutdownDone = new CountDownLatch(1);
    protected int maxWriteBatchSize = 1024 * 1024 * 4;

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
        public final WriteCommand first;
        public final CountDownLatch latch = new CountDownLatch(1);
        public int size;

        public WriteBatch(DataFile dataFile, WriteCommand write) throws IOException {
            this.dataFile = dataFile;
            this.first = write;
            size += write.location.getSize();
        }

        public boolean canAppend(DataFile dataFile, WriteCommand write) {
            if (dataFile != this.dataFile) {
                return false;
            }
            if (size + write.location.getSize() >= maxWriteBatchSize) {
                return false;
            }
            return true;
        }

        public void append(WriteCommand write) throws IOException {
            this.first.getTailNode().linkAfter(write);
            size += write.location.getSize();
        }
    }

    public static class WriteCommand extends LinkedNode {
        public final Location location;
        public final ByteSequence data;
        final boolean sync;
        public final Runnable onComplete;

        public WriteCommand(Location location, ByteSequence data, boolean sync) {
            this.location = location;
            this.data = data;
            this.sync = sync;
            this.onComplete=null;
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
    public DataFileAppender(AsyncDataManager dataManager) {
        this.dataManager = dataManager;
        this.inflightWrites = this.dataManager.getInflightWrites();
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
        int size = data.getLength() + AsyncDataManager.ITEM_HEAD_FOOT_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteBatch batch;
        WriteCommand write = new WriteCommand(location, data, sync);

        // Locate datafile and enqueue into the executor in sychronized block so
        // that
        // writes get equeued onto the executor in order that they were assigned
        // by
        // the data manager (which is basically just appending)

        synchronized (this) {
            // Find the position where this item will land at.
            DataFile dataFile = dataManager.allocateLocation(location);
            batch = enqueue(dataFile, write);
        }
        location.setLatch(batch.latch);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        } else {
            inflightWrites.put(new WriteKey(location), write);
        }

        return location;
    }
    
	public Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException {
        // Write the packet our internal buffer.
        int size = data.getLength() + AsyncDataManager.ITEM_HEAD_FOOT_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        WriteBatch batch;
        WriteCommand write = new WriteCommand(location, data, onComplete);

        // Locate datafile and enqueue into the executor in sychronized block so
        // that
        // writes get equeued onto the executor in order that they were assigned
        // by
        // the data manager (which is basically just appending)

        synchronized (this) {
            // Find the position where this item will land at.
            DataFile dataFile = dataManager.allocateLocation(location);
            batch = enqueue(dataFile, write);
        }
        location.setLatch(batch.latch);
        inflightWrites.put(new WriteKey(location), write);

        return location;
	}

    private WriteBatch enqueue(DataFile dataFile, WriteCommand write) throws IOException {
        synchronized (enqueueMutex) {
            WriteBatch rc = null;
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

            if (nextWriteBatch == null) {
                nextWriteBatch = new WriteBatch(dataFile, write);
                rc = nextWriteBatch;
                enqueueMutex.notify();
            } else {
                // Append to current batch if possible..
                if (nextWriteBatch.canAppend(dataFile, write)) {
                    nextWriteBatch.append(write);
                    rc = nextWriteBatch;
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

                    // Start a new batch.
                    nextWriteBatch = new WriteBatch(dataFile, write);
                    rc = nextWriteBatch;
                    enqueueMutex.notify();
                }
            }
            return rc;
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
     * force calls.
     * 
     * Since the file sync() call is the slowest of all the operations, this
     * algorithm tries to 'batch' or group together several file sync() requests
     * into a single file sync() call. The batching is accomplished attaching
     * the same CountDownLatch instance to every force request in a group.
     * 
     */
    protected void processQueue() {
        DataFile dataFile = null;
        RandomAccessFile file = null;
        try {

            DataByteArrayOutputStream buff = new DataByteArrayOutputStream(maxWriteBatchSize);
            while (true) {

                Object o = null;

                // Block till we get a command.
                synchronized (enqueueMutex) {
                    while (true) {
                        if (shutdown) {
                            o = SHUTDOWN_COMMAND;
                            break;
                        }
                        if (nextWriteBatch != null) {
                            o = nextWriteBatch;
                            nextWriteBatch = null;
                            break;
                        }
                        enqueueMutex.wait();
                    }
                    enqueueMutex.notify();
                }

                if (o == SHUTDOWN_COMMAND) {
                    break;
                }

                WriteBatch wb = (WriteBatch)o;
                if (dataFile != wb.dataFile) {
                    if (file != null) {
                        dataFile.closeRandomAccessFile(file);
                    }
                    dataFile = wb.dataFile;
                    file = dataFile.openRandomAccessFile(true);
                }

                WriteCommand write = wb.first;

                // Write all the data.
                // Only need to seek to first location.. all others
                // are in sequence.
                file.seek(write.location.getOffset());

                // 
                // is it just 1 big write?
                if (wb.size == write.location.getSize()) {

                    // Just write it directly..
                    file.writeInt(write.location.getSize());
                    file.writeByte(write.location.getType());
                    file.write(RESERVED_SPACE);
                    file.write(AsyncDataManager.ITEM_HEAD_SOR);
                    file.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                    file.write(AsyncDataManager.ITEM_HEAD_EOR);

                } else {

                    // Combine the smaller writes into 1 big buffer
                    while (write != null) {

                        buff.writeInt(write.location.getSize());
                        buff.writeByte(write.location.getType());
                        buff.write(RESERVED_SPACE);
                        buff.write(AsyncDataManager.ITEM_HEAD_SOR);
                        buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                        buff.write(AsyncDataManager.ITEM_HEAD_EOR);

                        write = (WriteCommand)write.getNext();
                    }

                    // Now do the 1 big write.
                    ByteSequence sequence = buff.toByteSequence();
                    file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());
                    buff.reset();
                }

                file.getFD().sync();                
                
                WriteCommand lastWrite = (WriteCommand)wb.first.getTailNode();
                dataManager.setLastAppendLocation(lastWrite.location);

                // Signal any waiting threads that the write is on disk.
                wb.latch.countDown();

                // Now that the data is on disk, remove the writes from the in
                // flight
                // cache.
                write = wb.first;
                while (write != null) {
                    if (!write.sync) {
                        inflightWrites.remove(new WriteKey(write.location));
                    }
                    if( write.onComplete !=null ) {
                    	 try {
							write.onComplete.run();
						} catch (Throwable e) {
							e.printStackTrace();
						}
                    }
                    write = (WriteCommand)write.getNext();
                }
            }
            buff.close();
        } catch (IOException e) {
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
