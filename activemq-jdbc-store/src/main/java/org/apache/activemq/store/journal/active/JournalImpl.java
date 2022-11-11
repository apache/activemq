/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.journal.active;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activemq.store.journal.InvalidRecordLocationException;
import org.apache.activemq.store.journal.Journal;
import org.apache.activemq.store.journal.JournalEventListener;
import org.apache.activemq.store.journal.RecordLocation;
import org.apache.activemq.store.journal.packet.ByteArrayPacket;
import org.apache.activemq.store.journal.packet.ByteBufferPacketPool;
import org.apache.activemq.store.journal.packet.Packet;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A high speed Journal implementation. Inspired by the ideas of the <a
 * href="http://howl.objectweb.org/">Howl </a> project but tailored to the needs
 * of ActiveMQ. <p/>This Journal provides the following features:
 * <ul>
 * <li>Concurrent writes are batched into a single write/force done by a
 * background thread.</li>
 * <li>Uses preallocated logs to avoid disk fragmentation and performance
 * degregation.</li>
 * <li>The number and size of the preallocated logs are configurable.</li>
 * <li>Uses direct ByteBuffers to write data to log files.</li>
 * <li>Allows logs to grow in case of an overflow condition so that overflow
 * exceptions are not not thrown. Grown logs that are inactivate (due to a new
 * mark) are resized to their original size.</li>
 * <li>No limit on the size of the record written to the journal</li>
 * <li>Should be possible to extend so that multiple physical disk are used
 * concurrently to increase throughput and decrease latency.</li>
 * </ul>
 * <p/>
 * 
 * @version $Revision: 1.1 $
 */
final public class JournalImpl implements Journal {

    public static final int DEFAULT_POOL_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultPoolSize", ""+(5)));
    public static final int DEFAULT_PACKET_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultPacketSize", ""+(1024*1024*4)));

    static final private int OVERFLOW_RENOTIFICATION_DELAY = 500;
    
    static private ByteBufferPacketPool lastPool;
    
    private boolean disposed = false;

    // The id of the current log file that is being filled.
    private int appendLogFileId = 0;

    // The offset in the current log file that is being filled.
    private int appendLogFileOffset = 0;

    // Used to batch writes together.
    private BatchedWrite pendingBatchWrite;
    
    private Location lastMarkedLocation;
    private LogFileManager file;
    private ThreadPoolExecutor executor;
    private int rolloverFence;
    private JournalEventListener eventListener;
    private ByteBufferPacketPool packetPool;
    private long overflowNotificationTime = System.currentTimeMillis();
    private Packet markPacket = new ByteArrayPacket(new byte[Location.SERIALIZED_SIZE]);
    private boolean doingNotification=false;
    
    public JournalImpl(File logDirectory) throws IOException {
        this(new LogFileManager(logDirectory));
    }

    public JournalImpl(File logDirectory, int logFileCount, int logFileSize) throws IOException {
        this(new LogFileManager(logDirectory, logFileCount, logFileSize, null));
    }
    
    public JournalImpl(File logDirectory, int logFileCount, int logFileSize, File archiveDirectory) throws IOException {
        this(new LogFileManager(logDirectory, logFileCount, logFileSize, archiveDirectory));
    }

    public JournalImpl(LogFileManager logFile) {
        this.file = logFile;
        this.packetPool = createBufferPool();
        this.executor = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread answer = new Thread(runnable, "Journal Writer");
                answer.setPriority(Thread.MAX_PRIORITY);
                answer.setDaemon(true);
                return answer;
            }
        });
        //executor.allowCoreThreadTimeOut(true);
        
        lastMarkedLocation = file.getLastMarkedRecordLocation();
        Location nextAppendLocation = file.getNextAppendLocation();
        appendLogFileId = nextAppendLocation.getLogFileId();
        appendLogFileOffset = nextAppendLocation.getLogFileOffset();
        
        rolloverFence = (file.getInitialLogFileSize() / 10) * 9;
    }

    
    /**
     * When running unit tests we may not be able to create new pools fast enough
     * since the old pools are not being gc'ed fast enough.  So we pool the pool.
     * @return
     */
    synchronized static private ByteBufferPacketPool createBufferPool() {
        if( lastPool !=null ) {
            ByteBufferPacketPool rc = lastPool;
            lastPool = null;
            return rc;
        } else { 
            return new ByteBufferPacketPool(DEFAULT_POOL_SIZE, DEFAULT_PACKET_SIZE);
        }
    }
    
    /**
     * When running unit tests we may not be able to create new pools fast enough
     * since the old pools are not being gc'ed fast enough.  So we pool the pool.
     * @return
     */
    synchronized static private void disposeBufferPool(ByteBufferPacketPool pool) {
        if( lastPool!=null ) {
            pool.dispose();
        } else {
            pool.waitForPacketsToReturn();
            lastPool = pool;
        }
    }



    public RecordLocation write(Packet data, boolean sync) throws IOException {
        return write(LogFileManager.DATA_RECORD_TYPE, data, sync, null);
    }

    private Location write(byte recordType, Packet data, boolean sync, Location mark) throws IOException {
        try {
            Location location;
            BatchedWrite writeCommand;
            
            Record record = new Record(recordType, data, mark);
            
            // The following synchronized block is the bottle neck of the journal.  Make this
            // code faster and the journal should speed up.
            synchronized (this) {
                if (disposed) {
                    throw new IOException("Journal has been closed.");
                }

                // Create our record
                location = new Location(appendLogFileId, appendLogFileOffset);
                record.setLocation(location);
                
                // Piggy back the packet on the pending write batch.
                writeCommand = addToPendingWriteBatch(record, mark, sync);

                // Update where the next record will land.
                appendLogFileOffset += data.limit() + Record.RECORD_BASE_SIZE;
                rolloverCheck();
            }

            if (sync) {
                writeCommand.waitForForce();
            }

            return location;
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw (IOException) new InterruptedIOException().initCause(e);
        } catch (Throwable e) {
            throw (IOException) new IOException("Write failed: " + e).initCause(e);
        }
    }

    /**
     * @param record
     * @return
     * @throws InterruptedException
     */
    private BatchedWrite addToPendingWriteBatch(Record record, Location mark, boolean force) throws InterruptedException {

        // Load the write batch up with data from our record.
        // it may take more than one write batch if the record is large.
        BatchedWrite answer = null;
        while (record.hasRemaining()) {
            
            // Do we need another BatchWrite?
            boolean queueTheWrite=false;
            if (pendingBatchWrite == null) {
                pendingBatchWrite =  new BatchedWrite(packetPool.getPacket());
                queueTheWrite = true;
            }
            answer = pendingBatchWrite;

            // Can we continue to use the pendingBatchWrite?
            boolean full = !pendingBatchWrite.append(record, mark, force);
            
            if( queueTheWrite ) {
                final BatchedWrite queuedWrite = pendingBatchWrite;
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            queuedWrite(queuedWrite);
                        } catch (InterruptedException e) {
                        }
                    }
                });
            }
            
            if( full )
                pendingBatchWrite = null;            
        }
        return answer;

    }

    /**
     * This is a blocking call
     * 
     * @param write
     * @throws InterruptedException
     */
    private void queuedWrite(BatchedWrite write) throws InterruptedException {

        // Stop other threads from appending more pendingBatchWrite.
        write.flip();

        // Do the write.
        try {
            file.append(write);
            write.forced();
        } catch (Throwable e) {
            write.writeFailed(e);
        } finally {
            write.getPacket().dispose();
        }
    }

    /**
     * 
     */
    private void rolloverCheck() throws IOException {

        // See if we need to issue an overflow notification.
        if (eventListener != null && file.isPastHalfActive()
                && overflowNotificationTime + OVERFLOW_RENOTIFICATION_DELAY < System.currentTimeMillis() && !doingNotification ) {
            doingNotification = true;
            try {
                // We need to send an overflow notification to free up
                // some logFiles.
                Location safeSpot = file.getFirstRecordLocationOfSecondActiveLogFile();
                eventListener.overflowNotification(safeSpot);
                overflowNotificationTime = System.currentTimeMillis();
            } finally {
                doingNotification = false;
            }
        }

        // Is it time to roll over?
        if (appendLogFileOffset > rolloverFence ) {

            // Can we roll over?
            if ( !file.canActivateNextLogFile() ) {
                // don't delay the next overflow notification.
                overflowNotificationTime -= OVERFLOW_RENOTIFICATION_DELAY;
                
            } else {
                
                try {
                    final FutureTask result = new FutureTask(new Callable() {
                        public Object call() throws Exception {
                            return queuedActivateNextLogFile();
                        }});
                    executor.execute(result);
                    Location location = (Location) result.get();
                    appendLogFileId = location.getLogFileId();
                    appendLogFileOffset = location.getLogFileOffset();
    
                } catch (InterruptedException e) {
                    throw (IOException) new IOException("Interrupted.").initCause(e);
                }
                catch (ExecutionException e) {
                    throw handleExecutionException(e);
                }
            }
        }
    }

    /**
     * This is a blocking call
     */
    private Location queuedActivateNextLogFile() throws IOException {
        file.activateNextLogFile();
        return file.getNextAppendLocation();
    }

    
    
    /**
     * @param recordLocator
     * @param force
     * @return
     * @throws InvalidRecordLocationException
     * @throws IOException
     * @throws InterruptedException
     */
    synchronized public void setMark(RecordLocation l, boolean force) throws InvalidRecordLocationException,
            IOException {
        
        Location location = (Location) l;
        if (location == null)
            throw new InvalidRecordLocationException("The location cannot be null.");
        if (lastMarkedLocation != null && location.compareTo(lastMarkedLocation) < 0)
            throw new InvalidRecordLocationException("The location is less than the last mark.");
        
        markPacket.clear();
        location.writeToPacket(markPacket);    
        markPacket.flip();
        write(LogFileManager.MARK_RECORD_TYPE, markPacket, force, location);
        
        lastMarkedLocation = location;
    }

    /**
     * @return
     */
    public RecordLocation getMark() {
        return lastMarkedLocation;
    }

    /**
     * @param lastLocation
     * @return
     * @throws IOException
     * @throws InvalidRecordLocationException
     */
    public RecordLocation getNextRecordLocation(final RecordLocation lastLocation) throws IOException,
            InvalidRecordLocationException {
        
        if (lastLocation == null) {
            if (lastMarkedLocation != null) {
                return lastMarkedLocation;
            } else {
                return file.getFirstActiveLogLocation();
            }
        }

        // Run this in the queued executor thread.
        try {
            final FutureTask result = new FutureTask(new Callable() {
                public Object call() throws Exception {
                    return queuedGetNextRecordLocation((Location) lastLocation);
                }});
            executor.execute(result);
            return (Location) result.get();
        } catch (InterruptedException e) {
            throw (IOException) new IOException("Interrupted.").initCause(e);
        }
        catch (ExecutionException e) {
            throw handleExecutionException(e);
        }
    }

    protected IOException handleExecutionException(ExecutionException e) throws IOException {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        else {
            return (IOException) new IOException(cause.getMessage()).initCause(cause);
        }
    }

    private Location queuedGetNextRecordLocation(Location location) throws IOException, InvalidRecordLocationException {
        return file.getNextDataRecordLocation(location);
    }

    /**
     * @param location
     * @return
     * @throws InvalidRecordLocationException
     * @throws IOException
     */
    public Packet read(final RecordLocation l) throws IOException, InvalidRecordLocationException {
        final Location location = (Location) l;
        // Run this in the queued executor thread.
        try {
            final FutureTask result = new FutureTask(new Callable() {
                public Object call() throws Exception {
                    return file.readPacket(location);
                }});
            executor.execute(result);
            return (Packet) result.get();
        } catch (InterruptedException e) {
            throw (IOException) new IOException("Interrupted.").initCause(e);
        }
        catch (ExecutionException e) {
            throw handleExecutionException(e);
        }
    }

    public void setJournalEventListener(JournalEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * @deprecated @see #dispose()
     */
    public void close() throws IOException {
    	dispose();
    }
    
    /**
     */
    public void dispose() {
        if (disposed)
            return;
        disposed=true;
        executor.shutdown();
        file.dispose();
        ByteBufferPacketPool pool = packetPool;
        packetPool=null;
        disposeBufferPool(pool);
    }

    /**
     * @return
     */
    public File getLogDirectory() {
        return file.getLogDirectory();
    }

    public int getInitialLogFileSize() {
        return file.getInitialLogFileSize();
    }
    
    public String toString() {
        return "Active Journal: using "+file.getOnlineLogFileCount()+" x " + (file.getInitialLogFileSize()/(1024*1024f)) + " Megs at: " + getLogDirectory();
    }

}
