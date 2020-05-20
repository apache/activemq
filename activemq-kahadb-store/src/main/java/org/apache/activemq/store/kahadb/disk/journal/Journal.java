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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.util.LinkedNode;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.store.kahadb.disk.util.Sequence;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages DataFiles
 */
public class Journal {
    public static final String CALLER_BUFFER_APPENDER = "org.apache.kahadb.journal.CALLER_BUFFER_APPENDER";
    public static final boolean callerBufferAppender = Boolean.parseBoolean(System.getProperty(CALLER_BUFFER_APPENDER, "false"));

    private static final int PREALLOC_CHUNK_SIZE = 1024*1024;

    // ITEM_HEAD_SPACE = length + type+ reserved space + SOR
    public static final int RECORD_HEAD_SPACE = 4 + 1;

    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    // Batch Control Item holds a 4 byte size of the batch and a 8 byte checksum of the batch.
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = bytes("WRITE BATCH");
    public static final int BATCH_CONTROL_RECORD_SIZE = RECORD_HEAD_SPACE + BATCH_CONTROL_RECORD_MAGIC.length + 4 + 8;
    public static final byte[] BATCH_CONTROL_RECORD_HEADER = createBatchControlRecordHeader();
    public static final byte[] EMPTY_BATCH_CONTROL_RECORD = createEmptyBatchControlRecordHeader();
    public static final int EOF_INT = ByteBuffer.wrap(new byte[]{'-', 'q', 'M', 'a'}).getInt();
    public static final byte EOF_EOT = '4';
    public static final byte[] EOF_RECORD = createEofBatchAndLocationRecord();

    private ScheduledExecutorService scheduler;

    // tackle corruption when checksum is disabled or corrupt with zeros, minimize data loss
    public void corruptRecoveryLocation(Location recoveryPosition) throws IOException {
        DataFile dataFile = getDataFile(recoveryPosition);
        // with corruption on recovery we have no faith in the content - slip to the next batch record or eof
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            RandomAccessFile randomAccessFile = reader.getRaf().getRaf();
            randomAccessFile.seek(recoveryPosition.getOffset() + 1);
            byte[] data = new byte[getWriteBatchSize()];
            ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data));
            int nextOffset = 0;
            if (findNextBatchRecord(bs, randomAccessFile) >= 0) {
                nextOffset = Math.toIntExact(randomAccessFile.getFilePointer() - bs.remaining());
            } else {
                nextOffset = Math.toIntExact(randomAccessFile.length());
            }
            Sequence sequence = new Sequence(recoveryPosition.getOffset(), nextOffset - 1);
            LOG.warn("Corrupt journal records found in '{}' between offsets: {}", dataFile.getFile(), sequence);

            // skip corruption on getNextLocation
            recoveryPosition.setOffset(nextOffset);
            recoveryPosition.setSize(-1);

            dataFile.corruptedBlocks.add(sequence);
        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    public DataFileAccessorPool getAccessorPool() {
        return accessorPool;
    }

    public void allowIOResumption() {
        if (appender instanceof DataFileAppender) {
            DataFileAppender dataFileAppender = (DataFileAppender)appender;
            dataFileAppender.shutdown = false;
        }
    }

    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public long getCleanupInterval() {
        return cleanupInterval;
    }

    public enum PreallocationStrategy {
        SPARSE_FILE,
        OS_KERNEL_COPY,
        ZEROS,
        CHUNKED_ZEROS;
    }

    public enum PreallocationScope {
        ENTIRE_JOURNAL,
        ENTIRE_JOURNAL_ASYNC,
        NONE;
    }

    public enum JournalDiskSyncStrategy {
        ALWAYS,
        PERIODIC,
        NEVER;
    }

    private static byte[] createBatchControlRecordHeader() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(BATCH_CONTROL_RECORD_SIZE);
            os.writeByte(BATCH_CONTROL_RECORD_TYPE);
            os.write(BATCH_CONTROL_RECORD_MAGIC);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create batch control record header.", e);
        }
    }

    private static byte[] createEmptyBatchControlRecordHeader() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(BATCH_CONTROL_RECORD_SIZE);
            os.writeByte(BATCH_CONTROL_RECORD_TYPE);
            os.write(BATCH_CONTROL_RECORD_MAGIC);
            os.writeInt(0);
            os.writeLong(0l);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create empty batch control record header.", e);
        }
    }

    private static byte[] createEofBatchAndLocationRecord() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(EOF_INT);
            os.writeByte(EOF_EOT);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create eof header.", e);
        }
    }

    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 1024 * 1024 * 4;

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

    protected final Map<WriteKey, WriteCommand> inflightWrites = new ConcurrentHashMap<WriteKey, WriteCommand>();

    protected File directory = new File(DEFAULT_DIRECTORY);
    protected File directoryArchive;
    private boolean directoryArchiveOverridden = false;

    protected String filePrefix = DEFAULT_FILE_PREFIX;
    protected String fileSuffix = DEFAULT_FILE_SUFFIX;
    protected boolean started;

    protected int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    protected int writeBatchSize = DEFAULT_MAX_WRITE_BATCH_SIZE;

    protected FileAppender appender;
    protected DataFileAccessorPool accessorPool;

    protected Map<Integer, DataFile> fileMap = new HashMap<Integer, DataFile>();
    protected Map<File, DataFile> fileByFileMap = new LinkedHashMap<File, DataFile>();
    protected LinkedNodeList<DataFile> dataFiles = new LinkedNodeList<DataFile>();

    protected final AtomicReference<Location> lastAppendLocation = new AtomicReference<Location>();
    protected ScheduledFuture cleanupTask;
    protected AtomicLong totalLength = new AtomicLong();
    protected boolean archiveDataLogs;
    private ReplicationTarget replicationTarget;
    protected boolean checksum;
    protected boolean checkForCorruptionOnStartup;
    protected boolean enableAsyncDiskSync = true;
    private int nextDataFileId = 1;
    private Object dataFileIdLock = new Object();
    private final AtomicReference<DataFile> currentDataFile = new AtomicReference<>(null);
    private volatile DataFile nextDataFile;

    protected PreallocationScope preallocationScope = PreallocationScope.ENTIRE_JOURNAL;
    protected PreallocationStrategy preallocationStrategy = PreallocationStrategy.SPARSE_FILE;
    private File osKernelCopyTemplateFile = null;
    private ByteBuffer preAllocateDirectBuffer = null;
    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    protected JournalDiskSyncStrategy journalDiskSyncStrategy = JournalDiskSyncStrategy.ALWAYS;

    public interface DataFileRemovedListener {
        void fileRemoved(DataFile datafile);
    }

    private DataFileRemovedListener dataFileRemovedListener;

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        long start = System.currentTimeMillis();
        accessorPool = new DataFileAccessorPool(this);
        started = true;

        appender = callerBufferAppender ? new CallerBufferingDataFileAppender(this) : new DataFileAppender(this);

        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }
        });

        if (files != null) {
            for (File file : files) {
                try {
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length()-fileSuffix.length());
                    int num = Integer.parseInt(numStr);
                    DataFile dataFile = new DataFile(file, num);
                    fileMap.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }

            // Sort the list so that we can link the DataFiles together in the
            // right order.
            LinkedList<DataFile> l = new LinkedList<>(fileMap.values());
            Collections.sort(l);
            for (DataFile df : l) {
                if (df.getLength() == 0) {
                    // possibly the result of a previous failed write
                    LOG.info("ignoring zero length, partially initialised journal data file: " + df);
                    continue;
                } else if (l.getLast().equals(df) && isUnusedPreallocated(df)) {
                    continue;
                }
                dataFiles.addLast(df);
                fileByFileMap.put(df.getFile(), df);

                if( isCheckForCorruptionOnStartup() ) {
                    lastAppendLocation.set(recoveryCheck(df));
                }
            }
        }

        if (preallocationScope != PreallocationScope.NONE) {
            switch (preallocationStrategy) {
                case SPARSE_FILE:
                    break;
                case OS_KERNEL_COPY: {
                    osKernelCopyTemplateFile = createJournalTemplateFile();
                }
                break;
                case CHUNKED_ZEROS: {
                    preAllocateDirectBuffer = allocateDirectBuffer(PREALLOC_CHUNK_SIZE);
                }
                break;
                case ZEROS: {
                    preAllocateDirectBuffer = allocateDirectBuffer(getMaxFileLength());
                }
                break;
            }
        }
        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread schedulerThread = new Thread(r);
                schedulerThread.setName("ActiveMQ Journal Scheduled executor");
                schedulerThread.setDaemon(true);
                return schedulerThread;
            }
        });

        // init current write file
        if (dataFiles.isEmpty()) {
            nextDataFileId = 1;
            rotateWriteFile();
        } else {
            currentDataFile.set(dataFiles.getTail());
            nextDataFileId = currentDataFile.get().dataFileId + 1;
        }

        if( lastAppendLocation.get()==null ) {
            DataFile df = dataFiles.getTail();
            lastAppendLocation.set(recoveryCheck(df));
        }

        // ensure we don't report unused space of last journal file in size metric
        int lastFileLength = dataFiles.getTail().getLength();
        if (totalLength.get() > lastFileLength && lastAppendLocation.get().getOffset() > 0) {
            totalLength.addAndGet(lastAppendLocation.get().getOffset() - lastFileLength);
        }

        cleanupTask = scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanup();
            }
        }, cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);

        long end = System.currentTimeMillis();
        LOG.trace("Startup took: "+(end-start)+" ms");
    }

    private ByteBuffer allocateDirectBuffer(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.put(EOF_RECORD);
        return buffer;
    }

    public void preallocateEntireJournalDataFile(RecoverableRandomAccessFile file) {

        if (PreallocationScope.NONE != preallocationScope) {

            try {
                if (PreallocationStrategy.OS_KERNEL_COPY == preallocationStrategy) {
                    doPreallocationKernelCopy(file);
                } else if (PreallocationStrategy.ZEROS == preallocationStrategy) {
                    doPreallocationZeros(file);
                } else if (PreallocationStrategy.CHUNKED_ZEROS == preallocationStrategy) {
                    doPreallocationChunkedZeros(file);
                } else {
                    doPreallocationSparseFile(file);
                }
            } catch (Throwable continueWithNoPrealloc) {
                // error on preallocation is non fatal, and we don't want to leak the journal handle
                LOG.error("cound not preallocate journal data file", continueWithNoPrealloc);
            }
        }
    }

    private void doPreallocationSparseFile(RecoverableRandomAccessFile file) {
        final ByteBuffer journalEof = ByteBuffer.wrap(EOF_RECORD);
        try {
            FileChannel channel = file.getChannel();
            channel.position(0);
            channel.write(journalEof);
            channel.position(maxFileLength - 5);
            journalEof.rewind();
            channel.write(journalEof);
            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with sparse file", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with sparse file", e);
        }
    }

    private void doPreallocationZeros(RecoverableRandomAccessFile file) {
        preAllocateDirectBuffer.rewind();
        try {
            FileChannel channel = file.getChannel();
            channel.write(preAllocateDirectBuffer);
            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with zeros", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with zeros", e);
        }
    }

    private void doPreallocationKernelCopy(RecoverableRandomAccessFile file) {
        try (RandomAccessFile templateRaf = new RandomAccessFile(osKernelCopyTemplateFile, "rw");){
            templateRaf.getChannel().transferTo(0, getMaxFileLength(), file.getChannel());
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with kernel copy", ignored);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find the template file on disk at " + osKernelCopyTemplateFile.getAbsolutePath(), e);
        } catch (IOException e) {
            LOG.error("Could not transfer the template file to journal, transferFile=" + osKernelCopyTemplateFile.getAbsolutePath(), e);
        }
    }

    private File createJournalTemplateFile() {
        String fileName = "db-log.template";
        File rc = new File(directory, fileName);
        try (RandomAccessFile templateRaf = new RandomAccessFile(rc, "rw");) {
            templateRaf.getChannel().write(ByteBuffer.wrap(EOF_RECORD));
            templateRaf.setLength(maxFileLength);
            templateRaf.getChannel().force(true);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find the template file on disk at " + osKernelCopyTemplateFile.getAbsolutePath(), e);
        } catch (IOException e) {
            LOG.error("Could not transfer the template file to journal, transferFile=" + osKernelCopyTemplateFile.getAbsolutePath(), e);
        }
        return rc;
    }

    private void doPreallocationChunkedZeros(RecoverableRandomAccessFile file) {
        preAllocateDirectBuffer.limit(preAllocateDirectBuffer.capacity());
        preAllocateDirectBuffer.rewind();
        try {
            FileChannel channel = file.getChannel();

            int remLen = maxFileLength;
            while (remLen > 0) {
                if (remLen < preAllocateDirectBuffer.remaining()) {
                    preAllocateDirectBuffer.limit(remLen);
                }
                int writeLen = channel.write(preAllocateDirectBuffer);
                remLen -= writeLen;
                preAllocateDirectBuffer.rewind();
            }

            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with zeros", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with zeros! Will continue without preallocation", e);
        }
    }

    private static byte[] bytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isUnusedPreallocated(DataFile dataFile) throws IOException {
        if (preallocationScope == PreallocationScope.ENTIRE_JOURNAL_ASYNC) {
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                byte[] firstFewBytes = new byte[BATCH_CONTROL_RECORD_HEADER.length];
                reader.readFully(0, firstFewBytes);
                ByteSequence bs = new ByteSequence(firstFewBytes);
                return bs.startsWith(EOF_RECORD);
            } catch (Exception ignored) {
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }
        }
        return false;
    }

    protected Location recoveryCheck(DataFile dataFile) throws IOException {
        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            RandomAccessFile randomAccessFile = reader.getRaf().getRaf();
            randomAccessFile.seek(0);
            final long totalFileLength = randomAccessFile.length();
            byte[] data = new byte[getWriteBatchSize()];
            ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data));

            while (true) {
                int size = checkBatchRecord(bs, randomAccessFile);
                if (size > 0 && location.getOffset() + BATCH_CONTROL_RECORD_SIZE + size <= totalFileLength) {
                    location.setOffset(location.getOffset() + BATCH_CONTROL_RECORD_SIZE + size);
                } else if (size == 0 && location.getOffset() + EOF_RECORD.length + size <= totalFileLength) {
                    // eof batch record
                    break;
                } else  {
                    // track corruption and skip if possible
                    Sequence sequence = new Sequence(location.getOffset());
                    if (findNextBatchRecord(bs, randomAccessFile) >= 0) {
                        int nextOffset = Math.toIntExact(randomAccessFile.getFilePointer() - bs.remaining());
                        sequence.setLast(nextOffset - 1);
                        dataFile.corruptedBlocks.add(sequence);
                        LOG.warn("Corrupt journal records found in '{}' between offsets: {}", dataFile.getFile(), sequence);
                        location.setOffset(nextOffset);
                    } else {
                        // corruption to eof, don't loose track of this corruption, don't truncate
                        sequence.setLast(Math.toIntExact(randomAccessFile.getFilePointer()));
                        dataFile.corruptedBlocks.add(sequence);
                        LOG.warn("Corrupt journal records found in '{}' from offset: {} to EOF", dataFile.getFile(), sequence);
                        break;
                    }
                }
            }

        } catch (IOException e) {
            LOG.trace("exception on recovery check of: " + dataFile + ", at " + location, e);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }

        int existingLen = dataFile.getLength();
        dataFile.setLength(location.getOffset());
        if (existingLen > dataFile.getLength()) {
            totalLength.addAndGet(dataFile.getLength() - existingLen);
        }
        return location;
    }

    private int findNextBatchRecord(ByteSequence bs, RandomAccessFile reader) throws IOException {
        final ByteSequence header = new ByteSequence(BATCH_CONTROL_RECORD_HEADER);
        int pos = 0;
        while (true) {
            pos = bs.indexOf(header, 0);
            if (pos >= 0) {
                bs.setOffset(bs.offset + pos);
                return pos;
            } else {
                // need to load the next data chunck in..
                if (bs.length != bs.data.length) {
                    // If we had a short read then we were at EOF
                    return -1;
                }
                bs.setOffset(bs.length - BATCH_CONTROL_RECORD_HEADER.length);
                bs.reset();
                bs.setLength(bs.length + reader.read(bs.data, bs.length, bs.data.length - BATCH_CONTROL_RECORD_HEADER.length));
            }
        }
    }

    private int checkBatchRecord(ByteSequence bs, RandomAccessFile reader) throws IOException {
        ensureAvailable(bs, reader, EOF_RECORD.length);
        if (bs.startsWith(EOF_RECORD)) {
            return 0; // eof
        }
        ensureAvailable(bs, reader, BATCH_CONTROL_RECORD_SIZE);
        try (DataByteArrayInputStream controlIs = new DataByteArrayInputStream(bs)) {

            // Assert that it's a batch record.
            for (int i = 0; i < BATCH_CONTROL_RECORD_HEADER.length; i++) {
                if (controlIs.readByte() != BATCH_CONTROL_RECORD_HEADER[i]) {
                    return -1;
                }
            }

            int size = controlIs.readInt();
            if (size < 0 || size > Integer.MAX_VALUE - (BATCH_CONTROL_RECORD_SIZE + EOF_RECORD.length)) {
                return -2;
            }

            long expectedChecksum = controlIs.readLong();
            Checksum checksum = null;
            if (isChecksum() && expectedChecksum > 0) {
                checksum = new Adler32();
            }

            // revert to bs to consume data
            bs.setOffset(controlIs.position());
            int toRead = size;
            while (toRead > 0) {
                if (bs.remaining() >= toRead) {
                    if (checksum != null) {
                        checksum.update(bs.getData(), bs.getOffset(), toRead);
                    }
                    bs.setOffset(bs.offset + toRead);
                    toRead = 0;
                } else {
                    if (bs.length != bs.data.length) {
                        // buffer exhausted
                        return  -3;
                    }

                    toRead -= bs.remaining();
                    if (checksum != null) {
                        checksum.update(bs.getData(), bs.getOffset(), bs.remaining());
                    }
                    bs.setLength(reader.read(bs.data));
                    bs.setOffset(0);
                }
            }
            if (checksum != null && expectedChecksum != checksum.getValue()) {
                return -4;
            }

            return size;
        }
    }

    private void ensureAvailable(ByteSequence bs, RandomAccessFile reader, int required) throws IOException {
        if (bs.remaining() < required) {
            bs.reset();
            int read = reader.read(bs.data, bs.length, bs.data.length - bs.length);
            if (read < 0) {
                if (bs.remaining() == 0) {
                    throw new EOFException("request for " + required + " bytes reached EOF");
                }
            }
            bs.setLength(bs.length + read);
        }
    }

    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    public long length() {
        return totalLength.get();
    }

    public void rotateWriteFile() throws IOException {
       synchronized (dataFileIdLock) {
            DataFile dataFile = nextDataFile;
            if (dataFile == null) {
                dataFile = newDataFile();
            }
            synchronized (currentDataFile) {
                fileMap.put(dataFile.getDataFileId(), dataFile);
                fileByFileMap.put(dataFile.getFile(), dataFile);
                dataFiles.addLast(dataFile);
                currentDataFile.set(dataFile);
            }
            nextDataFile = null;
        }
        if (PreallocationScope.ENTIRE_JOURNAL_ASYNC == preallocationScope) {
            preAllocateNextDataFileFuture = scheduler.submit(preAllocateNextDataFileTask);
        }
    }

    private Runnable preAllocateNextDataFileTask = new Runnable() {
        @Override
        public void run() {
            if (nextDataFile == null) {
                synchronized (dataFileIdLock){
                    try {
                        nextDataFile = newDataFile();
                    } catch (IOException e) {
                        LOG.warn("Failed to proactively allocate data file", e);
                    }
                }
            }
        }
    };

    private volatile Future preAllocateNextDataFileFuture;

    private DataFile newDataFile() throws IOException {
        int nextNum = nextDataFileId++;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        preallocateEntireJournalDataFile(nextWriteFile.appendRandomAccessFile());
        return nextWriteFile;
    }


    public DataFile reserveDataFile() {
        synchronized (dataFileIdLock) {
            int nextNum = nextDataFileId++;
            File file = getFile(nextNum);
            DataFile reservedDataFile = new DataFile(file, nextNum);
            synchronized (currentDataFile) {
                fileMap.put(reservedDataFile.getDataFileId(), reservedDataFile);
                fileByFileMap.put(file, reservedDataFile);
                if (dataFiles.isEmpty()) {
                    dataFiles.addLast(reservedDataFile);
                } else {
                    dataFiles.getTail().linkBefore(reservedDataFile);
                }
            }
            return reservedDataFile;
        }
    }

    public File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = null;
        synchronized (currentDataFile) {
            dataFile = fileMap.get(key);
        }
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    public void close() throws IOException {
        synchronized (this) {
            if (!started) {
                return;
            }
            cleanupTask.cancel(true);
            if (preAllocateNextDataFileFuture != null) {
                preAllocateNextDataFileFuture.cancel(true);
            }
            ThreadPoolUtils.shutdownGraceful(scheduler, 4000);
            accessorPool.close();
        }
        // the appender can be calling back to to the journal blocking a close AMQ-5620
        appender.close();
        synchronized (currentDataFile) {
            fileMap.clear();
            fileByFileMap.clear();
            dataFiles.clear();
            lastAppendLocation.set(null);
            started = false;
        }
    }

    public synchronized void cleanup() {
        if (accessorPool != null) {
            accessorPool.disposeUnused();
        }
    }

    public synchronized boolean delete() throws IOException {

        // Close all open file handles...
        appender.close();
        accessorPool.close();

        boolean result = true;
        for (Iterator<DataFile> i = fileMap.values().iterator(); i.hasNext();) {
            DataFile dataFile = i.next();
            result &= dataFile.delete();
        }

        if (preAllocateNextDataFileFuture != null) {
            preAllocateNextDataFileFuture.cancel(true);
        }
        synchronized (dataFileIdLock) {
            if (nextDataFile != null) {
                nextDataFile.delete();
                nextDataFile = null;
            }
        }

        totalLength.set(0);
        synchronized (currentDataFile) {
            fileMap.clear();
            fileByFileMap.clear();
            lastAppendLocation.set(null);
            dataFiles = new LinkedNodeList<DataFile>();
        }
        // reopen open file handles...
        accessorPool = new DataFileAccessorPool(this);
        appender = new DataFileAppender(this);
        return result;
    }

    public void removeDataFiles(Set<Integer> files) throws IOException {
        for (Integer key : files) {
            // Can't remove the data file (or subsequent files) that is currently being written to.
            if (key >= lastAppendLocation.get().getDataFileId()) {
                continue;
            }
            DataFile dataFile = null;
            synchronized (currentDataFile) {
                dataFile = fileMap.remove(key);
                if (dataFile != null) {
                    fileByFileMap.remove(dataFile.getFile());
                    dataFile.unlink();
                }
            }
            if (dataFile != null) {
                forceRemoveDataFile(dataFile);
            }
        }
    }

    private void forceRemoveDataFile(DataFile dataFile) throws IOException {
        accessorPool.disposeDataFileAccessors(dataFile);
        totalLength.addAndGet(-dataFile.getLength());
        if (archiveDataLogs) {
            File directoryArchive = getDirectoryArchive();
            if (directoryArchive.exists()) {
                LOG.debug("Archive directory exists: {}", directoryArchive);
            } else {
                if (directoryArchive.isAbsolute())
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Archive directory [{}] does not exist - creating it now",
                            directoryArchive.getAbsolutePath());
                }
                IOHelper.mkdirs(directoryArchive);
            }
            LOG.debug("Moving data file {} to {} ", dataFile, directoryArchive.getCanonicalPath());
            dataFile.move(directoryArchive);
            LOG.debug("Successfully moved data file");
        } else {
            LOG.debug("Deleting data file: {}", dataFile);
            if (dataFile.delete()) {
                LOG.debug("Discarded data file: {}", dataFile);
            } else {
                LOG.warn("Failed to discard data file : {}", dataFile.getFile());
            }
        }
        if (dataFileRemovedListener != null) {
            dataFileRemovedListener.fileRemoved(dataFile);
        }
    }

    /**
     * @return the maxFileLength
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * @param maxFileLength the maxFileLength to set
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    @Override
    public String toString() {
        return directory.toString();
    }

    public Location getNextLocation(Location location) throws IOException, IllegalStateException {
        return getNextLocation(location, null);
    }

    public Location getNextLocation(Location location, Location limit) throws IOException, IllegalStateException {
        Location cur = null;
        while (true) {
            if (cur == null) {
                if (location == null) {
                    DataFile head = null;
                    synchronized (currentDataFile) {
                        head = dataFiles.getHead();
                    }
                    if (head == null) {
                        return null;
                    }
                    cur = new Location();
                    cur.setDataFileId(head.getDataFileId());
                    cur.setOffset(0);
                } else {
                    // Set to the next offset..
                    if (location.getSize() == -1) {
                        cur = new Location(location);
                    } else {
                        cur = new Location(location);
                        cur.setOffset(location.getOffset() + location.getSize());
                    }
                }
            } else {
                cur.setOffset(cur.getOffset() + cur.getSize());
            }

            DataFile dataFile = getDataFile(cur);

            // Did it go into the next file??
            if (dataFile.getLength() <= cur.getOffset()) {
                synchronized (currentDataFile) {
                    dataFile = dataFile.getNext();
                }
                if (dataFile == null) {
                    return null;
                } else {
                    cur.setDataFileId(dataFile.getDataFileId().intValue());
                    cur.setOffset(0);
                    if (limit != null && cur.compareTo(limit) >= 0) {
                        LOG.trace("reached limit: {} at: {}", limit, cur);
                        return null;
                    }
                }
            }

            // Load in location size and type.
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                reader.readLocationDetails(cur);
            } catch (EOFException eof) {
                LOG.trace("EOF on next: " + location + ", cur: " + cur);
                throw eof;
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }

            Sequence corruptedRange = dataFile.corruptedBlocks.get(cur.getOffset());
            if (corruptedRange != null) {
                // skip corruption
                cur.setSize((int) corruptedRange.range());
            } else if (cur.getSize() == EOF_INT && cur.getType() == EOF_EOT ||
                    (cur.getType() == 0 && cur.getSize() == 0)) {
                // eof - jump to next datafile
                // EOF_INT and EOF_EOT replace 0,0 - we need to react to both for
                // replay of existing journals
                // possibly journal is larger than maxFileLength after config change
                cur.setSize(EOF_RECORD.length);
                cur.setOffset(Math.max(maxFileLength, dataFile.getLength()));
            } else if (cur.getType() == USER_RECORD_TYPE) {
                // Only return user records.
                return cur;
            }
        }
    }

    public ByteSequence read(Location location) throws IOException, IllegalStateException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        ByteSequence rc = null;
        try {
            rc = reader.readRecord(location);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        return rc;
    }

    public Location write(ByteSequence data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, sync);
        return loc;
    }

    public Location write(ByteSequence data, Runnable onComplete) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, onComplete);
        return loc;
    }

    public void update(Location location, ByteSequence data, boolean sync) throws IOException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor updater = accessorPool.openDataFileAccessor(dataFile);
        try {
            updater.updateRecord(location, data, sync);
        } finally {
            accessorPool.closeDataFileAccessor(updater);
        }
    }

    public PreallocationStrategy getPreallocationStrategy() {
        return preallocationStrategy;
    }

    public void setPreallocationStrategy(PreallocationStrategy preallocationStrategy) {
        this.preallocationStrategy = preallocationStrategy;
    }

    public PreallocationScope getPreallocationScope() {
        return preallocationScope;
    }

    public void setPreallocationScope(PreallocationScope preallocationScope) {
        this.preallocationScope = preallocationScope;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public Map<WriteKey, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    public Location getLastAppendLocation() {
        return lastAppendLocation.get();
    }

    public void setLastAppendLocation(Location lastSyncedLocation) {
        this.lastAppendLocation.set(lastSyncedLocation);
    }

    public File getDirectoryArchive() {
        if (!directoryArchiveOverridden && (directoryArchive == null)) {
            // create the directoryArchive relative to the journal location
            directoryArchive = new File(directory.getAbsolutePath() +
                    File.separator + DEFAULT_ARCHIVE_DIRECTORY);
        }
        return directoryArchive;
    }

    public void setDirectoryArchive(File directoryArchive) {
        directoryArchiveOverridden = true;
        this.directoryArchive = directoryArchive;
    }

    public boolean isArchiveDataLogs() {
        return archiveDataLogs;
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    public DataFile getDataFileById(int dataFileId) {
        synchronized (currentDataFile) {
            return fileMap.get(Integer.valueOf(dataFileId));
        }
    }

    public DataFile getCurrentDataFile(int capacity) throws IOException {
        //First just acquire the currentDataFile lock and return if no rotation needed
        synchronized (currentDataFile) {
            if (currentDataFile.get().getLength() + capacity < maxFileLength) {
                return currentDataFile.get();
            }
        }

        //AMQ-6545 - if rotation needed, acquire dataFileIdLock first to prevent deadlocks
        //then re-check if rotation is needed
        synchronized (dataFileIdLock) {
            synchronized (currentDataFile) {
                if (currentDataFile.get().getLength() + capacity >= maxFileLength) {
                    rotateWriteFile();
                }
                return currentDataFile.get();
            }
        }
    }

    public Integer getCurrentDataFileId() {
        synchronized (currentDataFile) {
            return currentDataFile.get().getDataFileId();
        }
    }

    /**
     * Get a set of files - only valid after start()
     *
     * @return files currently being used
     */
    public Set<File> getFiles() {
        synchronized (currentDataFile) {
            return fileByFileMap.keySet();
        }
    }

    public Map<Integer, DataFile> getFileMap() {
        synchronized (currentDataFile) {
            return new TreeMap<Integer, DataFile>(fileMap);
        }
    }

    public long getDiskSize() {
        return totalLength.get();
    }

    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }

    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public boolean isChecksum() {
        return checksum;
    }

    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    public boolean isCheckForCorruptionOnStartup() {
        return checkForCorruptionOnStartup;
    }

    public void setCheckForCorruptionOnStartup(boolean checkForCorruptionOnStartup) {
        this.checkForCorruptionOnStartup = checkForCorruptionOnStartup;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setSizeAccumulator(AtomicLong storeSizeAccumulator) {
       this.totalLength = storeSizeAccumulator;
    }

    public void setEnableAsyncDiskSync(boolean val) {
        this.enableAsyncDiskSync = val;
    }

    public boolean isEnableAsyncDiskSync() {
        return enableAsyncDiskSync;
    }

    public JournalDiskSyncStrategy getJournalDiskSyncStrategy() {
        return journalDiskSyncStrategy;
    }

    public void setJournalDiskSyncStrategy(JournalDiskSyncStrategy journalDiskSyncStrategy) {
        this.journalDiskSyncStrategy = journalDiskSyncStrategy;
    }

    public boolean isJournalDiskSyncPeriodic() {
        return JournalDiskSyncStrategy.PERIODIC.equals(journalDiskSyncStrategy);
    }

    public void setDataFileRemovedListener(DataFileRemovedListener dataFileRemovedListener) {
        this.dataFileRemovedListener = dataFileRemovedListener;
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
}
