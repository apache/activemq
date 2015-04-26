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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import org.apache.activemq.store.kahadb.disk.util.LinkedNode;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.store.kahadb.disk.util.SchedulerTimerTask;
import org.apache.activemq.store.kahadb.disk.util.Sequence;

/**
 * Manages DataFiles
 *
 *
 */
public class Journal {
    public static final String CALLER_BUFFER_APPENDER = "org.apache.kahadb.journal.CALLER_BUFFER_APPENDER";
    public static final boolean callerBufferAppender = Boolean.parseBoolean(System.getProperty(CALLER_BUFFER_APPENDER, "false"));

    private static final int MAX_BATCH_SIZE = 32*1024*1024;

    // ITEM_HEAD_SPACE = length + type+ reserved space + SOR
    public static final int RECORD_HEAD_SPACE = 4 + 1;

    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    // Batch Control Item holds a 4 byte size of the batch and a 8 byte checksum of the batch.
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = bytes("WRITE BATCH");
    public static final int BATCH_CONTROL_RECORD_SIZE = RECORD_HEAD_SPACE+BATCH_CONTROL_RECORD_MAGIC.length+4+8;
    public static final byte[] BATCH_CONTROL_RECORD_HEADER = createBatchControlRecordHeader();

    // tackle corruption when checksum is disabled or corrupt with zeros, minimise data loss
    public void corruptRecoveryLocation(Location recoveryPosition) throws IOException {
        DataFile dataFile = getDataFile(recoveryPosition);
        // with corruption on recovery we have no faith in the content - slip to the next batch record or eof
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            int nextOffset = findNextBatchRecord(reader, recoveryPosition.getOffset() + 1);
            Sequence sequence = new Sequence(recoveryPosition.getOffset(), nextOffset >= 0 ? nextOffset - 1 : dataFile.getLength() - 1);
            LOG.info("Corrupt journal records found in '" + dataFile.getFile() + "' between offsets: " + sequence);

            // skip corruption on getNextLocation
            recoveryPosition.setOffset((int) sequence.getLast() + 1);
            recoveryPosition.setSize(-1);

            dataFile.corruptedBlocks.add(sequence);

        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    public enum PreallocationStrategy {
        SPARSE_FILE,
        OS_KERNEL_COPY,
        ZEROS;
    }

    public enum PreallocationScope {
        ENTIRE_JOURNAL;
    }

    private static byte[] createBatchControlRecordHeader() {
        try {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream();
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

    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int PREFERED_DIFF = 1024 * 512;
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
    protected Runnable cleanupTask;
    protected AtomicLong totalLength = new AtomicLong();
    protected boolean archiveDataLogs;
    private ReplicationTarget replicationTarget;
    protected boolean checksum;
    protected boolean checkForCorruptionOnStartup;
    protected boolean enableAsyncDiskSync = true;
    private Timer timer;

    protected PreallocationScope preallocationScope = PreallocationScope.ENTIRE_JOURNAL;
    protected PreallocationStrategy preallocationStrategy = PreallocationStrategy.SPARSE_FILE;

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
            List<DataFile> l = new ArrayList<DataFile>(fileMap.values());
            Collections.sort(l);
            for (DataFile df : l) {
                if (df.getLength() == 0) {
                    // possibly the result of a previous failed write
                    LOG.info("ignoring zero length, partially initialised journal data file: " + df);
                    continue;
                }
                dataFiles.addLast(df);
                fileByFileMap.put(df.getFile(), df);

                if( isCheckForCorruptionOnStartup() ) {
                    lastAppendLocation.set(recoveryCheck(df));
                }
            }
        }

        getCurrentWriteFile();

        if (preallocationStrategy != PreallocationStrategy.SPARSE_FILE && maxFileLength != DEFAULT_MAX_FILE_LENGTH) {
            LOG.warn("You are using a preallocation strategy and journal maxFileLength which should be benchmarked accordingly to not introduce unexpected latencies.");
        }

        if( lastAppendLocation.get()==null ) {
            DataFile df = dataFiles.getTail();
            lastAppendLocation.set(recoveryCheck(df));
        }

        // ensure we don't report unused space of last journal file in size metric
        if (totalLength.get() > maxFileLength && lastAppendLocation.get().getOffset() > 0) {
            totalLength.addAndGet(lastAppendLocation.get().getOffset() - maxFileLength);
        }


        cleanupTask = new Runnable() {
            public void run() {
                cleanup();
            }
        };
        this.timer = new Timer("KahaDB Scheduler", true);
        TimerTask task = new SchedulerTimerTask(cleanupTask);
        this.timer.scheduleAtFixedRate(task, DEFAULT_CLEANUP_INTERVAL,DEFAULT_CLEANUP_INTERVAL);
        long end = System.currentTimeMillis();
        LOG.trace("Startup took: "+(end-start)+" ms");
    }


    public void preallocateEntireJournalDataFile(RecoverableRandomAccessFile file) {

        if (PreallocationScope.ENTIRE_JOURNAL == preallocationScope) {

            if (PreallocationStrategy.OS_KERNEL_COPY == preallocationStrategy) {
                doPreallocationKernelCopy(file);

            }else if (PreallocationStrategy.ZEROS == preallocationStrategy) {
                doPreallocationZeros(file);
            }
            else {
                doPreallocationSparseFile(file);
            }
        }else {
            LOG.info("Using journal preallocation scope of batch allocation");
        }
    }

    private void doPreallocationSparseFile(RecoverableRandomAccessFile file) {
        try {
            file.seek(maxFileLength - 1);
            file.write((byte)0x00);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with sparse file! Will continue without preallocation", e);
        }
    }

    private void doPreallocationZeros(RecoverableRandomAccessFile file) {
        ByteBuffer buffer = ByteBuffer.allocate(maxFileLength);
        for (int i = 0; i < maxFileLength; i++) {
            buffer.put((byte) 0x00);
        }
        buffer.flip();

        try {
            FileChannel channel = file.getChannel();
            channel.write(buffer);
            channel.force(false);
            channel.position(0);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with zeros! Will continue without preallocation", e);
        }
    }

    private void doPreallocationKernelCopy(RecoverableRandomAccessFile file) {

        // create a template file that will be used to pre-allocate the journal files
        File templateFile = createJournalTemplateFile();

        RandomAccessFile templateRaf = null;
        try {
            templateRaf = new RandomAccessFile(templateFile, "rw");
            templateRaf.setLength(maxFileLength);
            templateRaf.getChannel().force(true);
            templateRaf.getChannel().transferTo(0, getMaxFileLength(), file.getChannel());
            templateRaf.close();
            templateFile.delete();
        } catch (FileNotFoundException e) {
            LOG.error("Could not find the template file on disk at " + templateFile.getAbsolutePath(), e);
        } catch (IOException e) {
            LOG.error("Could not transfer the template file to journal, transferFile=" + templateFile.getAbsolutePath(), e);
        }
    }

    private File createJournalTemplateFile() {
        String fileName = "db-log.template";
        File rc  = new File(directory, fileName);
        if (rc.exists()) {
            System.out.println("deleting file because it already exists...");
            rc.delete();

        }
        return rc;
    }

    private static byte[] bytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    protected Location recoveryCheck(DataFile dataFile) throws IOException {
        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            while( true ) {
                int size = checkBatchRecord(reader, location.getOffset());
                if ( size>=0 && location.getOffset()+BATCH_CONTROL_RECORD_SIZE+size <= dataFile.getLength()) {
                    location.setOffset(location.getOffset()+BATCH_CONTROL_RECORD_SIZE+size);
                } else {

                    // Perhaps it's just some corruption... scan through the file to find the next valid batch record.  We
                    // may have subsequent valid batch records.
                    int nextOffset = findNextBatchRecord(reader, location.getOffset()+1);
                    if( nextOffset >=0 ) {
                        Sequence sequence = new Sequence(location.getOffset(), nextOffset - 1);
                        LOG.info("Corrupt journal records found in '"+dataFile.getFile()+"' between offsets: "+sequence);
                        dataFile.corruptedBlocks.add(sequence);
                        location.setOffset(nextOffset);
                    } else {
                        break;
                    }
                }
            }

        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }

        int existingLen = dataFile.getLength();
        dataFile.setLength(location.getOffset());
        if (existingLen > dataFile.getLength()) {
            totalLength.addAndGet(dataFile.getLength() - existingLen);
        }

        if( !dataFile.corruptedBlocks.isEmpty() ) {
            // Is the end of the data file corrupted?
            if( dataFile.corruptedBlocks.getTail().getLast()+1 == location.getOffset() ) {
                dataFile.setLength((int) dataFile.corruptedBlocks.removeLastSequence().getFirst());
            }
        }

        return location;
    }

    private int findNextBatchRecord(DataFileAccessor reader, int offset) throws IOException {
        ByteSequence header = new ByteSequence(BATCH_CONTROL_RECORD_HEADER);
        byte data[] = new byte[1024*4];
        ByteSequence bs = new ByteSequence(data, 0, reader.read(offset, data));

        int pos = 0;
        while( true ) {
            pos = bs.indexOf(header, pos);
            if( pos >= 0 ) {
                return offset+pos;
            } else {
                // need to load the next data chunck in..
                if( bs.length != data.length ) {
                    // If we had a short read then we were at EOF
                    return -1;
                }
                offset += bs.length-BATCH_CONTROL_RECORD_HEADER.length;
                bs = new ByteSequence(data, 0, reader.read(offset, data));
                pos=0;
            }
        }
    }


    public int checkBatchRecord(DataFileAccessor reader, int offset) throws IOException {
        byte controlRecord[] = new byte[BATCH_CONTROL_RECORD_SIZE];
        DataByteArrayInputStream controlIs = new DataByteArrayInputStream(controlRecord);

        reader.readFully(offset, controlRecord);

        // Assert that it's  a batch record.
        for( int i=0; i < BATCH_CONTROL_RECORD_HEADER.length; i++ ) {
            if( controlIs.readByte() != BATCH_CONTROL_RECORD_HEADER[i] ) {
                return -1;
            }
        }

        int size = controlIs.readInt();
        if( size > MAX_BATCH_SIZE ) {
            return -1;
        }

        if( isChecksum() ) {

            long expectedChecksum = controlIs.readLong();
            if( expectedChecksum == 0 ) {
                // Checksuming was not enabled when the record was stored.
                // we can't validate the record :(
                return size;
            }

            byte data[] = new byte[size];
            reader.readFully(offset+BATCH_CONTROL_RECORD_SIZE, data);

            Checksum checksum = new Adler32();
            checksum.update(data, 0, data.length);

            if( expectedChecksum!=checksum.getValue() ) {
                return -1;
            }

        }
        return size;
    }


    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    public long length() {
        return totalLength.get();
    }

    synchronized DataFile getCurrentWriteFile() throws IOException {
        if (dataFiles.isEmpty()) {
            rotateWriteFile();
        }
        return dataFiles.getTail();
    }

    synchronized DataFile rotateWriteFile() {
        int nextNum = !dataFiles.isEmpty() ? dataFiles.getTail().getDataFileId().intValue() + 1 : 1;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        fileMap.put(nextWriteFile.getDataFileId(), nextWriteFile);
        fileByFileMap.put(file, nextWriteFile);
        dataFiles.addLast(nextWriteFile);
        return nextWriteFile;
    }

    public File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    synchronized DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    synchronized File getFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile.getFile();
    }

    private DataFile getNextDataFile(DataFile dataFile) {
        return dataFile.getNext();
    }

    public void close() throws IOException {
        synchronized (this) {
            if (!started) {
                return;
            }
            if (this.timer != null) {
                this.timer.cancel();
            }
            accessorPool.close();
        }
        // the appender can be calling back to to the journal blocking a close AMQ-5620
        appender.close();
        synchronized (this) {
            fileMap.clear();
            fileByFileMap.clear();
            dataFiles.clear();
            lastAppendLocation.set(null);
            started = false;
        }
    }

    protected synchronized void cleanup() {
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
        totalLength.set(0);
        fileMap.clear();
        fileByFileMap.clear();
        lastAppendLocation.set(null);
        dataFiles = new LinkedNodeList<DataFile>();

        // reopen open file handles...
        accessorPool = new DataFileAccessorPool(this);
        appender = new DataFileAppender(this);
        return result;
    }

    public synchronized void removeDataFiles(Set<Integer> files) throws IOException {
        for (Integer key : files) {
            // Can't remove the data file (or subsequent files) that is currently being written to.
            if( key >= lastAppendLocation.get().getDataFileId() ) {
                continue;
            }
            DataFile dataFile = fileMap.get(key);
            if( dataFile!=null ) {
                forceRemoveDataFile(dataFile);
            }
        }
    }

    private synchronized void forceRemoveDataFile(DataFile dataFile) throws IOException {
        accessorPool.disposeDataFileAccessors(dataFile);
        fileByFileMap.remove(dataFile.getFile());
        fileMap.remove(dataFile.getDataFileId());
        totalLength.addAndGet(-dataFile.getLength());
        dataFile.unlink();
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
            if ( dataFile.delete() ) {
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

    public synchronized Location getNextLocation(Location location) throws IOException, IllegalStateException {

        Location cur = null;
        while (true) {
            if (cur == null) {
                if (location == null) {
                    DataFile head = dataFiles.getHead();
                    if( head == null ) {
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
                dataFile = getNextDataFile(dataFile);
                if (dataFile == null) {
                    return null;
                } else {
                    cur.setDataFileId(dataFile.getDataFileId().intValue());
                    cur.setOffset(0);
                }
            }

            // Load in location size and type.
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                reader.readLocationDetails(cur);
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }

            Sequence corruptedRange = dataFile.corruptedBlocks.get(cur.getOffset());
            if (corruptedRange != null) {
                // skip corruption
                cur.setSize((int) corruptedRange.range());
            } else if (cur.getType() == 0) {
                // eof - jump to next datafile
                cur.setOffset(maxFileLength);
            } else if (cur.getType() == USER_RECORD_TYPE) {
                // Only return user records.
                return cur;
            }
        }
    }

    public synchronized ByteSequence read(Location location) throws IOException, IllegalStateException {
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

    synchronized public Integer getCurrentDataFileId() {
        if (dataFiles.isEmpty())
            return null;
        return dataFiles.getTail().getDataFileId();
    }

    /**
     * Get a set of files - only valid after start()
     *
     * @return files currently being used
     */
    public Set<File> getFiles() {
        return fileByFileMap.keySet();
    }

    public synchronized Map<Integer, DataFile> getFileMap() {
        return new TreeMap<Integer, DataFile>(fileMap);
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
}
