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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.kaha.impl.async.DataFileAppender.WriteCommand;
import org.apache.activemq.kaha.impl.async.DataFileAppender.WriteKey;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * Manages DataFiles
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class AsyncDataManager {

    public static final int CONTROL_RECORD_MAX_LENGTH = 1024;
    public static final int ITEM_HEAD_RESERVED_SPACE = 21;
    // ITEM_HEAD_SPACE = length + type+ reserved space + SOR
    public static final int ITEM_HEAD_SPACE = 4 + 1 + ITEM_HEAD_RESERVED_SPACE + 3;
    public static final int ITEM_HEAD_OFFSET_TO_SOR = ITEM_HEAD_SPACE - 3;
    public static final int ITEM_FOOT_SPACE = 3; // EOR

    public static final int ITEM_HEAD_FOOT_SPACE = ITEM_HEAD_SPACE + ITEM_FOOT_SPACE;

    public static final byte[] ITEM_HEAD_SOR = new byte[] {'S', 'O', 'R'}; // 
    public static final byte[] ITEM_HEAD_EOR = new byte[] {'E', 'O', 'R'}; // 

    public static final byte DATA_ITEM_TYPE = 1;
    public static final byte REDO_ITEM_TYPE = 2;
    public static final String DEFAULT_DIRECTORY = "data";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "data-";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int PREFERED_DIFF = 1024 * 512;

    private static final Log LOG = LogFactory.getLog(AsyncDataManager.class);
    protected static Scheduler scheduler  = Scheduler.getInstance();

    protected final Map<WriteKey, WriteCommand> inflightWrites = new ConcurrentHashMap<WriteKey, WriteCommand>();

    protected File directory = new File(DEFAULT_DIRECTORY);
    protected File directoryArchive = new File (DEFAULT_ARCHIVE_DIRECTORY);
    protected String filePrefix = DEFAULT_FILE_PREFIX;
    protected ControlFile controlFile;
    protected boolean started;
    protected boolean useNio = true;

    protected int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    protected int preferedFileLength = DEFAULT_MAX_FILE_LENGTH - PREFERED_DIFF;

    protected DataFileAppender appender;
    protected DataFileAccessorPool accessorPool = new DataFileAccessorPool(this);

    protected Map<Integer, DataFile> fileMap = new HashMap<Integer, DataFile>();
    protected Map<File, DataFile> fileByFileMap = new LinkedHashMap<File, DataFile>();
    protected DataFile currentWriteFile;

    protected Location mark;
    protected final AtomicReference<Location> lastAppendLocation = new AtomicReference<Location>();
    protected Runnable cleanupTask;
    protected final AtomicLong storeSize;
    protected boolean archiveDataLogs;
    
    public AsyncDataManager(AtomicLong storeSize) {
        this.storeSize=storeSize;
    }
    
    public AsyncDataManager() {
        this(new AtomicLong());
    }

    @SuppressWarnings("unchecked")
    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        started = true;
        preferedFileLength=Math.max(PREFERED_DIFF, getMaxFileLength()-PREFERED_DIFF);
        lock();

        ByteSequence sequence = controlFile.load();
        if (sequence != null && sequence.getLength() > 0) {
            unmarshallState(sequence);
        }
        if (useNio) {
            appender = new NIODataFileAppender(this);
        } else {
            appender = new DataFileAppender(this);
        }

        File[] files = directory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix);
            }
        });
       
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                try {
                    File file = files[i];
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length());
                    int num = Integer.parseInt(numStr);
                    DataFile dataFile = new DataFile(file, num, preferedFileLength);
                    fileMap.put(dataFile.getDataFileId(), dataFile);
                    storeSize.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }

            // Sort the list so that we can link the DataFiles together in the
            // right order.
            List<DataFile> l = new ArrayList<DataFile>(fileMap.values());
            Collections.sort(l);
            currentWriteFile = null;
            for (DataFile df : l) {
                if (currentWriteFile != null) {
                    currentWriteFile.linkAfter(df);
                }
                currentWriteFile = df;
                fileByFileMap.put(df.getFile(), df);
            }
        }

        // Need to check the current Write File to see if there was a partial
        // write to it.
        if (currentWriteFile != null) {

            // See if the lastSyncedLocation is valid..
            Location l = lastAppendLocation.get();
            if (l != null && l.getDataFileId() != currentWriteFile.getDataFileId().intValue()) {
                l = null;
            }

            // If we know the last location that was ok.. then we can skip lots
            // of checking
            try{
            l = recoveryCheck(currentWriteFile, l);
            lastAppendLocation.set(l);
            }catch(IOException e){
            	LOG.warn("recovery check failed", e);
            }
        }

        storeState(false);

        cleanupTask = new Runnable() {
            public void run() {
                cleanup();
            }
        };
        scheduler.executePeriodically(cleanupTask, DEFAULT_CLEANUP_INTERVAL);
    }

    public void lock() throws IOException {
        synchronized (this) {
            if (controlFile == null) {
                IOHelper.mkdirs(directory);
                controlFile = new ControlFile(new File(directory, filePrefix + "control"), CONTROL_RECORD_MAX_LENGTH);
            }
            controlFile.lock();
        }
    }

    protected Location recoveryCheck(DataFile dataFile, Location location) throws IOException {
        if (location == null) {
            location = new Location();
            location.setDataFileId(dataFile.getDataFileId());
            location.setOffset(0);
        }
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            reader.readLocationDetails(location);
            while (reader.readLocationDetailsAndValidate(location)) {
                location.setOffset(location.getOffset() + location.getSize());
            }
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        dataFile.setLength(location.getOffset());
        return location;
    }

    protected void unmarshallState(ByteSequence sequence) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(sequence.getData(), sequence.getOffset(), sequence.getLength());
        DataInputStream dis = new DataInputStream(bais);
        if (dis.readBoolean()) {
            mark = new Location();
            mark.readExternal(dis);
        } else {
            mark = null;
        }
        if (dis.readBoolean()) {
            Location l = new Location();
            l.readExternal(dis);
            lastAppendLocation.set(l);
        } else {
            lastAppendLocation.set(null);
        }
    }

    private synchronized ByteSequence marshallState() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        if (mark != null) {
            dos.writeBoolean(true);
            mark.writeExternal(dos);
        } else {
            dos.writeBoolean(false);
        }
        Location l = lastAppendLocation.get();
        if (l != null) {
            dos.writeBoolean(true);
            l.writeExternal(dos);
        } else {
            dos.writeBoolean(false);
        }

        byte[] bs = baos.toByteArray();
        return new ByteSequence(bs, 0, bs.length);
    }

    synchronized DataFile allocateLocation(Location location) throws IOException {
        if (currentWriteFile == null || ((currentWriteFile.getLength() + location.getSize()) > maxFileLength)) {
            int nextNum = currentWriteFile != null ? currentWriteFile.getDataFileId().intValue() + 1 : 1;

            String fileName = filePrefix + nextNum;
            File file = new File(directory, fileName);
            DataFile nextWriteFile = new DataFile(file, nextNum, preferedFileLength);
            //actually allocate the disk space
            nextWriteFile.closeRandomAccessFile(nextWriteFile.openRandomAccessFile(true));
            fileMap.put(nextWriteFile.getDataFileId(), nextWriteFile);
            fileByFileMap.put(file, nextWriteFile);
            if (currentWriteFile != null) {
                currentWriteFile.linkAfter(nextWriteFile);
                if (currentWriteFile.isUnused()) {
                    removeDataFile(currentWriteFile);
                }
            }
            currentWriteFile = nextWriteFile;

        }
        location.setOffset(currentWriteFile.getLength());
        location.setDataFileId(currentWriteFile.getDataFileId().intValue());
        int size = location.getSize();
        currentWriteFile.incrementLength(size);
        currentWriteFile.increment();
        storeSize.addAndGet(size);
        return currentWriteFile;
    }
    
    public synchronized void removeLocation(Location location) throws IOException{
       
        DataFile dataFile = getDataFile(location);
        dataFile.decrement();
    }

    synchronized DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + filePrefix + item.getDataFileId());
        }
        return dataFile;
    }
    
    synchronized File getFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = fileMap.get(key);
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + filePrefix  + item.getDataFileId());
        }
        return dataFile.getFile();
    }

    private DataFile getNextDataFile(DataFile dataFile) {
        return (DataFile)dataFile.getNext();
    }

    public synchronized void close() throws IOException {
        if (!started) {
            return;
        }
        scheduler.cancel(cleanupTask);
        accessorPool.close();
        storeState(false);
        appender.close();
        fileMap.clear();
        fileByFileMap.clear();
        controlFile.unlock();
        controlFile.dispose();
        started = false;
    }

    synchronized void cleanup() {
        if (accessorPool != null) {
            accessorPool.disposeUnused();
        }
    }

    public synchronized boolean delete() throws IOException {

        // Close all open file handles...
        appender.close();
        accessorPool.close();

        boolean result = true;
        for (Iterator i = fileMap.values().iterator(); i.hasNext();) {
            DataFile dataFile = (DataFile)i.next();
            storeSize.addAndGet(-dataFile.getLength());
            result &= dataFile.delete();
        }
        fileMap.clear();
        fileByFileMap.clear();
        lastAppendLocation.set(null);
        mark = null;
        currentWriteFile = null;

        // reopen open file handles...
        accessorPool = new DataFileAccessorPool(this);
        if (useNio) {
            appender = new NIODataFileAppender(this);
        } else {
            appender = new DataFileAppender(this);
        }
        return result;
    }

    public synchronized void addInterestInFile(int file) throws IOException {
        if (file >= 0) {
            Integer key = Integer.valueOf(file);
            DataFile dataFile = (DataFile)fileMap.get(key);
            if (dataFile == null) {
                throw new IOException("That data file does not exist");
            }
            addInterestInFile(dataFile);
        }
    }

    synchronized void addInterestInFile(DataFile dataFile) {
        if (dataFile != null) {
            dataFile.increment();
        }
    }

    public synchronized void removeInterestInFile(int file) throws IOException {
        if (file >= 0) {
            Integer key = Integer.valueOf(file);
            DataFile dataFile = (DataFile)fileMap.get(key);
            removeInterestInFile(dataFile);
        }
       
    }

    synchronized void removeInterestInFile(DataFile dataFile) throws IOException {
        if (dataFile != null) {
            if (dataFile.decrement() <= 0) {
                removeDataFile(dataFile);
            }
        }
    }

    public synchronized void consolidateDataFilesNotIn(Set<Integer> inUse, Set<Integer>inProgress) throws IOException {
        Set<Integer> unUsed = new HashSet<Integer>(fileMap.keySet());
        unUsed.removeAll(inUse);
        unUsed.removeAll(inProgress);
                
        List<DataFile> purgeList = new ArrayList<DataFile>();
        for (Integer key : unUsed) {
            DataFile dataFile = (DataFile)fileMap.get(key);
            purgeList.add(dataFile);
        }
        for (DataFile dataFile : purgeList) {
            if (dataFile.getDataFileId() != currentWriteFile.getDataFileId()) {
                forceRemoveDataFile(dataFile);
            }
        }
    }

    public synchronized void consolidateDataFilesNotIn(Set<Integer> inUse, Integer lastFile) throws IOException {
        Set<Integer> unUsed = new HashSet<Integer>(fileMap.keySet());
        unUsed.removeAll(inUse);
                
        List<DataFile> purgeList = new ArrayList<DataFile>();
        for (Integer key : unUsed) {
        	// Only add files less than the lastFile..
        	if( key.intValue() < lastFile.intValue() ) {
                DataFile dataFile = (DataFile)fileMap.get(key);
                purgeList.add(dataFile);
        	}
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("lastFileId=" + lastFile + ", purgeList: (" + purgeList.size() + ") " + purgeList);
        }
        for (DataFile dataFile : purgeList) {
            forceRemoveDataFile(dataFile);
        }
	}

    public synchronized void consolidateDataFiles() throws IOException {
        List<DataFile> purgeList = new ArrayList<DataFile>();
        for (DataFile dataFile : fileMap.values()) {
            if (dataFile.isUnused()) {
                purgeList.add(dataFile);
            }
        }
        for (DataFile dataFile : purgeList) {
            removeDataFile(dataFile);
        }
    }

    private synchronized void removeDataFile(DataFile dataFile) throws IOException {

        // Make sure we don't delete too much data.
        if (dataFile == currentWriteFile || mark == null || dataFile.getDataFileId() >= mark.getDataFileId()) {
            LOG.debug("Won't remove DataFile" + dataFile);
        	return;
        }
        forceRemoveDataFile(dataFile);
    }
    
    private synchronized void forceRemoveDataFile(DataFile dataFile)
            throws IOException {
        accessorPool.disposeDataFileAccessors(dataFile);
        fileByFileMap.remove(dataFile.getFile());
        fileMap.remove(dataFile.getDataFileId());
        storeSize.addAndGet(-dataFile.getLength());
        dataFile.unlink();
        if (archiveDataLogs) {
            dataFile.move(getDirectoryArchive());
            LOG.debug("moved data file " + dataFile + " to "
                    + getDirectoryArchive());
        } else {
            boolean result = dataFile.delete();
            if (!result) {
                LOG.info("Failed to discard data file " + dataFile);
            }
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

    public String toString() {
        return "DataManager:(" + filePrefix + ")";
    }

    public synchronized Location getMark() throws IllegalStateException {
        return mark;
    }

    public synchronized Location getNextLocation(Location location) throws IOException, IllegalStateException {

        Location cur = null;
        while (true) {
            if (cur == null) {
                if (location == null) {
                    DataFile head = (DataFile)currentWriteFile.getHeadNode();
                    cur = new Location();
                    cur.setDataFileId(head.getDataFileId());
                    cur.setOffset(0);
                } else {
                    // Set to the next offset..
                	if( location.getSize() == -1 ) {
                		cur = new Location(location);
                	}  else {
	            		cur = new Location(location);
	            		cur.setOffset(location.getOffset()+location.getSize());
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

            if (cur.getType() == 0) {
                return null;
            } else if (cur.getType() > 0) {
                // Only return user records.
                return cur;
            }
        }
    }
    
    public synchronized Location getNextLocation(File file, Location lastLocation,boolean thisFileOnly) throws IllegalStateException, IOException{
        DataFile df = fileByFileMap.get(file);
        return getNextLocation(df, lastLocation,thisFileOnly);
    }
    
    public synchronized Location getNextLocation(DataFile dataFile,
            Location lastLocation,boolean thisFileOnly) throws IOException, IllegalStateException {

        Location cur = null;
        while (true) {
            if (cur == null) {
                if (lastLocation == null) {
                    DataFile head = (DataFile)dataFile.getHeadNode();
                    cur = new Location();
                    cur.setDataFileId(head.getDataFileId());
                    cur.setOffset(0);
                } else {
                    // Set to the next offset..
                    cur = new Location(lastLocation);
                    cur.setOffset(cur.getOffset() + cur.getSize());
                }
            } else {
                cur.setOffset(cur.getOffset() + cur.getSize());
            }

            
            // Did it go into the next file??
            if (dataFile.getLength() <= cur.getOffset()) {
                if (thisFileOnly) {
                    return null;
                }else {
                dataFile = getNextDataFile(dataFile);
                if (dataFile == null) {
                    return null;
                } else {
                    cur.setDataFileId(dataFile.getDataFileId().intValue());
                    cur.setOffset(0);
                }
                }
            }

            // Load in location size and type.
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                reader.readLocationDetails(cur);
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }

            if (cur.getType() == 0) {
                return null;
            } else if (cur.getType() > 0) {
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

    public void setMark(Location location, boolean sync) throws IOException, IllegalStateException {
        synchronized (this) {
            mark = location;
        }
        storeState(sync);
    }

    protected synchronized void storeState(boolean sync) throws IOException {
        ByteSequence state = marshallState();
        appender.storeItem(state, Location.MARK_TYPE, sync);
        controlFile.store(state, sync);
    }

    public synchronized Location write(ByteSequence data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, sync);
        return loc;
    }
    
    public synchronized Location write(ByteSequence data, Runnable onComplete) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, onComplete);
        return loc;
    }

    public synchronized Location write(ByteSequence data, byte type, boolean sync) throws IOException, IllegalStateException {
        return appender.storeItem(data, type, sync);
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
        this.filePrefix = IOHelper.toFileSystemSafeName(filePrefix);
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

	public boolean isUseNio() {
		return useNio;
	}

	public void setUseNio(boolean useNio) {
		this.useNio = useNio;
	}
	
	public File getDirectoryArchive() {
        return directoryArchive;
    }

    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }
    
    public boolean isArchiveDataLogs() {
        return archiveDataLogs;
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    synchronized public Integer getCurrentDataFileId() {
        if( currentWriteFile==null )
            return null;
        return currentWriteFile.getDataFileId();
    }
    
    /**
     * Get a set of files - only valid after start()
     * @return files currently being used
     */
    public Set<File> getFiles(){
        return fileByFileMap.keySet();
    }

	synchronized public long getDiskSize() {
		long rc=0;
        DataFile cur = (DataFile)currentWriteFile.getHeadNode();
        while( cur !=null ) {
        	rc += cur.getLength();
        	cur = (DataFile) cur.getNext();
        }
		return rc;
	}

	synchronized public long getDiskSizeUntil(Location startPosition) {
		long rc=0;
        DataFile cur = (DataFile)currentWriteFile.getHeadNode();
        while( cur !=null ) {
        	if( cur.getDataFileId().intValue() >= startPosition.getDataFileId() ) {
        		return rc + startPosition.getOffset();
        	}
        	rc += cur.getLength();
        	cur = (DataFile) cur.getNext();
        }
		return rc;
	}

}
