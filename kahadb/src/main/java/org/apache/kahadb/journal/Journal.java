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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.journal.DataFileAppender.WriteCommand;
import org.apache.kahadb.journal.DataFileAppender.WriteKey;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.DataByteArrayInputStream;
import org.apache.kahadb.util.LinkedNodeList;
import org.apache.kahadb.util.Scheduler;

/**
 * Manages DataFiles
 * 
 * @version $Revision$
 */
public class Journal {

    private static final int MAX_BATCH_SIZE = 32*1024*1024;

	// ITEM_HEAD_SPACE = length + type+ reserved space + SOR
    public static final int RECORD_HEAD_SPACE = 4 + 1;
    
    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    // Batch Control Item holds a 4 byte size of the batch and a 8 byte checksum of the batch. 
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = bytes("WRITE BATCH");
    public static final int BATCH_CONTROL_RECORD_SIZE = RECORD_HEAD_SPACE+BATCH_CONTROL_RECORD_MAGIC.length+4+8;
    
    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int PREFERED_DIFF = 1024 * 512;

    private static final Log LOG = LogFactory.getLog(Journal.class);

    protected final Map<WriteKey, WriteCommand> inflightWrites = new ConcurrentHashMap<WriteKey, WriteCommand>();

    protected File directory = new File(DEFAULT_DIRECTORY);
    protected File directoryArchive = new File(DEFAULT_ARCHIVE_DIRECTORY);
    protected String filePrefix = DEFAULT_FILE_PREFIX;
    protected String fileSuffix = DEFAULT_FILE_SUFFIX;
    protected boolean started;
    
    protected int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    protected int preferedFileLength = DEFAULT_MAX_FILE_LENGTH - PREFERED_DIFF;

    protected DataFileAppender appender;
    protected DataFileAccessorPool accessorPool;

    protected Map<Integer, DataFile> fileMap = new HashMap<Integer, DataFile>();
    protected Map<File, DataFile> fileByFileMap = new LinkedHashMap<File, DataFile>();
    protected LinkedNodeList<DataFile> dataFiles = new LinkedNodeList<DataFile>();

    protected final AtomicReference<Location> lastAppendLocation = new AtomicReference<Location>();
    protected Runnable cleanupTask;
    protected final AtomicLong totalLength = new AtomicLong();
    protected boolean archiveDataLogs;
	private ReplicationTarget replicationTarget;
    protected boolean checksum;

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }
        
        long start = System.currentTimeMillis();
        accessorPool = new DataFileAccessorPool(this);
        started = true;
        preferedFileLength = Math.max(PREFERED_DIFF, getMaxFileLength() - PREFERED_DIFF);

        appender = new DataFileAppender(this);

        File[] files = directory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String n) {
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }
        });

        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                try {
                    File file = files[i];
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length()-fileSuffix.length());
                    int num = Integer.parseInt(numStr);
                    DataFile dataFile = new DataFile(file, num, preferedFileLength);
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
                dataFiles.addLast(df);
                fileByFileMap.put(df.getFile(), df);
            }
        }

    	getCurrentWriteFile();
        try {
        	Location l = recoveryCheck(dataFiles.getTail());
            lastAppendLocation.set(l);
        } catch (IOException e) {
            LOG.warn("recovery check failed", e);
        }
        
        cleanupTask = new Runnable() {
            public void run() {
                cleanup();
            }
        };
        Scheduler.executePeriodically(cleanupTask, DEFAULT_CLEANUP_INTERVAL);
        long end = System.currentTimeMillis();
        LOG.trace("Startup took: "+(end-start)+" ms");
    }

    private static byte[] bytes(String string) {
    	try {
			return string.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	protected Location recoveryCheck(DataFile dataFile) throws IOException {
    	byte controlRecord[] = new byte[BATCH_CONTROL_RECORD_SIZE];
    	DataByteArrayInputStream controlIs = new DataByteArrayInputStream(controlRecord);
    	
        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

    	DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            while( true ) {
	        	reader.read(location.getOffset(), controlRecord);
	        	controlIs.restart();
	        	
	        	// Assert that it's  a batch record.
	        	if( controlIs.readInt() != BATCH_CONTROL_RECORD_SIZE ) {
	        		break;
	        	}
	        	if( controlIs.readByte() != BATCH_CONTROL_RECORD_TYPE ) {
	        		break;
	        	}
	        	for( int i=0; i < BATCH_CONTROL_RECORD_MAGIC.length; i++ ) {
	        		if( controlIs.readByte() != BATCH_CONTROL_RECORD_MAGIC[i] ) {
	        			break;
	        		}
	        	}
	        	
	        	int size = controlIs.readInt();
	        	if( size > MAX_BATCH_SIZE ) {
	        		break;
	        	}
	        	
	        	if( isChecksum() ) {
		        	
	        		long expectedChecksum = controlIs.readLong();	        	
		        	
	        		byte data[] = new byte[size];
		        	reader.read(location.getOffset()+BATCH_CONTROL_RECORD_SIZE, data);
		        	
		        	Checksum checksum = new Adler32();
	                checksum.update(data, 0, data.length);
	                
	                if( expectedChecksum!=checksum.getValue() ) {
	                	break;
	                }
	                
	        	}
                
	        	
                location.setOffset(location.getOffset()+BATCH_CONTROL_RECORD_SIZE+size);
            }
            
        } catch (IOException e) {
		} finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        
        dataFile.setLength(location.getOffset());
        return location;
    }

	void addToTotalLength(int size) {
		totalLength.addAndGet(size);
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
		DataFile nextWriteFile = new DataFile(file, nextNum, preferedFileLength);
		// actually allocate the disk space
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

    public synchronized void close() throws IOException {
        if (!started) {
            return;
        }
        Scheduler.cancel(cleanupTask);
        accessorPool.close();
        appender.close();
        fileMap.clear();
        fileByFileMap.clear();
        dataFiles.clear();
        lastAppendLocation.set(null);
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
        for (Iterator<DataFile> i = fileMap.values().iterator(); i.hasNext();) {
            DataFile dataFile = i.next();
            totalLength.addAndGet(-dataFile.getLength());
            result &= dataFile.delete();
        }
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
            dataFile.move(getDirectoryArchive());
            LOG.debug("moved data file " + dataFile + " to " + getDirectoryArchive());
        } else {
            if ( dataFile.delete() ) {
            	LOG.debug("Discarded data file " + dataFile);
            } else {
            	LOG.warn("Failed to discard data file " + dataFile.getFile());
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
        return directory.toString();
    }

	public synchronized void appendedExternally(Location loc, int length) throws IOException {
		DataFile dataFile = null;
		if( dataFiles.getTail().getDataFileId() == loc.getDataFileId() ) {
			// It's an update to the current log file..
			dataFile = dataFiles.getTail();
			dataFile.incrementLength(length);
		} else if( dataFiles.getTail().getDataFileId()+1 == loc.getDataFileId() ) {
			// It's an update to the next log file.
            int nextNum = loc.getDataFileId();
            File file = getFile(nextNum);
            dataFile = new DataFile(file, nextNum, preferedFileLength);
            // actually allocate the disk space
            fileMap.put(dataFile.getDataFileId(), dataFile);
            fileByFileMap.put(file, dataFile);
            dataFiles.addLast(dataFile);
		} else {
			throw new IOException("Invalid external append.");
		}
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

            if (cur.getType() == 0) {
                return null;
            } else if (cur.getType() == USER_RECORD_TYPE) {
                // Only return user records.
                return cur;
            }
        }
    }

    public synchronized Location getNextLocation(File file, Location lastLocation, boolean thisFileOnly) throws IllegalStateException, IOException {
        DataFile df = fileByFileMap.get(file);
        return getNextLocation(df, lastLocation, thisFileOnly);
    }

    public synchronized Location getNextLocation(DataFile dataFile, Location lastLocation, boolean thisFileOnly) throws IOException, IllegalStateException {

        Location cur = null;
        while (true) {
            if (cur == null) {
                if (lastLocation == null) {
                    DataFile head = dataFile.getHeadNode();
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
                } else {
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

    public synchronized Location write(ByteSequence data, boolean sync) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, sync);
        return loc;
    }

    public synchronized Location write(ByteSequence data, Runnable onComplete) throws IOException, IllegalStateException {
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

    public Map<Integer, DataFile> getFileMap() {
        return new TreeMap<Integer, DataFile>(fileMap);
    }
    
    public long getDiskSize() {
        long tailLength=0;
        synchronized( this ) {
            if( !dataFiles.isEmpty() ) {
                tailLength = dataFiles.getTail().getLength();
            }
        }
        
        long rc = totalLength.get();
        
        // The last file is actually at a minimum preferedFileLength big.
        if( tailLength < preferedFileLength ) {
            rc -= tailLength;
            rc += preferedFileLength;
        }
        return rc;
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


}
