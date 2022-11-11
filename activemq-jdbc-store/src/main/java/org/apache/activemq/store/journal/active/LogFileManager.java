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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.activemq.store.journal.adapter.PacketOutputStream;
import org.apache.activemq.store.journal.adapter.PacketToInputStream;
import org.apache.activemq.store.journal.InvalidRecordLocationException;
import org.apache.activemq.store.journal.packet.ByteArrayPacket;
import org.apache.activemq.store.journal.packet.ByteBufferPacket;
import org.apache.activemq.store.journal.packet.Packet;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a logical view of many separate files as one single long log file.
 * The separate files that compose the LogFile are Segments of the LogFile.
 * <p/>This class is not thread safe.
 * 
 * @version $Revision: 1.1 $
 */
final public class LogFileManager {

    public static final int DEFAULT_LOGFILE_COUNT = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultLogFileCount", ""+(2)));
    public static final int DEFAULT_LOGFILE_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.journal.active.DefaultLogFileSize", ""+(1024*1024*20)));

    static final public int SERIALIZED_SIZE = 6+Location.SERIALIZED_SIZE;

    static final public byte DATA_RECORD_TYPE = 1;
    static final public byte MARK_RECORD_TYPE = 2;
    static final private NumberFormat onlineLogNameFormat = NumberFormat.getNumberInstance();
    static {
        onlineLogNameFormat.setMinimumIntegerDigits(3);
        onlineLogNameFormat.setMaximumIntegerDigits(3);
        onlineLogNameFormat.setGroupingUsed(false);
        onlineLogNameFormat.setParseIntegerOnly(true);
        onlineLogNameFormat.setMaximumFractionDigits(0);
    }
    
    static final private NumberFormat archiveLogNameFormat = NumberFormat.getNumberInstance();
    static {
        archiveLogNameFormat.setMinimumIntegerDigits(8);
        archiveLogNameFormat.setMaximumIntegerDigits(8);
        archiveLogNameFormat.setGroupingUsed(false);
        archiveLogNameFormat.setParseIntegerOnly(true);
        archiveLogNameFormat.setMaximumFractionDigits(0);
    }

    // Config
    private final File logDirectory;
    private final int initialLogFileSize;
    private final int onlineLogFileCount;
    private final AtomicInteger activeLogFileCount = new AtomicInteger(0);
    
    // Keeps track of the online log file.
    private LogFileNode firstNode;
    private LogFileNode firstActiveNode;
    private LogFileNode firstInactiveNode;
    private LogFileNode appendNode;

    private ControlFile controlFile;
    private int lastLogFileId = -1;
    private Location lastMark;
    private boolean disposed;
    private boolean loadedFromCleanShutDown;
    
    private File archiveDirectory;
    HashMap openArchivedLogs = new HashMap();

    public LogFileManager(File logDirectory) throws IOException {
        this(logDirectory, DEFAULT_LOGFILE_COUNT, DEFAULT_LOGFILE_SIZE, null);
    }

    public LogFileManager(File logDirectory, int onlineLogFileCount, int initialLogFileSize, File archiveDirectory) throws IOException {
        this.logDirectory = logDirectory;
        this.onlineLogFileCount = onlineLogFileCount;
        this.initialLogFileSize = initialLogFileSize;
        initialize(onlineLogFileCount);
        this.archiveDirectory=archiveDirectory;
    }

    void initialize(int onlineLogFileCount) throws IOException {
      try
      {
        LogFileNode logFiles[] = new LogFileNode[onlineLogFileCount];

        // Create the log directory if it does not exist.
        if (!logDirectory.exists()) {
            if (!logDirectory.mkdirs()) {
                throw new IOException("Could not create directory: " + logDirectory);
            }
        }

        // Open the control file.        
        int controlDataSize = SERIALIZED_SIZE + (LogFileNode.SERIALIZED_SIZE*onlineLogFileCount);
        controlFile = new ControlFile(new File(logDirectory, "control.dat"),  controlDataSize);
        // Make sure we are the only process using the control file.
        controlFile.lock();
        
        // Initialize the nodes.
        for (int i = 0; i < onlineLogFileCount; i++) {
            LogFile file = new LogFile(new File(logDirectory, "log-" + onlineLogNameFormat.format(i) + ".dat"),
                    initialLogFileSize);
            logFiles[i] = new LogFileNode(file);
        }

        // Link the nodes together.
        for (int i = 0; i < onlineLogFileCount; i++) {
            if (i == (onlineLogFileCount - 1)) {
                logFiles[i].setNext(logFiles[0]);
            } else {
                logFiles[i].setNext(logFiles[i + 1]);
            }
        }
        
        firstNode = logFiles[0];
        loadState();

        // Find the first active node
        for (int i = 0; i < onlineLogFileCount; i++) {
            if( logFiles[i].isActive() ) {
                activeLogFileCount.incrementAndGet();
                if( firstActiveNode == null || logFiles[i].getId() < firstActiveNode.getId() ) {
                    firstActiveNode = logFiles[i];
                }
            }
        }
        
        // None was active? activate one.
        if ( firstActiveNode == null ) {
            firstInactiveNode = logFiles[0];
            activateNextLogFile();
        } else {            
            // Find the append log and the first inactive node
            firstInactiveNode = null;
            LogFileNode log = firstActiveNode;
            do {
                if( !log.isActive() ) {
                    firstInactiveNode = log;
                    break;
                } else {
                    appendNode = log;
                }
                log = log.getNext();
            } while (log != firstActiveNode);
        }
        
        // If we did not have a clean shut down then we have to check the state 
        // of the append log.
        if( !this.loadedFromCleanShutDown ) {
            checkAppendLog();
        }
                    
        loadedFromCleanShutDown = false;
        storeState();
      }
      catch (JournalLockedException e)
      {
        controlFile.dispose();
        throw e;
      }
    }

    private void checkAppendLog() throws IOException {
        
        // We are trying to get the true append offset and the last Mark that was written in 
        // the append log.
        
        int offset = 0;
        Record record = new Record();
        LogFile logFile = appendNode.getLogFile();
        Location markLocation=null;
        
        while( logFile.loadAndCheckRecord(offset, record) ) {
            
            if( record.getLocation().getLogFileId()!= appendNode.getId() || record.getLocation().getLogFileOffset()!=offset ) {
                // We must have run past the end of the append location.
                break;
            }

            if ( record.getRecordType()==LogFileManager.MARK_RECORD_TYPE) {
                markLocation = record.getLocation();
            }
            
            offset += record.getRecordLength();            
        }
        
        appendNode.setAppendOffset(offset);
        
        if( markLocation!=null ) {
            try {
                Packet packet = readPacket(markLocation);
                markLocation = Location.readFromPacket(packet);
            } catch (InvalidRecordLocationException e) {
                throw (IOException)new IOException(e.getMessage()).initCause(e);
            }
            updateMark(markLocation);
        }
        
    }

    private void storeState() throws IOException {
        Packet controlData = controlFile.getControlData();
        if( controlData.remaining() == 0 )
            return;
        
        DataOutput data = new DataOutputStream(new PacketOutputStream(controlData));

        data.writeInt(lastLogFileId);
        data.writeBoolean(lastMark!=null);
        if( lastMark!=null )
            lastMark.writeToDataOutput(data);
        data.writeBoolean(loadedFromCleanShutDown);
        
        // Load each node's state
        LogFileNode log = firstNode;
        do {            
            log.writeExternal( data );
            log = log.getNext();
        } while (log != firstNode);
        
        controlFile.store();
    }

    private void loadState() throws IOException {
        if( controlFile.load() ) {
            Packet controlData = controlFile.getControlData();
            if( controlData.remaining() == 0 )
                return;
            
            DataInput data = new DataInputStream(new PacketToInputStream(controlData));
    
            lastLogFileId =data.readInt();
            if( data.readBoolean() )
                lastMark = Location.readFromDataInput(data);
            else
                lastMark = null;
            loadedFromCleanShutDown = data.readBoolean();
    
            // Load each node's state
            LogFileNode log = firstNode;
            do {            
                log.readExternal( data );
                log = log.getNext();
            } while (log != firstNode);
        }
    }

    public void dispose() {

        if (disposed)
            return;
        this.disposed = true;
        try {
	        // Close all the opened log files.
	        LogFileNode log = firstNode;
	        do {
	            log.getLogFile().dispose();
	            log = log.getNext();
	        } while (log != firstNode);

			Iterator iter = openArchivedLogs.values().iterator();
			while (iter.hasNext()) {
			    ((LogFile)iter.next()).dispose();
            }

            loadedFromCleanShutDown=true;
	        storeState();
	        controlFile.dispose();
        } catch ( IOException e ) {        	
        }
        
    }

    private int getNextLogFileId() {
        return ++lastLogFileId;
    }

    /**
     * @param write
     * @throws IOException
     */
    public void append(BatchedWrite write) throws IOException {

        if (!appendNode.isActive())
            throw new IllegalStateException("Log file is not active.  Writes are not allowed");
        if (appendNode.isReadOnly())
            throw new IllegalStateException("Log file has been marked Read Only.  Writes are not allowed");

        // Write and force the data to disk.
        LogFile logFile = appendNode.getLogFile();
        ByteBuffer buffer = ((ByteBufferPacket)write.getPacket().getAdapter(ByteBufferPacket.class)).getByteBuffer();
        int size = buffer.remaining();
        logFile.write(appendNode.getAppendOffset(), buffer);
        if( write.getForce() )
            logFile.force();

        // Update state
        appendNode.appended(size);
        if (write.getMark() != null) {
            updateMark(write.getMark());
        }
    }

    /**
     * @param write
     * @throws IOException
     */
    synchronized private void updateMark(Location mark) throws IOException {
        // If we wrote a mark we may need to deactivate some log files.
        this.lastMark = mark;
        while (firstActiveNode != appendNode) {
            if (firstActiveNode.getId() < lastMark.getLogFileId()) {
                
                if( archiveDirectory!=null ) {
                    File file = getArchiveFile(firstActiveNode.getId());
                    firstActiveNode.getLogFile().copyTo(file);
                }
                
                firstActiveNode.deactivate();
                activeLogFileCount.decrementAndGet();
                if( firstInactiveNode == null )
                    firstInactiveNode = firstActiveNode;
                firstActiveNode = firstActiveNode.getNextActive();
                
            } else {
                break;
            }
        }
    }
    
    private File getArchiveFile(int logId) {
        return new File(archiveDirectory, "" + archiveLogNameFormat.format(logId) + ".log");
    }
    
    RecordInfo readRecordInfo(Location location) throws IOException, InvalidRecordLocationException {

        LogFile logFile;
        LogFileNode logFileState = getLogFileWithId(location.getLogFileId());
        if( logFileState !=null ) {
            // There can be no record at the append offset.
            if (logFileState.getAppendOffset() == location.getLogFileOffset()) {
                throw new InvalidRecordLocationException("No record at (" + location
                        + ") found.  Location past end of logged data.");
            }
            logFile = logFileState.getLogFile();
        } else {
            if( archiveDirectory==null ) {
                throw new InvalidRecordLocationException("Log file: " + location.getLogFileId() + " is not active.");
            } else {
                logFile = getArchivedLogFile(location.getLogFileId());
            }
        }

        // Is there a record header at the seeked location?
        try {
            Record header = new Record();
            logFile.readRecordHeader(location.getLogFileOffset(), header);
            return new RecordInfo(location, header, logFileState, logFile);
        } catch (IOException e) {
            throw new InvalidRecordLocationException("No record at (" + location + ") found.");
        }
    }

    private LogFile getArchivedLogFile(int logFileId) throws InvalidRecordLocationException, IOException {
        Integer key = new Integer(logFileId);
        LogFile rc = (LogFile) openArchivedLogs.get(key);
        if( rc == null ) {
            File archiveFile = getArchiveFile(logFileId);
            if( !archiveFile.canRead() )
                throw new InvalidRecordLocationException("Log file: " + logFileId + " does not exist.");
            rc = new LogFile(archiveFile, getInitialLogFileSize());
            openArchivedLogs.put(key, rc);
            
            // TODO: turn openArchivedLogs into LRU cache and close old log files.
        }
        return rc;
    }

    LogFileNode getLogFileWithId(int logFileId) throws InvalidRecordLocationException {
        for (LogFileNode lf = firstActiveNode; lf != null; lf = lf.getNextActive()) {
            if (lf.getId() == logFileId) {
                return lf;
            }

            // Short cut since id's will only increment
            if (logFileId < lf.getId())
                break;
        }
        return null;
    }

    /**
     * @param lastLocation
     * @return
     */
    public Location getNextDataRecordLocation(Location lastLocation) throws IOException, InvalidRecordLocationException {
        RecordInfo ri = readRecordInfo(lastLocation);
        while (true) {

            int logFileId = ri.getLocation().getLogFileId();
            int offset = ri.getNextLocation();

            // Are we overflowing into next logFile?
            if (offset >= ri.getLogFileState().getAppendOffset()) {
                LogFileNode nextActive = ri.getLogFileState().getNextActive();
                if (nextActive == null || nextActive.getId() <= ri.getLogFileState().getId() ) {
                    return null;
                }
                logFileId = nextActive.getId();
                offset = 0;
            }

            try {
                ri = readRecordInfo(new Location(logFileId, offset));
            } catch (InvalidRecordLocationException e) {
                return null;
            }

            // Is the next record the right record type?
            if (ri.getHeader().getRecordType() == DATA_RECORD_TYPE) {
                return ri.getLocation();
            }
            // No? go onto the next record.
        }
    }

    /**
     * @param logFileIndex
     * @param logFileOffset
     * @return
     * @throws IOException
     * @throws InvalidRecordLocationException
     */
    public Packet readPacket(Location location) throws IOException, InvalidRecordLocationException {

        // Is there a record header at the seeked location?
        RecordInfo recordInfo = readRecordInfo(location);

        byte data[] = new byte[recordInfo.getHeader().getPayloadLength()];

        LogFile logFile = recordInfo.getLogFile();
        logFile.read(recordInfo.getDataOffset(), data);

        return new ByteArrayPacket(data);

    }

    public int getInitialLogFileSize() {
        return initialLogFileSize;
    }

    public Location getFirstActiveLogLocation() {
        if (firstActiveNode == null)
            return null;
        if (firstActiveNode.getAppendOffset() == 0)
            return null;
        return new Location(firstActiveNode.getId(), 0);
    }

    void activateNextLogFile() throws IOException {

        // The current append logFile becomes readonly
        if (appendNode != null) {
            appendNode.setReadOnly(true);
        }

        LogFileNode next = firstInactiveNode;
        synchronized (this) {
            firstInactiveNode = firstInactiveNode.getNextInactive();
            next.activate(getNextLogFileId());
            if (firstActiveNode == null) {
                firstActiveNode = next;
            }
        }        
        activeLogFileCount.incrementAndGet();        
        appendNode = next;
        
        storeState();
    }

    /**
     * @return Returns the logDirectory.
     */
    public File getLogDirectory() {
        return logDirectory;
    }

    /**
     * @return Returns the lastMark.
     */
    public Location getLastMarkedRecordLocation() {
        return lastMark;
    }

    public Location getNextAppendLocation() {
        return new Location(appendNode.getId(), appendNode.getAppendOffset());
    }

    /**
     * @return Returns the onlineLogFileCount.
     */
    public int getOnlineLogFileCount() {
        return onlineLogFileCount;
    }

    public boolean isPastHalfActive() {
        return (onlineLogFileCount/2.f) < activeLogFileCount.get();
    }

    synchronized  public Location getFirstRecordLocationOfSecondActiveLogFile() {
        return firstActiveNode.getNextActive().getFirstRecordLocation();
    }

    synchronized public boolean canActivateNextLogFile() {
        return firstInactiveNode!=null;
    }

}
