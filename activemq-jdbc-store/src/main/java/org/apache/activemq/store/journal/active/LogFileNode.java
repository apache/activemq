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
import java.io.DataOutput;
import java.io.IOException;

/**
 * @version $Revision: 1.1 $
 */
final class LogFileNode {
    
    static final public int SERIALIZED_SIZE = 10;

    private final LogFile logFile;    
    private LogFileNode next;

    /** The id of the log file. */
    private int id;
    /** Does it have live records in it? */
    private boolean active = false;
    /** Is the log file in readonly mode */
    private boolean readOnly;
    /** The location of the next append offset */
    private int appendOffset = 0;

    public LogFileNode(LogFile logFile) {
        this.logFile = logFile;
    }

    public LogFile getLogFile() {
        return logFile;
    }

    /////////////////////////////////////////////////////////////
    //
    // Method used to mange the state of the log file.
    //
    /////////////////////////////////////////////////////////////

    public void activate(int id) {
        if (active)
            throw new IllegalStateException("Log already active.");
        this.id = id;
        this.readOnly = false;
        this.active = true;
        this.appendOffset = 0;
    }

    public int getId() {
        return id;
    }

    public void setReadOnly(boolean enable) {
        if (!active)
            throw new IllegalStateException("Log not active.");
        this.readOnly = enable;
    }

    public void deactivate() throws IOException {
        if (!active)
            throw new IllegalStateException("Log already inactive.");      
        this.active=false; 
        this.id = -1;
        this.readOnly = true;
        this.appendOffset = 0;
        getLogFile().resize();
    }

    public boolean isActive() {
        return active;
    }

    public int getAppendOffset() {
        return appendOffset;
    }

    public Location getFirstRecordLocation() {
        if (isActive() && appendOffset > 0)
            return new Location(getId(), 0);
        return null;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void appended(int i) {
        appendOffset += i;
    }
    
    /////////////////////////////////////////////////////////////
    //
    // Method used to maintain the list of LogFileNodes used by 
    // the LogFileManager
    //
    /////////////////////////////////////////////////////////////
    
    public LogFileNode getNext() {
        return next;
    }

    public void setNext(LogFileNode state) {
        next = state;
    }

    public LogFileNode getNextActive() {
        if (getNext().isActive())
            return getNext();
        return null;
    }

    public LogFileNode getNextInactive() {
        if (!getNext().isActive())
            return getNext();
        return null;
    }
    
    /**
     * @param data
     * @throws IOException 
     */
    public void writeExternal(DataOutput data) throws IOException {
        data.writeInt(id);
        data.writeBoolean(active);
        data.writeBoolean(readOnly);
        data.writeInt(appendOffset);
    }

    /**
     * @param data
     * @throws IOException 
     */
    public void readExternal(DataInput data) throws IOException {
        id = data.readInt();
        active = data.readBoolean();
        readOnly = data.readBoolean();
        appendOffset = data.readInt();
    }

    public void setAppendOffset(int offset) {
        appendOffset = offset;
    }

}
