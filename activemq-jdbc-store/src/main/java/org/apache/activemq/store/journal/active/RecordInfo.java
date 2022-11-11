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

/**
 * @version $Revision: 1.1 $
 */
final public class RecordInfo {

    private final Location location;
    private final Record header;
    private final LogFileNode logFileState;
    private final LogFile logFile;

    public RecordInfo(Location location, Record header, LogFileNode logFileState, LogFile logFile) {
        this.location = location;
        this.header = header;
        this.logFileState = logFileState;
        this.logFile = logFile;
    }

    int getNextLocation() {
        return location.getLogFileOffset() + header.getPayloadLength() + Record.RECORD_BASE_SIZE;
    }

    public Record getHeader() {
        return header;
    }

    public Location getLocation() {
        return location;
    }

    public LogFileNode getLogFileState() {
        return logFileState;
    }

    public LogFile getLogFile() {
        return logFile;
    }

    public int getDataOffset() {
        return location.getLogFileOffset() + Record.RECORD_HEADER_SIZE;
    }
}
