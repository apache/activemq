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
package org.apache.activeio.journal.active;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.PacketData;

/**
 * Defines a where a record can be located in the Journal.
 * 
 * @version $Revision: 1.1 $
 */
final public class Location implements RecordLocation {
    
    static final public int SERIALIZED_SIZE=8;

    final private int logFileId;
    final private int logFileOffset;

    public Location(int logFileId, int fileOffset) {
        this.logFileId = logFileId;
        this.logFileOffset = fileOffset;
    }

    public int compareTo(Object o) {
        int rc = logFileId - ((Location) o).logFileId;
        if (rc != 0)
            return rc;

        return logFileOffset - ((Location) o).logFileOffset;
    }

    public int hashCode() {
        return logFileOffset ^ logFileId;
    }

    public boolean equals(Object o) {
        if (o == null || o.getClass() != Location.class)
            return false;
        Location rl = (Location) o;
        return rl.logFileId == this.logFileId && rl.logFileOffset == this.logFileOffset;
    }

    public String toString() {
        return "" + logFileId + ":" + logFileOffset;
    }

    public int getLogFileId() {
        return logFileId;
    }

    public int getLogFileOffset() {
        return logFileOffset;
    }
    
    public void writeToPacket(Packet packet) throws IOException {
        PacketData data = new PacketData(packet);
        data.writeInt(logFileId);
        data.writeInt(logFileOffset);
    }

    public void writeToDataOutput(DataOutput data) throws IOException {
        data.writeInt(logFileId);
        data.writeInt(logFileOffset);
    }    

    static public Location readFromPacket(Packet packet) throws IOException {
        PacketData data = new PacketData(packet);
        return new Location(data.readInt(), data.readInt());
    }

    public static Location readFromDataInput(DataInput data) throws IOException {
        return new Location(data.readInt(), data.readInt());
    }

    
}
