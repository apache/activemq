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

import org.apache.activeio.journal.InvalidRecordLocationException;
import org.apache.activeio.journal.Journal;
import org.apache.activeio.journal.JournalEventListener;
import org.apache.activeio.journal.RecordLocation;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activemq.util.ByteSequence;

/**
 * Provides a Journal Facade to the DataManager.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class JournalFacade implements Journal {

    private final AsyncDataManager dataManager;

    public static class RecordLocationFacade implements RecordLocation {
        private final Location location;

        public RecordLocationFacade(Location location) {
            this.location = location;
        }

        public Location getLocation() {
            return location;
        }

        public int compareTo(Object o) {
            RecordLocationFacade rlf = (RecordLocationFacade)o;
            int rc = location.compareTo(rlf.location);
            return rc;
        }
    }

    public JournalFacade(AsyncDataManager dataManager) {
        this.dataManager = dataManager;
    }

    private static RecordLocation convertToRecordLocation(Location location) {
        if (location == null) {
            return null;
        }
        return new RecordLocationFacade(location);
    }

    private static Location convertFromRecordLocation(RecordLocation location) {

        if (location == null) {
            return null;
        }

        return ((RecordLocationFacade)location).getLocation();
    }

    public void close() throws IOException {
        dataManager.close();
    }

    public RecordLocation getMark() throws IllegalStateException {
        return convertToRecordLocation(dataManager.getMark());
    }

    public RecordLocation getNextRecordLocation(RecordLocation location) throws InvalidRecordLocationException, IOException, IllegalStateException {
        return convertToRecordLocation(dataManager.getNextLocation(convertFromRecordLocation(location)));
    }

    public Packet read(RecordLocation location) throws InvalidRecordLocationException, IOException, IllegalStateException {
        ByteSequence rc = dataManager.read(convertFromRecordLocation(location));
        if (rc == null) {
            return null;
        }
        return new ByteArrayPacket(rc.getData(), rc.getOffset(), rc.getLength());
    }

    public void setJournalEventListener(JournalEventListener listener) throws IllegalStateException {
    }

    public void setMark(RecordLocation location, boolean sync) throws InvalidRecordLocationException, IOException, IllegalStateException {
        dataManager.setMark(convertFromRecordLocation(location), sync);
    }

    public RecordLocation write(Packet packet, boolean sync) throws IOException, IllegalStateException {
        org.apache.activeio.packet.ByteSequence data = packet.asByteSequence();
        ByteSequence sequence = new ByteSequence(data.getData(), data.getOffset(), data.getLength());
        return convertToRecordLocation(dataManager.write(sequence, sync));
    }
    
    public RecordLocation write(Packet packet, Runnable onComplete) throws IOException, IllegalStateException {
        org.apache.activeio.packet.ByteSequence data = packet.asByteSequence();
        ByteSequence sequence = new ByteSequence(data.getData(), data.getOffset(), data.getLength());
        return convertToRecordLocation(dataManager.write(sequence, onComplete));
    }

}
