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

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.RedoListener;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;

/**
 * Provides a Kaha DataManager Facade to the DataManager.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class DataManagerFacade implements org.apache.activemq.kaha.impl.DataManager {

    private static final ByteSequence FORCE_COMMAND = new ByteSequence(new byte[] {'F', 'O', 'R', 'C', 'E'});

    private AsyncDataManager dataManager;
    private final String name;
    private Marshaller redoMarshaller;

    private static class StoreLocationFacade implements StoreLocation {
        private final Location location;

        public StoreLocationFacade(Location location) {
            this.location = location;
        }

        public int getFile() {
            return location.getDataFileId();
        }

        public long getOffset() {
            return location.getOffset();
        }

        public int getSize() {
            return location.getSize();
        }

        public Location getLocation() {
            return location;
        }
    }

    public DataManagerFacade(AsyncDataManager dataManager, String name) {
        this.dataManager = dataManager;
        this.name = name;
    }

    private static StoreLocation convertToStoreLocation(Location location) {
        if (location == null) {
            return null;
        }
        return new StoreLocationFacade(location);
    }

    private static Location convertFromStoreLocation(StoreLocation location) {

        if (location == null) {
            return null;
        }

        if (location.getClass() == StoreLocationFacade.class) {
            return ((StoreLocationFacade)location).getLocation();
        }

        Location l = new Location();
        l.setOffset((int)location.getOffset());
        l.setSize(location.getSize());
        l.setDataFileId(location.getFile());
        return l;
    }


    public Object readItem(Marshaller marshaller, StoreLocation location) throws IOException {
        ByteSequence sequence = dataManager.read(convertFromStoreLocation(location));
        DataByteArrayInputStream dataIn = new DataByteArrayInputStream(sequence);
        return marshaller.readPayload(dataIn);
    }

    public StoreLocation storeDataItem(Marshaller marshaller, Object payload) throws IOException {
        final DataByteArrayOutputStream buffer = new DataByteArrayOutputStream();
        marshaller.writePayload(payload, buffer);
        ByteSequence data = buffer.toByteSequence();
        return convertToStoreLocation(dataManager.write(data, (byte)1, false));
    }

    public void force() throws IOException {
        dataManager.write(FORCE_COMMAND, (byte)2, true);
    }

    public void updateItem(StoreLocation location, Marshaller marshaller, Object payload) throws IOException {
        final DataByteArrayOutputStream buffer = new DataByteArrayOutputStream();
        marshaller.writePayload(payload, buffer);
        ByteSequence data = buffer.toByteSequence();
        dataManager.update(convertFromStoreLocation(location), data, false);
    }

    public void close() throws IOException {
        dataManager.close();
    }

    public void consolidateDataFiles() throws IOException {
        dataManager.consolidateDataFiles();
    }

    public boolean delete() throws IOException {
        return dataManager.delete();
    }

    public void addInterestInFile(int file) throws IOException {
        dataManager.addInterestInFile(file);
    }

    public void removeInterestInFile(int file) throws IOException {
        dataManager.removeInterestInFile(file);
    }

    public void recoverRedoItems(RedoListener listener) throws IOException {
        throw new RuntimeException("Not Implemented..");
    }

    public StoreLocation storeRedoItem(Object payload) throws IOException {
        throw new RuntimeException("Not Implemented..");
    }

    public Marshaller getRedoMarshaller() {
        return redoMarshaller;
    }

    public void setRedoMarshaller(Marshaller redoMarshaller) {
        this.redoMarshaller = redoMarshaller;
    }

    public String getName() {
        return name;
    }

}
