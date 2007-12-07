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
package org.apache.activemq.store.kahadaptor;

import java.util.Iterator;

import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.StoreEntry;

/**
 * Holds information for the subscriber
 * 
 * @version $Revision: 1.10 $
 */
public class TopicSubContainer {
    private transient MapContainer mapContainer;
    private transient StoreEntry batchEntry;

    public TopicSubContainer(MapContainer container) {
        this.mapContainer = container;
    }

    /**
     * @return the batchEntry
     */
    public StoreEntry getBatchEntry() {
        return this.batchEntry;
    }

    /**
     * @param id
     * @param batchEntry the batchEntry to set
     */
    public void setBatchEntry(String id, StoreEntry batchEntry) {
        this.batchEntry = batchEntry;
    }

    public void reset() {
        batchEntry = null;
    }

    public boolean isEmpty() {
        return mapContainer.isEmpty();
    }

    public StoreEntry add(ConsumerMessageRef ref) {
        return mapContainer.place(ref.getMessageId(),ref);
    }

    public ConsumerMessageRef remove(MessageId id) {
        ConsumerMessageRef result = null;
        StoreEntry entry = mapContainer.getEntry(id);
        if (entry != null) {
            result = (ConsumerMessageRef) mapContainer.getValue(entry);
            mapContainer.remove(entry);
            if (batchEntry != null && batchEntry.equals(entry)) {
                reset();
            }
        }
        if(mapContainer.isEmpty()) {
            reset();
        }
        return result;
    }
    
    
    public ConsumerMessageRef get(StoreEntry entry) {
        return (ConsumerMessageRef)mapContainer.getValue(entry);
    }

    public StoreEntry getEntry() {
        return mapContainer.getFirst();
    }

    public StoreEntry refreshEntry(StoreEntry entry) {
        return mapContainer.refresh(entry);
    }

    public StoreEntry getNextEntry(StoreEntry entry) {
        return mapContainer.getNext(entry);
    }

    public Iterator iterator() {
        return mapContainer.values().iterator();
    }

    public int size() {
        return mapContainer.size();
    }

    public void clear() {
        reset();
        mapContainer.clear();
    }
}
