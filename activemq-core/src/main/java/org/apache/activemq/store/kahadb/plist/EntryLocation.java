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
package org.apache.activemq.store.kahadb.plist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.util.VariableMarshaller;

class EntryLocation {
    static final long NOT_SET = -1;
    private String id;
    private Page<EntryLocation> page;
    private long next;
    private long prev;
    private Location location;

    static class EntryLocationMarshaller extends VariableMarshaller<EntryLocation> {
        static final EntryLocationMarshaller INSTANCE = new EntryLocationMarshaller();
        public EntryLocation readPayload(DataInput dataIn) throws IOException {
            EntryLocation result = new EntryLocation();
            result.readExternal(dataIn);
            return result;
        }

        public void writePayload(EntryLocation value, DataOutput dataOut) throws IOException {
            value.writeExternal(dataOut);
        }
    }
    EntryLocation(Location location) {
        this.location = location;

    }

    EntryLocation() {
    }
    
    EntryLocation copy() {
        EntryLocation result = new EntryLocation();
        result.id=this.id;
        result.location=this.location;
        result.next=this.next;
        result.prev=this.prev;
        result.page=this.page;
        return result;
    }

    void reset() {
        this.id = "";
        this.next = NOT_SET;
        this.prev = NOT_SET;
    }

    public void readExternal(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.prev = in.readLong();
        this.next = in.readLong();
        if (this.location == null) {
            this.location = new Location();
        }
        this.location.readExternal(in);
    }

    public void writeExternal(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.prev);
        out.writeLong(this.next);
        if (this.location == null) {
            this.location = new Location();
        }
        this.location.writeExternal(out);
    }

    /**
     * @return the jobId
     */
    String getId() {
        return this.id;
    }

    /**
     * @param id
     *            the id to set
     */
    void setId(String id) {
        this.id = id;
    }

    Location getLocation() {
        return this.location;
    }

    /**
     * @param location
     *            the location to set
     */
    void setLocation(Location location) {
        this.location = location;
    }

    /**
     * @return the next
     */
    long getNext() {
        return this.next;
    }

    /**
     * @param next
     *            the next to set
     */
    void setNext(long next) {
        this.next = next;
    }

    /**
     * @return the prev
     */
    long getPrev() {
        return this.prev;
    }

    /**
     * @param prev
     *            the prev to set
     */
    void setPrev(long prev) {
        this.prev = prev;
    }

    /**
     * @return the page
     */
    Page<EntryLocation> getPage() {
        return this.page;
    }

    /**
     * @param page
     *            the page to set
     */
    void setPage(Page<EntryLocation> page) {
        this.page = page;
    }

}
