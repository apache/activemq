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
package org.apache.activemq.group;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Used to pass information around
 *
 */
public class EntryMessage implements Externalizable{
    static enum MessageType{INSERT,DELETE,SYNC};
    private EntryKey key;
    private Object value;
    private MessageType type;
    private boolean mapUpdate;
    private boolean expired;
    
    /**
     * @return the owner
     */
    public EntryKey getKey() {
        return this.key;
    }
    /**
     * @param key
     */
    public void setKey(EntryKey key) {
        this.key = key;
    }
    /**
     * @return the value
     */
    public Object getValue() {
        return this.value;
    }
    /**
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }
    
    /**
     * @return the type
     */
    public MessageType getType() {
        return this.type;
    }
    /**
     * @param type the type to set
     */
    public void setType(MessageType type) {
        this.type = type;
    }
    
    /**
     * @return the mapUpdate
     */
    public boolean isMapUpdate() {
        return this.mapUpdate;
    }
    /**
     * @param mapUpdate the mapUpdate to set
     */
    public void setMapUpdate(boolean mapUpdate) {
        this.mapUpdate = mapUpdate;
    }
    
    /**
     * @return the expired
     */
    public boolean isExpired() {
        return expired;
    }
    /**
     * @param expired the expired to set
     */
    public void setExpired(boolean expired) {
        this.expired = expired;
    }
    
    /**
     * @return if insert message
     */
    public boolean isInsert() {
        return this.type != null && this.type.equals(MessageType.INSERT);
    }
    
    /**
     * @return true if delete message
     */
    public boolean isDelete() {
        return this.type != null && this.type.equals(MessageType.DELETE);
    }
    
    public boolean isSync() {
        return this.type != null && this.type.equals(MessageType.SYNC);
    }
    
    public EntryMessage copy() {
        EntryMessage result = new EntryMessage();
        result.key=this.key;
        result.value=this.value;
        result.type=this.type;
        result.mapUpdate=this.mapUpdate;
        result.expired=this.expired;
        return result;
    }
    
    
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.key=(EntryKey) in.readObject();
        this.value=in.readObject();
        this.type=(MessageType) in.readObject();  
        this.mapUpdate=in.readBoolean();
        this.expired=in.readBoolean();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.key);
        out.writeObject(this.value);
        out.writeObject(this.type);
        out.writeBoolean(this.mapUpdate);
        out.writeBoolean(this.expired);
    }
    
    public String toString() {
        return "EntryMessage: "+this.type + "[" + this.key + "," + this.value +
            "]{update=" + this.mapUpdate + "}";
    }
}
