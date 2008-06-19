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
    static enum MessageType{INSERT,DELETE};
    private EntryKey key;
    private Object value;
    private MessageType type;
    
    /**
     * @return the owner
     */
    public EntryKey getKey() {
        return key;
    }
    /**
     * @param owner the owner to set
     */
    public void setKey(EntryKey key) {
        this.key = key;
    }
    /**
     * @return the value
     */
    public Object getValue() {
        return value;
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
        return type;
    }
    /**
     * @param type the type to set
     */
    public void setType(MessageType type) {
        this.type = type;
    }
    
    
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.key=(EntryKey) in.readObject();
        this.value=in.readObject();
        this.type=(MessageType) in.readObject();        
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.key);
        out.writeObject(this.value);
        out.writeObject(this.type);
    }
}
