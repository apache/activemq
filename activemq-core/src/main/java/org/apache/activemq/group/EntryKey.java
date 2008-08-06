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
 * Holds information about an EntryKey
 * 
 */
class EntryKey<K> implements Externalizable {
    private Member owner;
    private K key;
    private boolean share;
    private boolean removeOnExit;
    private long expiration;

    /**
     * Default constructor - for serialization
     */
    public EntryKey() {
    }

    EntryKey(Member owner, K key) {
        this.owner = owner;
        this.key = key;
    }

    public int hashCode() {
        return this.key != null ? this.key.hashCode() : super.hashCode();
    }

    /**
     * @return the owner
     */
    public Member getOwner() {
        return this.owner;
    }

    /**
     * @return the key
     */
    public K getKey() {
        return this.key;
    }
    
    /**
     * @return the share
     */
    public boolean isShare() {
        return this.share;
    }

    /**
     * @param share the share to set
     */
    public void setShare(boolean share) {
        this.share = share;
    }

    /**
     * @return the removeOnExit
     */
    public boolean isRemoveOnExit() {
        return this.removeOnExit;
    }

    /**
     * @param removeOnExit
     *            the removeOnExit to set
     */
    public void setRemoveOnExit(boolean removeOnExit) {
        this.removeOnExit = removeOnExit;
    }
    
    /**
     * @return the expiration
     */
    public long getExpiration() {
        return expiration;
    }

    /**
     * @param expiration the expiration to set
     */
    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
    
    void setTimeToLive(long ttl) {
        if (ttl > 0 ) {
            this.expiration=ttl+System.currentTimeMillis();
        }else {
            this.expiration =0l;
        }
    }
    
    boolean isExpired() {
        return isExpired(System.currentTimeMillis());
    }
    
    boolean isExpired(long currentTime) {
        return this.expiration > 0 && this.expiration < currentTime;
    }
    
   

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof EntryKey) {
            EntryKey other = (EntryKey) obj;
            result = other.key.equals(this.key);
        }
        return result;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.owner);
        out.writeObject(this.key);
        out.writeBoolean(isShare());
        out.writeBoolean(isRemoveOnExit());
        out.writeLong(getExpiration());
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.owner = (Member) in.readObject();
        this.key = (K) in.readObject();
        this.share = in.readBoolean();
        this.removeOnExit=in.readBoolean();
        this.expiration=in.readLong();
    }
    
    public String toString() {
        return "key:"+this.key;
    }
}
