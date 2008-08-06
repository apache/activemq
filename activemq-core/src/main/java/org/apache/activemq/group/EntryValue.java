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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY VIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.group;


/**
 * Holds information about the Value in the Map
 *
 */
class EntryValue<V> {
    private Member owner;
    private V value;
    
    
    EntryValue(Member owner, V value){
        this.owner=owner;
        this.value=value;
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
    public V getValue() {
        return this.value;
    }
    
    /**
     * set the value
     * @param value
     */
    public void setValue(V value) {
        this.value=value;
    }
    
    public int hashCode() {
        return this.value != null ? this.value.hashCode() : super.hashCode();
    }
    
    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof EntryValue) {
            EntryValue other = (EntryValue)obj;
            result = (this.value==null && other.value==null) ||
                (this.value != null && other.value != null && this.value.equals(other.value));
        }
        return result;
    }
}

