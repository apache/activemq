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
package org.apache.activegroups;

/**
 *Get notifications about changes to the state of the map
 *
 */
public interface GroupStateChangedListener {
    
    /**
     * Called when a key/value pair is inserted into the map
     * @param owner 
     * @param key
     * @param value 
     */
    void mapInsert(Member owner,Object key, Object value);
    
    /**
     * Called when a key value is updated in the map
     * @param owner
     * @param Key
     * @param oldValue
     * @param newValue
     */
    void mapUpdate(Member owner,Object Key,Object oldValue,Object newValue);
    
    /**
     * Called when a key value is removed from the map
     * @param owner
     * @param key
     * @param value
     * @param expired
     */
    void mapRemove(Member owner,Object key, Object value,boolean expired);
}
