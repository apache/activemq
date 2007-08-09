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
package org.apache.activemq.kaha;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Represents a container of persistent objects in the store Acts as a map, but
 * values can be retrieved in insertion order
 * 
 * @version $Revision: 1.2 $
 */
public interface MapContainer<K, V> extends Map<K, V> {

    /**
     * The container is created or retrieved in an unloaded state. load
     * populates the container will all the indexes used etc and should be
     * called before any operations on the container
     */
    void load();

    /**
     * unload indexes from the container
     * 
     */
    void unload();

    /**
     * @return true if the indexes are loaded
     */
    boolean isLoaded();

    /**
     * For homogenous containers can set a custom marshaller for loading keys
     * The default uses Object serialization
     * 
     * @param keyMarshaller
     */
    void setKeyMarshaller(Marshaller<K> keyMarshaller);

    /**
     * For homogenous containers can set a custom marshaller for loading values
     * The default uses Object serialization
     * 
     * @param valueMarshaller
     * 
     */
    void setValueMarshaller(Marshaller<V> valueMarshaller);

    /**
     * @return the id the MapContainer was create with
     */
    Object getId();

    /**
     * @return the number of values in the container
     */
    int size();

    /**
     * @return true if there are no values stored in the container
     */
    boolean isEmpty();

    /**
     * @param key
     * @return true if the container contains the key
     */
    boolean containsKey(K key);

    /**
     * Get the value associated with the key
     * 
     * @param key
     * @return the value associated with the key from the store
     */
    V get(K key);

    /**
     * @param o
     * @return true if the MapContainer contains the value o
     */
    boolean containsValue(K o);

    /**
     * Add add entries in the supplied Map
     * 
     * @param map
     */
    void putAll(Map<K, V> map);

    /**
     * @return a Set of all the keys
     */
    Set<K> keySet();

    /**
     * @return a collection of all the values - the values will be lazily pulled
     *         out of the store if iterated etc.
     */
    Collection<V> values();

    /**
     * @return a Set of all the Map.Entry instances - the values will be lazily
     *         pulled out of the store if iterated etc.
     */
    Set<Map.Entry<K, V>> entrySet();

    /**
     * Add an entry
     * 
     * @param key
     * @param value
     * @return the old value for the key
     */
    V put(K key, V value);

    /**
     * remove an entry associated with the key
     * 
     * @param key
     * @return the old value assocaited with the key or null
     */
    V remove(K key);

    /**
     * empty the container
     */
    void clear();

    /**
     * Add an entry to the Store Map
     * 
     * @param key
     * @param Value
     * @return the StoreEntry associated with the entry
     */
    StoreEntry place(K key, V value);

    /**
     * Remove an Entry from ther Map
     * 
     * @param entry
     */
    void remove(StoreEntry entry);

    /**
     * Get the Key object from it's location
     * 
     * @param keyLocation
     * @return the key for the entry
     */
    K getKey(StoreEntry keyLocation);

    /**
     * Get the value from it's location
     * 
     * @param Valuelocation
     * @return the Object
     */
    V getValue(StoreEntry valueLocation);

    /**
     * Get the StoreEntry for the first value in the Map
     * 
     * @return the first StoreEntry or null if the map is empty
     */
    StoreEntry getFirst();

    /**
     * Get the StoreEntry for the last value item of the Map
     * 
     * @return the last StoreEntry or null if the list is empty
     */
    StoreEntry getLast();

    /**
     * Get the next StoreEntry value from the map
     * 
     * @param entry
     * @return the next StoreEntry or null
     */
    StoreEntry getNext(StoreEntry entry);

    /**
     * Get the previous StoreEntry from the map
     * 
     * @param entry
     * @return the previous store entry or null
     */
    StoreEntry getPrevious(StoreEntry entry);

    /**
     * It's possible that a StoreEntry could be come stale this will return an
     * upto date entry for the StoreEntry position
     * 
     * @param entry old entry
     * @return a refreshed StoreEntry
     */
    StoreEntry refresh(StoreEntry entry);

    /**
     * Get the StoreEntry associated with the key
     * 
     * @param key
     * @return the StoreEntry
     */
    StoreEntry getEntry(K key);
}
