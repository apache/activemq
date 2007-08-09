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

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Represents a container of persistent objects in the store Acts as a map, but
 * values can be retrieved in insertion order
 * 
 * @version $Revision: 1.2 $
 */
public interface ListContainer<V> extends List<V> {

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
     * For homogenous containers can set a custom marshaller for loading values
     * The default uses Object serialization
     * 
     * @param marshaller
     */
    void setMarshaller(Marshaller marshaller);

    /**
     * @return the id the MapContainer was create with
     */
    Object getId();

    /**
     * @return the number of values in the container
     */
    int size();

    /**
     * Inserts the given element at the beginning of this list.
     * 
     * @param o the element to be inserted at the beginning of this list.
     */
    void addFirst(V o);

    /**
     * Appends the given element to the end of this list. (Identical in function
     * to the <tt>add</tt> method; included only for consistency.)
     * 
     * @param o the element to be inserted at the end of this list.
     */
    void addLast(V o);

    /**
     * Removes and returns the first element from this list.
     * 
     * @return the first element from this list.
     * @throws NoSuchElementException if this list is empty.
     */
    V removeFirst();

    /**
     * Removes and returns the last element from this list.
     * 
     * @return the last element from this list.
     * @throws NoSuchElementException if this list is empty.
     */
    V removeLast();

    /**
     * remove an objecr from the list without retrieving the old value from the
     * store
     * 
     * @param position
     * @return true if successful
     */
    boolean doRemove(int position);

    /**
     * add an Object to the list but get a StoreEntry of its position
     * 
     * @param object
     * @return the entry in the Store
     */
    StoreEntry placeLast(V object);

    /**
     * insert an Object in first position int the list but get a StoreEntry of
     * its position
     * 
     * @param object
     * @return the location in the Store
     */
    StoreEntry placeFirst(V object);

    /**
     * Advanced feature = must ensure the object written doesn't overwrite other
     * objects in the container
     * 
     * @param entry
     * @param object
     */
    void update(StoreEntry entry, V object);

    /**
     * Retrieve an Object from the Store by its location
     * 
     * @param entry
     * @return the Object at that entry
     */
    V get(StoreEntry entry);

    /**
     * Get the StoreEntry for the first item of the list
     * 
     * @return the first StoreEntry or null if the list is empty
     */
    StoreEntry getFirst();

    /**
     * Get the StoreEntry for the last item of the list
     * 
     * @return the last StoreEntry or null if the list is empty
     */
    StoreEntry getLast();

    /**
     * Get the next StoreEntry from the list
     * 
     * @param entry
     * @return the next StoreEntry or null
     */
    StoreEntry getNext(StoreEntry entry);

    /**
     * Get the previous StoreEntry from the list
     * 
     * @param entry
     * @return the previous store entry or null
     */
    StoreEntry getPrevious(StoreEntry entry);

    /**
     * remove the Object at the StoreEntry
     * 
     * @param entry
     * @return true if successful
     */
    boolean remove(StoreEntry entry);

    /**
     * It's possible that a StoreEntry could be come stale this will return an
     * upto date entry for the StoreEntry position
     * 
     * @param entry old entry
     * @return a refreshed StoreEntry
     */
    StoreEntry refresh(StoreEntry entry);
}
