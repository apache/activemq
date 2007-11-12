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
package org.apache.activemq.util;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A Simple LRU Set
 * 
 * @version $Revision$
 * @param <K>
 * @param <V>
 */

public class LRUSet<E>
extends AbstractSet<E>
implements Set<E>, Cloneable, java.io.Serializable{
   
    private static final Object IGNORE = new Object();
   
    private final LRUCache cache;

    /**
     * Default constructor for an LRU Cache The default capacity is 10000
     */
    public LRUSet() {
        this(0,10000, 0.75f, true);
    }

    /**
     * Constructs a LRUCache with a maximum capacity
     * 
     * @param maximumCacheSize
     */
    public LRUSet(int maximumCacheSize) {
        this(0, maximumCacheSize, 0.75f, true);
    }

    /**
     * Constructs an empty <tt>LRUCache</tt> instance with the specified
     * initial capacity, maximumCacheSize,load factor and ordering mode.
     * 
     * @param initialCapacity
     *            the initial capacity.
     * @param maximumCacheSize
     * @param loadFactor
     *            the load factor.
     * @param accessOrder
     *            the ordering mode - <tt>true</tt> for access-order,
     *            <tt>false</tt> for insertion-order.
     * @throws IllegalArgumentException
     *             if the initial capacity is negative or the load factor is
     *             non-positive.
     */

    public LRUSet(int initialCapacity, int maximumCacheSize, float loadFactor, boolean accessOrder) {
        this.cache = new LRUCache<E,Object>(initialCapacity,maximumCacheSize,loadFactor,accessOrder);
    }

   
    public Iterator<E> iterator() {
    return cache.keySet().iterator();
    }

   
    public int size() {
    return cache.size();
    }

   
    public boolean isEmpty() {
    return cache.isEmpty();
    }

    public boolean contains(Object o) {
    return cache.containsKey(o);
    }

   
    public boolean add(E o) {
    return cache.put(o, IGNORE)==null;
    }

    public boolean remove(Object o) {
    return cache.remove(o)==IGNORE;
    }

    
    public void clear() {
    cache.clear();
    }

    

    
      
}
