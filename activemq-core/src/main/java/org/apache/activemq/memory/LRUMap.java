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
package org.apache.activemq.memory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple least-recently-used cache of a fixed size.
 * 
 * @version $Revision:$
 */
public class LRUMap extends LinkedHashMap {
    private static final long serialVersionUID = -9179676638408888162L;

    protected static final float DEFAULT_LOAD_FACTOR = (float) 0.75;
    protected static final int DEFAULT_INITIAL_CAPACITY = 5000;

    private int maximumSize;

    public LRUMap(int maximumSize) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true, maximumSize);
    }

    public LRUMap(int maximumSize, boolean accessOrder) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, accessOrder, maximumSize);
    }

    public LRUMap(int initialCapacity, float loadFactor, boolean accessOrder, int maximumSize) {
        super(initialCapacity, loadFactor, accessOrder);
        this.maximumSize = maximumSize;
    }

    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > maximumSize;
    }
}
