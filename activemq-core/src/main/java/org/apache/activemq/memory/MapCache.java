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

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Use any Map to implement the Cache.  No cache eviction going on here.  Just gives
 * a Map a Cache interface.
 * 
 * 
 */
public class MapCache implements Cache {
    
    protected final Map<Object, Object> map;
    
    public MapCache() {
        this(new ConcurrentHashMap<Object, Object>());
    }
    
    public MapCache(Map<Object, Object> map) {
        this.map = map;
    }

    public Object put(Object key, Object value) {
        return map.put(key, value);
    }

    public Object get(Object key) {
        return map.get(key);
    }

    public Object remove(Object key) {
        return map.remove(key);
    }
    
    public void close() {
        map.clear();
    }

    public int size() {
        return map.size();
    }
}
