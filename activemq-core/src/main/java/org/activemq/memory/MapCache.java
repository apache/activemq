/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.memory;

import java.util.Map;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * Use any Map to implement the Cache.  No cache eviction going on here.  Just gives
 * a Map a Cache interface.
 * 
 * @version $Revision$
 */
public class MapCache implements Cache {
    
    protected final Map map;
    
    public MapCache() {
        this(new ConcurrentHashMap());
    }
    
    public MapCache(Map map) {
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
