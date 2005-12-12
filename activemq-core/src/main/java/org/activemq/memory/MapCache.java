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
