package org.activemq.memory;


/**
 * Filters another Cache implementation.
 * 
 * @version $Revision$
 */
public class CacheFilter implements Cache {
    
    protected final Cache next;
    
    public CacheFilter(Cache next) {
        this.next = next;
    }

    public Object put(Object key, Object value) {
        return next.put(key, value);
    }

    public Object get(Object key) {
        return next.get(key);
    }

    public Object remove(Object key) {
        return next.remove(key);
    }
    
    public void close() {
        next.close();
    }

    public int size() {
        return next.size();
    }
}
