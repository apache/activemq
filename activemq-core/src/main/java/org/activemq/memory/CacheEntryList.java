package org.activemq.memory;

/**
 * Maintains a simple linked list of CacheEntry objects.  It is thread safe.
 * 
 * @version $Revision$
 */
public class CacheEntryList {
    
    // Points at the tail of the CacheEntry list
    public final CacheEntry tail = new CacheEntry(null, null);
    
    public CacheEntryList() {
        tail.next = tail.previous = tail;
    }
            
    public void add(CacheEntry ce) {
        addEntryBefore(tail, ce);
    }
    
    private void addEntryBefore(CacheEntry position, CacheEntry ce) {
        assert ce.key!=null && ce.next==null && ce.owner==null;
        
        synchronized( tail ) {
            ce.owner=this;
            ce.next = position;
            ce.previous = position.previous;            
            ce.previous.next = ce;
            ce.next.previous = ce;
        }
    }
        
    public void clear() {
        synchronized( tail ) {            
            tail.next = tail.previous = tail;
        }
    }

    public CacheEvictor createFIFOCacheEvictor() {
        return new CacheEvictor() {
            public CacheEntry evictCacheEntry() {
                CacheEntry rc;
                synchronized( tail ) {
                    rc = tail.next;
                }
                return rc.remove() ? rc : null;
            }
        };
    }
    
    public CacheEvictor createLIFOCacheEvictor() {
        return new CacheEvictor() {
            public CacheEntry evictCacheEntry() {
                CacheEntry rc;
                synchronized( tail ) {
                    rc = tail.previous;
                }
                return rc.remove() ? rc : null;
            }
        };
    }

}
