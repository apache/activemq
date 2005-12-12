package org.activemq.memory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

/**
 * Simple CacheFilter that increases/decreases usage on a UsageManager as
 * objects are added/removed from the Cache.
 * 
 * @version $Revision$
 */
public class UsageManagerCacheFilter extends CacheFilter {

    private final AtomicLong totalUsage = new AtomicLong(0);
    private final UsageManager um;

    public UsageManagerCacheFilter(Cache next, UsageManager um) {
        super(next);
        this.um = um;
    }

    public Object put(Object key, Object value) {
        long usage = getUsageOfAddedObject(value);
        Object rc = super.put(key, value);
        if( rc !=null ) {
            usage -= getUsageOfRemovedObject(rc);
        }
        totalUsage.addAndGet(usage);
        um.increaseUsage(usage);
        return rc;
    }
    
    public Object remove(Object key) {
        Object rc = super.remove(key);
        if( rc !=null ) {
            long usage = getUsageOfRemovedObject(rc);
            totalUsage.addAndGet(-usage);
            um.decreaseUsage(usage);
        }
        return rc;
    }
    
    
    protected long getUsageOfAddedObject(Object value) {
        return 1;
    }
    
    protected long getUsageOfRemovedObject(Object value) {
        return 1;
    }

    public void close() {
        um.decreaseUsage(totalUsage.get());
    }
}
