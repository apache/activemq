package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class LRUCacheTest {
    protected static final Log LOG = LogFactory.getLog(LRUCacheTest.class);
    
    @Test
    public void testResize() throws Exception {
        LRUCache<Long, Long> underTest = new LRUCache<Long, Long>(1000);
        
        Long count = new Long(0);
        long max = 0;
        for (; count < 27276827; count++) {
            long start = System.currentTimeMillis();
            if (!underTest.containsKey(count)) {
                underTest.put(count, count);
            }
            long duration = System.currentTimeMillis() - start;
            if (duration > max) {
                LOG.info("count: " + count + ", new max=" + duration);
                max = duration;
            }
            if (count % 100000000 == 0) {
                LOG.info("count: " + count + ", max=" + max);
            }
        }
        assertEquals("size is still in order", 1000, underTest.size());
    }
}
