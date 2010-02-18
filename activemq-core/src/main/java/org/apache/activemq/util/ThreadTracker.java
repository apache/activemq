package org.apache.activemq.util;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Debugging tool to track entry points through code, useful to see runtime call paths
 * To use, add to a method as follows:<code>
 *  public void someMethod() {
 *      ThreadTracker.track("someMethod");
 *      ...
 *  }</code>
 *  and at some stage call <code>result</code> to get a LOG
 *  output of the callers with an associated call count
 *      
 */
public class ThreadTracker {

    static final Log LOG = LogFactory.getLog(ThreadTracker.class);  
    static HashMap<String, Tracker> trackers = new HashMap<String, Tracker>();
    
    /**
     * track the stack trace of callers
     * @param name the method being tracked
     */
    public static void track(final String name) {
        Tracker t;
        final String key = name.intern();
        synchronized(trackers) {
            t = trackers.get(key);
            if (t == null) {
                t = new Tracker();
                trackers.put(key, t);
            }
        }
        t.track();
    }
    
    /**
     * output the result of stack trace capture to the log
     */
    public static void result() {
        for (Entry<String, Tracker> t: trackers.entrySet()) {
            LOG.info("Tracker: " + t.getKey() + ", " + t.getValue().size() + " entry points...");
            for (Trace trace : t.getValue().values()) {
                LOG.info("count: " + trace.count, trace);
            }
            LOG.info("Tracker: " + t.getKey() + ", done.");
        }
    }

}

@SuppressWarnings("serial")
class Trace extends Throwable {
    public int count = 1;
    public final long id;
    Trace() {
        super();
        id = calculateIdentifier();
    }
    private long calculateIdentifier() {
        int len = 0;
        for (int i=0; i<this.getStackTrace().length; i++) {
            len += this.getStackTrace()[i].toString().intern().hashCode();
        }
        return len;
    }
}

@SuppressWarnings("serial")
class Tracker extends HashMap<Long, Trace> {
    public void track() {
        Trace current = new Trace();
        synchronized(this) {
            Trace exist = get(current.id);
            if (exist != null) {
                exist.count++;
            } else {
                put(current.id, current);
            }
        }
    }
}
