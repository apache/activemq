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
    public static void track(String name) {
        Tracker t;
        synchronized(trackers) {
            t = trackers.get(name);
            if (t == null) {
                t = new Tracker();
                trackers.put(name, t);
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
    public final int size;
    Trace() {
        super();
        size = this.getStackTrace().length;
    }
}

@SuppressWarnings("serial")
class Tracker extends HashMap<Integer, Trace> {
    public void track() {
        Trace current = new Trace();
        synchronized(this) {
            Trace exist = get(current.size);
            if (exist != null) {
                exist.count++;
            } else {
                put(current.size, current);
            }
        }
    }
}
