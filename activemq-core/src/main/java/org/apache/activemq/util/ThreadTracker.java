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
package org.apache.activemq.util;

import java.util.HashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static final Logger LOG = LoggerFactory.getLogger(ThreadTracker.class);  
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
        synchronized(trackers) {
            for (Entry<String, Tracker> t: trackers.entrySet()) {
                LOG.info("Tracker: " + t.getKey() + ", " + t.getValue().size() + " entry points...");
                for (Trace trace : t.getValue().values()) {
                    LOG.info("count: " + trace.count, trace);
                }
                LOG.info("Tracker: " + t.getKey() + ", done.");
            }
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
