/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.memory;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;


/**
 * Used to keep track of how much of something is being used so that 
 * a productive working set usage can be controlled.
 * 
 * Main use case is manage memory usage.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision: 1.3 $
 */
public class UsageManager {

    private static final Log log = LogFactory.getLog(UsageManager.class);
    
    private final UsageManager parent;
    private long limit;
    private long usage;
    
    private int percentUsage;
    private int percentUsageMinDelta=10;
    
    private final Object usageMutex = new Object();
    
    private final CopyOnWriteArrayList listeners = new CopyOnWriteArrayList();
    
    public UsageManager() {
        this(null);
    }
    
    /**
     * Create the memory manager linked to a parent.  When the memory manager is linked to 
     * a parent then when usage increased or decreased, the parent's usage is also increased 
     * or decreased.
     * 
     * @param parent
     */
    public UsageManager(UsageManager parent) {
        this.parent = parent;
    }
    
    /**
     * Tries to increase the usage by value amount but blocks if this object
     * is currently full.
     * @throws InterruptedException 
     */
    public void enqueueUsage(long value) throws InterruptedException {
        waitForSpace(); 
        increaseUsage(value);
    }

    /**
     * @throws InterruptedException
     */
    public void waitForSpace() throws InterruptedException {
        synchronized (usageMutex) {
            for( int i=0; percentUsage >= 100 ; i++) {
                usageMutex.wait();
            }
        }
    }
    
    /**
     * Increases the usage by the value amount.  
     * 
     * @param value
     */
    public void increaseUsage(long value) {
        if( value == 0 )
            return;
        if(parent!=null)
            parent.increaseUsage(value);
        synchronized(usageMutex) {
            usage+=value;
            setPercentUsage(caclPercentUsage());
        }
    }
    
    /**
     * Decreases the usage by the value amount.  
     * 
     * @param value
     */
    public void decreaseUsage(long value) {
        if( value == 0 )
            return;
        if(parent!=null)
            parent.decreaseUsage(value);
        synchronized(usageMutex) {
            usage-=value;
            setPercentUsage(caclPercentUsage());
        }
    }
    
    public boolean isFull() {
        synchronized (usageMutex) {
            return percentUsage >= 100;
        }
    }
    
    public void addUsageListener(UsageListener listener) {
        listeners.add(listener);
    }
    public void removeUsageListener(UsageListener listener) {
        listeners.remove(listener);
    }
    
    public long getLimit() {
        synchronized (usageMutex) {
            return limit;
        }
    }

    public void setLimit(long limit) {
        if(percentUsageMinDelta < 0 ) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        synchronized (usageMutex) {
            this.limit = limit;
            setPercentUsage(caclPercentUsage());
        }
    }
    
    /*
    * Sets the minimum number of percentage points the usage has to change before a UsageListener
    * event is fired by the manager.
    */
    public int getPercentUsage() {
        synchronized (usageMutex) {
            return percentUsage;
        }
    }

    public int getPercentUsageMinDelta() {
        synchronized (usageMutex) {
            return percentUsageMinDelta;
        }
    }

    /**
     * Sets the minimum number of percentage points the usage has to change before a UsageListener
     * event is fired by the manager.
     * 
     * @param percentUsageMinDelta
     */
    public void setPercentUsageMinDelta(int percentUsageMinDelta) {
        if(percentUsageMinDelta < 1) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater than 0");
        }
        synchronized (usageMutex) {
            this.percentUsageMinDelta = percentUsageMinDelta;
            setPercentUsage(caclPercentUsage());
        }
    }

    public long getUsage() {
        synchronized (usageMutex) {
            return usage;
        }
    }
    
    
    private void setPercentUsage(int value) {
        int oldValue = percentUsage;
        percentUsage = value;
        if( oldValue!=value ) {
            fireEvent(oldValue, value);
        }
    }
    
    private int caclPercentUsage() {
        if( limit==0 ) return 0;
        return (int)((((usage*100)/limit)/percentUsageMinDelta)*percentUsageMinDelta);
    }
    
    private void fireEvent(int oldPercentUsage, int newPercentUsage) {
        
        log.debug("Memory usage change.  from: "+oldPercentUsage+", to: "+newPercentUsage);
        
        // Switching from being full to not being full..
        if( oldPercentUsage >= 100 && newPercentUsage < 100 ) {
            synchronized (usageMutex) {
                usageMutex.notifyAll();
            }            
        }
        
        // Let the listeners know
        for (Iterator iter = listeners.iterator(); iter.hasNext();) {
            UsageListener l = (UsageListener) iter.next();
            l.onMemoryUseChanged(this, oldPercentUsage, newPercentUsage);
        }
    }

    public String toString() {
        return "UsageManager: percentUsage="+percentUsage+"%, usage="+usage+" limit="+limit+" percentUsageMinDelta="+percentUsageMinDelta+"%";
    }
}
