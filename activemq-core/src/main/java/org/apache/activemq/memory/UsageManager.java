/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.memory;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Used to keep track of how much of something is being used so that 
 * a productive working set usage can be controlled.
 * 
 * Main use case is manage memory usage.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.3 $
 */
public class UsageManager {

    private static final Log log = LogFactory.getLog(UsageManager.class);
    
    private final UsageManager parent;
    private long limit;
    private long usage;
    
    private int percentUsage;
    private int percentUsageMinDelta=1;
    
    private final Object usageMutex = new Object();
    
    private final CopyOnWriteArrayList listeners = new CopyOnWriteArrayList();
    
    private boolean sendFailIfNoSpace;

    /** True if someone called setSendFailIfNoSpace() on this particular usage manager */
    private boolean sendFailIfNoSpaceExplicitySet;

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
        if(parent!=null)
            parent.waitForSpace();
        synchronized (usageMutex) {
            for( int i=0; percentUsage >= 100 ; i++) {
                usageMutex.wait();
            }
            for( int i=0; percentUsage > 90 ; i++) {
                usageMutex.wait(100);
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
        int percentUsage;
        synchronized(usageMutex) {
            usage+=value;
            percentUsage = caclPercentUsage();  
        }
        setPercentUsage(percentUsage);
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
        int percentUsage;
        synchronized(usageMutex) {
            usage-=value;
            percentUsage = caclPercentUsage();  
        }
        setPercentUsage(percentUsage);
    }
    
    public boolean isFull() {
        if(parent!=null && parent.isFull())
            return true;
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

    /**
     * Sets the memory limit in bytes.
     * 
     * When set using XBean, you can use values such as: "20 mb", "1024 kb", or "1 gb"
     * 
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setLimit(long limit) {
        if(percentUsageMinDelta < 0 ) {
            throw new IllegalArgumentException("percentUsageMinDelta must be greater or equal to 0");
        }
        int percentUsage;
        synchronized (usageMutex) {
            this.limit = limit;
            percentUsage = caclPercentUsage();
        }
        setPercentUsage(percentUsage);
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
        int percentUsage;
        synchronized (usageMutex) {
            this.percentUsageMinDelta = percentUsageMinDelta;
            percentUsage = caclPercentUsage();  
        } 
        setPercentUsage(percentUsage);
    }

    public long getUsage() {
        synchronized (usageMutex) {
            return usage;
        }
    }
    
    /**
     * Sets whether or not a send() should fail if there is no space free. The default
     * value is false which means to block the send() method until space becomes available
     */
    public void setSendFailIfNoSpace(boolean failProducerIfNoSpace) {
        sendFailIfNoSpaceExplicitySet = true;
        this.sendFailIfNoSpace = failProducerIfNoSpace;
    }

    public boolean isSendFailIfNoSpace() {
        if (sendFailIfNoSpaceExplicitySet || parent == null) {
            return sendFailIfNoSpace;
        } else {
            return parent.isSendFailIfNoSpace();
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
                System.err.println("Memory usage change.  from: "+oldPercentUsage+", to: "+newPercentUsage);
                usageMutex.notifyAll();
            }            
        }
//      Let the listeners know
        for (Iterator iter = listeners.iterator(); iter.hasNext();) {
            UsageListener l = (UsageListener) iter.next();
            l.onMemoryUseChanged(this, oldPercentUsage, newPercentUsage);
        }
       
    }

    public String toString() {
        return "UsageManager: percentUsage="+percentUsage+"%, usage="+usage+" limit="+limit+" percentUsageMinDelta="+percentUsageMinDelta+"%";
    }
}
