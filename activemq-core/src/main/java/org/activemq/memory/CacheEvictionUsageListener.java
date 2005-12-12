package org.activemq.memory;

import java.util.Iterator;
import java.util.LinkedList;

import org.activemq.thread.Task;
import org.activemq.thread.TaskRunner;
import org.activemq.thread.TaskRunnerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

public class CacheEvictionUsageListener implements UsageListener {
    
    private final static Log log = LogFactory.getLog(CacheEvictionUsageListener.class);
    
    private final CopyOnWriteArrayList evictors = new CopyOnWriteArrayList();
    private final int usageHighMark;
    private final int usageLowMark;

    private final TaskRunner evictionTask;
    private final UsageManager usageManager;
    
    public CacheEvictionUsageListener(UsageManager usageManager, int usageHighMark, int usageLowMark, TaskRunnerFactory taskRunnerFactory) {
        this.usageManager = usageManager;
        this.usageHighMark = usageHighMark;
        this.usageLowMark = usageLowMark;
        evictionTask = taskRunnerFactory.createTaskRunner(new Task(){
            public boolean iterate() {
                return evictMessages();
            }
        });
    }
    
    private boolean evictMessages() {
        // Try to take the memory usage down below the low mark.
        try {            
            log.debug("Evicting cache memory usage: "+usageManager.getPercentUsage());
            
            LinkedList list = new LinkedList(evictors);
            while (list.size()>0 && usageManager.getPercentUsage() > usageLowMark) {
                
                // Evenly evict messages from all evictors
                for (Iterator iter = list.iterator(); iter.hasNext();) {
                    CacheEvictor evictor = (CacheEvictor) iter.next();
                    if( evictor.evictCacheEntry() == null )
                        iter.remove();
                }
            }
        } finally {
        }
        return false;
    }
    
    public void onMemoryUseChanged(UsageManager memoryManager, int oldPercentUsage, int newPercentUsage) {
        // Do we need to start evicting cache entries? Usage > than the
        // high mark
        if (oldPercentUsage < newPercentUsage && memoryManager.getPercentUsage()  >= usageHighMark) {
            try {
                evictionTask.wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    public void add(CacheEvictor evictor) {
        evictors.add(evictor);
    }
    
    public void remove(CacheEvictor evictor) {
        evictors.remove(evictor);
    }
}
