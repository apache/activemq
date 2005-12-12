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
package org.activemq.transport;

import java.io.IOException;

import org.activemq.command.KeepAliveInfo;
import org.activemq.management.CountStatisticImpl;
import org.activemq.thread.Scheduler;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to make sure that commands are arriving periodically from the peer of the transport.  
 * 
 * @version $Revision$
 */
public class InactivityMonitor extends TransportFilter implements Runnable {

    private final long maxInactivityDuration;
    private final AtomicBoolean cancled = new AtomicBoolean(false);
    private byte runIteration=0;

    private long lastReadCount;
    private long lastWriteCount;
    private final CountStatisticImpl readCounter;
    private final CountStatisticImpl writeCounter;
    
    public InactivityMonitor(Transport next, long maxInactivityDuration, CountStatisticImpl readCounter, CountStatisticImpl writeCounter ) {
        super(next);
        this.maxInactivityDuration = maxInactivityDuration;
        this.readCounter = readCounter;
        this.writeCounter = writeCounter;
    }
    
    public void start() throws Exception {
        next.start();
        Scheduler.executePeriodically(this, maxInactivityDuration/5);
    }
    
    public void stop() throws Exception {
        if( cancled.compareAndSet(false, true) ) {
            Scheduler.cancel(this);
        }
        next.stop();
    }
    
    public void run() {
        
        switch(runIteration) {
        case 1:
        case 2:
            long wc = writeCounter.getCount();
            if( wc==lastWriteCount ) {
                try {
                    oneway(new KeepAliveInfo());
                } catch (IOException e) {
                    onException(e);
                }
            } else {
                lastWriteCount = wc;
            }
            break;
        case 4:
            long rc = readCounter.getCount();
            if( rc == lastReadCount ) {
                onException(new InactivityIOException("Channel was inactive for too long."));
            } else {
                lastReadCount = rc;
            }
        }
        
        runIteration++;
        if(runIteration>=5)
            runIteration=0;
    }
    
    public void onException(IOException error) {
        if( cancled.compareAndSet(false, true) ) {
            Scheduler.cancel(this);
        }
        commandListener.onException(error);
    }
}
