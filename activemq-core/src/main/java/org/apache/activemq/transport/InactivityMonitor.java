/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.thread.Scheduler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to make sure that commands are arriving periodically from the peer of the transport.  
 * 
 * @version $Revision$
 */
public class InactivityMonitor extends TransportFilter implements Runnable {

    private final Log log = LogFactory.getLog(InactivityMonitor.class);
    
    private final long maxInactivityDuration;
    private byte readCheckIteration=0;

    private final AtomicBoolean commandSent=new AtomicBoolean(false);
    private final AtomicBoolean inSend=new AtomicBoolean(false);

    private final AtomicBoolean commandReceived=new AtomicBoolean(true);
    private final AtomicBoolean inReceive=new AtomicBoolean(false);
    
    public InactivityMonitor(Transport next, long maxInactivityDuration) {
        super(next);
        this.maxInactivityDuration = maxInactivityDuration;
    }
        
    public void start() throws Exception {
        next.start();
        Scheduler.executePeriodically(this, maxInactivityDuration/2);
    }
    
    public void stop() throws Exception {
        Scheduler.cancel(this);
        next.stop();
    }
    
    synchronized public void run() {
        switch(readCheckIteration) {
        case 0:
            writeCheck();
            readCheckIteration++;
            break;
        case 1:
            readCheck();
            writeCheck();
            readCheckIteration=0;
            break;
        }        
    }
    
    private void writeCheck() {
        if( inSend.get() ) {
            log.debug("A send is in progress");
            return;
        }
        
        if( !commandSent.get() ) {
            log.debug("No message sent since last write check, sending a KeepAliveInfo");
            try {
                next.oneway(new KeepAliveInfo());
            } catch (IOException e) {
                onException(e);
            }
        } else {
            log.debug("Message sent since last write check, resetting flag");
        }
        
        commandSent.set(false);
        
    }

    private void readCheck() {
        if( inReceive.get() ) {
            log.debug("A receive is in progress");
            return;
        }
        
        if( !commandReceived.get() ) {
            log.debug("No message received since last read check! ");
            onException(new InactivityIOException("Channel was inactive for too long."));           
        } else {
            log.debug("Message received since last read check, resetting flag: ");
        }
        
        commandReceived.set(false);
    }

    public void onCommand(Command command) {
        inReceive.set(true);
        try {
            getTransportListener().onCommand(command);
        } finally {
            inReceive.set(false);
            commandReceived.set(true);
        }
    }
    
    public void oneway(Command command) throws IOException {
        // Disable inactivity monitoring while processing a command.
        inSend.set(true);
        commandSent.set(true);
        try {
            next.oneway(command);
        } finally {
            inSend.set(false);
        }
    }
    
    public void onException(IOException error) {
        Scheduler.cancel(this);
        getTransportListener().onException(error);
    }
}
