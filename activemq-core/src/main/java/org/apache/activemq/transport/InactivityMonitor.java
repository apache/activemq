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
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.thread.Scheduler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to make sure that commands are arriving periodically from the peer of the transport.  
 * 
 * @version $Revision$
 */
public class InactivityMonitor extends TransportFilter {

    private final Log log = LogFactory.getLog(InactivityMonitor.class);
    
    private WireFormatInfo localWireFormatInfo;
    private WireFormatInfo remoteWireFormatInfo;
    private boolean monitorStarted=false;

    private final AtomicBoolean commandSent=new AtomicBoolean(false);
    private final AtomicBoolean inSend=new AtomicBoolean(false);

    private final AtomicBoolean commandReceived=new AtomicBoolean(true);
    private final AtomicBoolean inReceive=new AtomicBoolean(false);
    
    private final Runnable readChecker = new Runnable() {
        public void run() {
            readCheck();
        }
    };
    
    private final Runnable writeChecker = new Runnable() {
        public void run() {
            writeCheck();
        }
    };
    
    
    public InactivityMonitor(Transport next) {
        super(next);
    }

    public void stop() throws Exception {
        stopMonitorThreads();
        next.stop();
    }

        
    private void writeCheck() {
        if( inSend.get() ) {
            log.trace("A send is in progress");
            return;
        }
        
        if( !commandSent.get() ) {
            log.trace("No message sent since last write check, sending a KeepAliveInfo");
            try {
                next.oneway(new KeepAliveInfo());                
            } catch (IOException e) {
                onException(e);
            }
        } else {
            log.trace("Message sent since last write check, resetting flag");
        }
        
        commandSent.set(false);
        
    }

    private void readCheck() {
        if( inReceive.get() ) {
            log.trace("A receive is in progress");
            return;
        }
        
        if( !commandReceived.get() ) {
            log.debug("No message received since last read check for " + toString() + "! Throwing InactivityIOException.");
            onException(new InactivityIOException("Channel was inactive for too long."));           
        } else {
            log.trace("Message received since last read check, resetting flag: ");
        }
        
        commandReceived.set(false);
    }

    public void onCommand(Command command) {
        inReceive.set(true);
        try {
            if( command.isWireFormatInfo() ) {
                synchronized( this ) {
                    remoteWireFormatInfo = (WireFormatInfo) command;
                    try {
                        startMonitorThreads();
                    } catch (IOException e) {
                        onException(e);
                    }
                }
            }
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
            if( command.isWireFormatInfo() ) {
                synchronized( this ) {
                    localWireFormatInfo = (WireFormatInfo) command;
                    startMonitorThreads();
                }
            }
            next.oneway(command);
        } finally {
            inSend.set(false);
        }
    }
    
    public void onException(IOException error) {
        stopMonitorThreads();
        getTransportListener().onException(error);
    }
    
    
    synchronized private void startMonitorThreads() throws IOException {
        if( monitorStarted ) 
            return;
        if( localWireFormatInfo == null )
            return;
        if( remoteWireFormatInfo == null )
            return;
        
        long l = Math.min(localWireFormatInfo.getMaxInactivityDuration(), remoteWireFormatInfo.getMaxInactivityDuration());
        if( l > 0 ) {
            Scheduler.executePeriodically(writeChecker, l/2);
            Scheduler.executePeriodically(readChecker, l);
            monitorStarted=true;        
        }
    }
    
    /**
     * 
     */
    synchronized private void stopMonitorThreads() {
        if( monitorStarted ) {
            Scheduler.cancel(readChecker);
            Scheduler.cancel(writeChecker);
            monitorStarted=false;
        }
    }
    

}
