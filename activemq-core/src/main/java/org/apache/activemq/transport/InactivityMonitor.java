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
package org.apache.activemq.transport;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Used to make sure that commands are arriving periodically from the peer of
 * the transport.
 * 
 * @version $Revision$
 */
public class InactivityMonitor extends TransportFilter {

    private static final Log LOG = LogFactory.getLog(InactivityMonitor.class);
    private static final ThreadPoolExecutor ASYNC_TASKS;
    
    private static int CHECKER_COUNTER;
    private static Timer  READ_CHECK_TIMER;
    private static Timer  WRITE_CHECK_TIMER;
    
    private WireFormatInfo localWireFormatInfo;
    private WireFormatInfo remoteWireFormatInfo;
    private final AtomicBoolean monitorStarted = new AtomicBoolean(false);

    private final AtomicBoolean commandSent = new AtomicBoolean(false);
    private final AtomicBoolean inSend = new AtomicBoolean(false);

    private final AtomicBoolean commandReceived = new AtomicBoolean(true);
    private final AtomicBoolean inReceive = new AtomicBoolean(false);
    private SchedulerTimerTask writeCheckerTask;
    private SchedulerTimerTask readCheckerTask;

    private long readCheckTime;
    private long writeCheckTime;
    
    private final Runnable readChecker = new Runnable() {
        long lastRunTime;
        public void run() {
            long now = System.currentTimeMillis();
            long elapsed = (now-lastRunTime);

            if( lastRunTime != 0 && LOG.isDebugEnabled() ) {
                LOG.debug(""+elapsed+" ms elapsed since last read check.");
            }
            
            // Perhaps the timer executed a read check late.. and then executes
            // the next read check on time which causes the time elapsed between
            // read checks to be small..
            
            // If less than 90% of the read check Time elapsed then abort this readcheck. 
            if( !allowReadCheck(elapsed) ) { // FUNKY qdox bug does not allow me to inline this expression.
                LOG.debug("Aborting read check.. Not enough time elapsed since last read check.");
                return;
            }
            
            lastRunTime = now;
            readCheck();
        }
    };
    
    private boolean allowReadCheck(long elapsed) {
        return elapsed > (readCheckTime * 9 / 10);
    }

    private final Runnable writeChecker = new Runnable() {
        long lastRunTime;
        public void run() {
            long now = System.currentTimeMillis();
            if( lastRunTime != 0 && LOG.isDebugEnabled() ) {
                LOG.debug(""+(now-lastRunTime)+" ms elapsed since last write check.");
                
            }
            lastRunTime = now; 
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

    final void writeCheck() {
            if (inSend.get()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("A send is in progress");
            }
            return;
        }

        if (!commandSent.get()) {
            if(LOG.isTraceEnabled()) {
                LOG.trace("No message sent since last write check, sending a KeepAliveInfo");
            }
            ASYNC_TASKS.execute(new Runnable() {
                public void run() {
                    try {
                        KeepAliveInfo info = new KeepAliveInfo();
                        info.setResponseRequired(true);
                        oneway(info);
                    } catch (IOException e) {
                        onException(e);
                    }
                };
            });
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message sent since last write check, resetting flag");
            }
        }

        commandSent.set(false);
    }

    final void readCheck() {
        if (inReceive.get()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("A receive is in progress");
            }
            return;
        }
        if (!commandReceived.get()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No message received since last read check for " + toString() + "! Throwing InactivityIOException.");
            }
           

            // TODO: use a thread pool for this..
            ASYNC_TASKS.execute(new Runnable() {
                public void run() {
                        onException(new InactivityIOException("Channel was inactive for too long: "+next.getRemoteAddress()));
                };
            });

        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message received since last read check, resetting flag: ");
            }
        }
        commandReceived.set(false);
    }

    public void onCommand(Object command) {
        commandReceived.set(true);
        inReceive.set(true);
        try {
            if (command.getClass() == KeepAliveInfo.class) {
                KeepAliveInfo info = (KeepAliveInfo) command;
                if (info.isResponseRequired()) {
                    try {
                        info.setResponseRequired(false);
                        oneway(info);
                    } catch (IOException e) {
                        onException(e);
                    }
                }
            } else {
                if (command.getClass() == WireFormatInfo.class) {
                    synchronized (this) {
                        IOException error = null;
                        remoteWireFormatInfo = (WireFormatInfo) command;
                        try {
                            startMonitorThreads();
                        } catch (IOException e) {
                            error = e;
                        }
                        if (error != null) {
                            onException(error);
                        }
                    }
                }
                synchronized (readChecker) {
                    transportListener.onCommand(command);
                }
            }
        } finally {
            
            inReceive.set(false);
        }
    }

    public void oneway(Object o) throws IOException {
        // Disable inactivity monitoring while processing a command.
        inSend.set(true);
        try {
            if (o.getClass() == WireFormatInfo.class) {
                synchronized (this) {
                    localWireFormatInfo = (WireFormatInfo)o;
                    startMonitorThreads();
                }
            }
            synchronized (writeChecker) {
                next.oneway(o);
            }
        } finally {
            commandSent.set(true);
            inSend.set(false);
        }
    }

    public void onException(IOException error) {
        if (monitorStarted.get()) {
            stopMonitorThreads();
        }
        transportListener.onException(error);
    }

    private synchronized void startMonitorThreads() throws IOException {
        if (monitorStarted.get()) {
            return;
        }
        if (localWireFormatInfo == null) {
            return;
        }
        if (remoteWireFormatInfo == null) {
            return;
        }

        readCheckTime = Math.min(localWireFormatInfo.getMaxInactivityDuration(), remoteWireFormatInfo.getMaxInactivityDuration());
        if (readCheckTime > 0) {
            monitorStarted.set(true);
            writeCheckerTask = new SchedulerTimerTask(writeChecker);
            readCheckerTask = new  SchedulerTimerTask(readChecker);
            writeCheckTime = readCheckTime/3;
            synchronized( InactivityMonitor.class ) {
            	if( CHECKER_COUNTER == 0 ) {
            	    READ_CHECK_TIMER = new Timer("InactivityMonitor ReadCheck");
            	    WRITE_CHECK_TIMER = new Timer("InactivityMonitor WriteCheck");
            	}
            	CHECKER_COUNTER++;
                WRITE_CHECK_TIMER.scheduleAtFixedRate(writeCheckerTask, writeCheckTime,writeCheckTime);
                READ_CHECK_TIMER.scheduleAtFixedRate(readCheckerTask, readCheckTime,readCheckTime);
            }
        }
    }

    /**
     *
     */
    private synchronized void stopMonitorThreads() {
        if (monitorStarted.compareAndSet(true, false)) {
            readCheckerTask.cancel();
            writeCheckerTask.cancel();
            synchronized( InactivityMonitor.class ) {
	            WRITE_CHECK_TIMER.purge();
	            READ_CHECK_TIMER.purge();
	            CHECKER_COUNTER--;
	            if(CHECKER_COUNTER==0) {
	            	WRITE_CHECK_TIMER.cancel();
	            	READ_CHECK_TIMER.cancel();
	            	WRITE_CHECK_TIMER = null;
	            	READ_CHECK_TIMER = null;
	            }
            }
        }
    }
    
       
    static {
        ASYNC_TASKS =   new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "InactivityMonitor Async Task: "+runnable);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

}
