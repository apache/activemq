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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.thread.Scheduler;
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

    private WireFormatInfo localWireFormatInfo;
    private WireFormatInfo remoteWireFormatInfo;
    private final AtomicBoolean monitorStarted = new AtomicBoolean(false);

    private final AtomicBoolean commandSent = new AtomicBoolean(false);
    private final AtomicBoolean inSend = new AtomicBoolean(false);

    private final AtomicBoolean commandReceived = new AtomicBoolean(true);
    private final AtomicBoolean inReceive = new AtomicBoolean(false);

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

    final void writeCheck() {
        synchronized (writeChecker) {
            if (inSend.get()) {
                LOG.trace("A send is in progress");
                return;
            }

            if (!commandSent.get()) {
                LOG.trace("No message sent since last write check, sending a KeepAliveInfo");
                try {
                    next.oneway(new KeepAliveInfo());
                } catch (IOException e) {
                    onException(e);
                }
            } else {
                LOG.trace("Message sent since last write check, resetting flag");
            }

            commandSent.set(false);
        }
    }

    final void readCheck() {
        synchronized (readChecker) {
            if (inReceive.get()) {
                LOG.trace("A receive is in progress");
                return;
            }

            if (!commandReceived.get()) {
                LOG.debug("No message received since last read check for " + toString() + "! Throwing InactivityIOException.");
                onException(new InactivityIOException("Channel was inactive for too long."));
            } else {
                LOG.trace("Message received since last read check, resetting flag: ");
            }

            commandReceived.set(false);
        }

    }

    public void onCommand(Object command) {
        synchronized (readChecker) {
            inReceive.set(true);
            try {
                if (command.getClass() == WireFormatInfo.class) {
                    synchronized (this) {
                        remoteWireFormatInfo = (WireFormatInfo)command;
                        try {
                            startMonitorThreads();
                        } catch (IOException e) {
                            onException(e);
                        }
                    }
                }
                transportListener.onCommand(command);
            } finally {
                inReceive.set(false);
                commandReceived.set(true);
            }
        }
    }

    public void oneway(Object o) throws IOException {
        synchronized (writeChecker) {
            // Disable inactivity monitoring while processing a command.
            inSend.set(true);
            commandSent.set(true);
            try {
                if (o.getClass() == WireFormatInfo.class) {
                    synchronized (this) {
                        localWireFormatInfo = (WireFormatInfo)o;
                        startMonitorThreads();
                    }
                }
                next.oneway(o);
            } finally {
                inSend.set(false);
            }
        }
    }

    public void onException(IOException error) {
        if (monitorStarted.get()) {
            stopMonitorThreads();
        }
        getTransportListener().onException(error);
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

        long l = Math.min(localWireFormatInfo.getMaxInactivityDuration(), remoteWireFormatInfo.getMaxInactivityDuration());
        if (l > 0) {
            monitorStarted.set(true);
            Scheduler.executePeriodically(writeChecker, l / 2);
            Scheduler.executePeriodically(readChecker, l);
        }
    }

    /**
     *
     */
    private synchronized void stopMonitorThreads() {
        if (monitorStarted.compareAndSet(true, false)) {
            Scheduler.cancel(readChecker);
            Scheduler.cancel(writeChecker);
        }
    }

}
