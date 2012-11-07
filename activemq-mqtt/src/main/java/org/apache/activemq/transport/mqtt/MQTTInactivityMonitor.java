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

package org.apache.activemq.transport.mqtt;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.activemq.transport.AbstractInactivityMonitor;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTInactivityMonitor extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTInactivityMonitor.class);

    private static final long DEFAULT_CHECK_TIME_MILLS = 30000;

    private static ThreadPoolExecutor ASYNC_TASKS;
    private static int CHECKER_COUNTER;
    private static Timer READ_CHECK_TIMER;

    private final AtomicBoolean monitorStarted = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean commandReceived = new AtomicBoolean(true);
    private final AtomicBoolean inReceive = new AtomicBoolean(false);
    private final AtomicInteger lastReceiveCounter = new AtomicInteger(0);

    private final ReentrantLock sendLock = new ReentrantLock();
    private SchedulerTimerTask readCheckerTask;

    private long readCheckTime = DEFAULT_CHECK_TIME_MILLS;
    private long initialDelayTime = DEFAULT_CHECK_TIME_MILLS;
    private boolean keepAliveResponseRequired;
    private MQTTProtocolConverter protocolConverter;

    private final Runnable readChecker = new Runnable() {
        long lastRunTime;

        public void run() {
            long now = System.currentTimeMillis();
            long elapsed = (now - lastRunTime);

            if (lastRunTime != 0 && LOG.isDebugEnabled()) {
                LOG.debug("" + elapsed + " ms elapsed since last read check.");
            }

            // Perhaps the timer executed a read check late.. and then executes
            // the next read check on time which causes the time elapsed between
            // read checks to be small..

            // If less than 90% of the read check Time elapsed then abort this readcheck.
            if (!allowReadCheck(elapsed)) { // FUNKY qdox bug does not allow me to inline this expression.
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

    public MQTTInactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next);
    }

    public void start() throws Exception {
        next.start();
        startMonitorThread();
    }

    public void stop() throws Exception {
        stopMonitorThread();
        next.stop();
    }

    final void readCheck() {
        int currentCounter = next.getReceiveCounter();
        int previousCounter = lastReceiveCounter.getAndSet(currentCounter);

        // for the PINGREQ/RESP frames, the currentCounter will be different from previousCounter, and that
        // should be sufficient to indicate the connection is still alive. If there were random data, or something
        // outside the scope of the spec, the wire format unrmarshalling would fail, so we don't need to handle
        // PINGREQ/RESP explicitly here
        if (inReceive.get() || currentCounter != previousCounter) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("A receive is in progress");
            }
            return;
        }
        if (!commandReceived.get() && monitorStarted.get() && !ASYNC_TASKS.isTerminating()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No message received since last read check for " + toString() + "! Throwing InactivityIOException.");
            }
            ASYNC_TASKS.execute(new Runnable() {
                public void run() {
                    if (protocolConverter != null) {
                        protocolConverter.onTransportError();
                    }
                    onException(new InactivityIOException("Channel was inactive for too (>" + readCheckTime + ") long: " + next.getRemoteAddress()));
                }
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
            transportListener.onCommand(command);
        } finally {
            inReceive.set(false);
        }
    }

    public void oneway(Object o) throws IOException {
        // To prevent the inactivity monitor from sending a message while we
        // are performing a send we take the lock.
        this.sendLock.lock();
        try {
            doOnewaySend(o);
        } finally {
            this.sendLock.unlock();
        }
    }

    // Must be called under lock, either read or write on sendLock.
    private void doOnewaySend(Object command) throws IOException {
        if (failed.get()) {
            throw new InactivityIOException("Cannot send, channel has already failed: " + next.getRemoteAddress());
        }
        next.oneway(command);
    }

    public void onException(IOException error) {
        if (failed.compareAndSet(false, true)) {
            stopMonitorThread();
            transportListener.onException(error);
        }
    }

    public long getReadCheckTime() {
        return readCheckTime;
    }

    public void setReadCheckTime(long readCheckTime) {
        this.readCheckTime = readCheckTime;
    }

    public long getInitialDelayTime() {
        return initialDelayTime;
    }

    public void setInitialDelayTime(long initialDelayTime) {
        this.initialDelayTime = initialDelayTime;
    }

    public boolean isKeepAliveResponseRequired() {
        return this.keepAliveResponseRequired;
    }

    public void setKeepAliveResponseRequired(boolean value) {
        this.keepAliveResponseRequired = value;
    }

    public boolean isMonitorStarted() {
        return this.monitorStarted.get();
    }

    public void setProtocolConverter(MQTTProtocolConverter protocolConverter) {
        this.protocolConverter = protocolConverter;
    }

    public MQTTProtocolConverter getProtocolConverter() {
        return protocolConverter;
    }

    synchronized void startMonitorThread() {

        // Not yet configured if this isn't set yet.
        if (protocolConverter == null) {
            return;
        }

        if (monitorStarted.get()) {
            return;
        }

        if (readCheckTime > 0) {
            readCheckerTask = new SchedulerTimerTask(readChecker);
        }

        if (readCheckTime > 0) {
            monitorStarted.set(true);
            synchronized (AbstractInactivityMonitor.class) {
                if (CHECKER_COUNTER == 0) {
                    ASYNC_TASKS = createExecutor();
                    READ_CHECK_TIMER = new Timer("InactivityMonitor ReadCheck", true);
                }
                CHECKER_COUNTER++;
                if (readCheckTime > 0) {
                    READ_CHECK_TIMER.schedule(readCheckerTask, initialDelayTime, readCheckTime);
                }
            }
        }
    }

    synchronized void stopMonitorThread() {
        if (monitorStarted.compareAndSet(true, false)) {
            if (readCheckerTask != null) {
                readCheckerTask.cancel();
            }

            synchronized (AbstractInactivityMonitor.class) {
                READ_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
                if (CHECKER_COUNTER == 0) {
                    READ_CHECK_TIMER.cancel();
                    READ_CHECK_TIMER = null;
                    ThreadPoolUtils.shutdown(ASYNC_TASKS);
                    ASYNC_TASKS = null;
                }
            }
        }
    }

    private ThreadFactory factory = new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "MQTTInactivityMonitor Async Task: " + runnable);
            thread.setDaemon(true);
            return thread;
        }
    };

    private ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory);
        exec.allowCoreThreadTimeOut(true);
        return exec;
    }
}
