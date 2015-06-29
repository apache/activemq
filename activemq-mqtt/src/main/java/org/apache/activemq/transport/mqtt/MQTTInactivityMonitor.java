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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.activemq.transport.AbstractInactivityMonitor;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTInactivityMonitor extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTInactivityMonitor.class);

    private static final long DEFAULT_CHECK_TIME_MILLS = 30000;

    private static ThreadPoolExecutor ASYNC_TASKS;
    private static int CHECKER_COUNTER;
    private static Timer READ_CHECK_TIMER;

    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean inReceive = new AtomicBoolean(false);
    private final AtomicInteger lastReceiveCounter = new AtomicInteger(0);

    private final ReentrantLock sendLock = new ReentrantLock();
    private SchedulerTimerTask readCheckerTask;

    private long readGraceTime = DEFAULT_CHECK_TIME_MILLS;
    private long readKeepAliveTime = DEFAULT_CHECK_TIME_MILLS;
    private MQTTProtocolConverter protocolConverter;

    private long connectionTimeout = MQTTWireFormat.DEFAULT_CONNECTION_TIMEOUT;
    private SchedulerTimerTask connectCheckerTask;
    private final Runnable connectChecker = new Runnable() {

        private final long startTime = System.currentTimeMillis();

        @Override
        public void run() {

            long now = System.currentTimeMillis();

            if ((now - startTime) >= connectionTimeout && connectCheckerTask != null && !ASYNC_TASKS.isShutdown()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No CONNECT frame received in time for " + MQTTInactivityMonitor.this.toString() + "! Throwing InactivityIOException.");
                }

                try {
                    ASYNC_TASKS.execute(new Runnable() {
                        @Override
                        public void run() {
                            onException(new InactivityIOException("Channel was inactive for too (>" + (readKeepAliveTime + readGraceTime) + ") long: "
                                + next.getRemoteAddress()));
                        }
                    });
                } catch (RejectedExecutionException ex) {
                    if (!ASYNC_TASKS.isShutdown()) {
                        LOG.error("Async connection timeout task was rejected from the executor: ", ex);
                        throw ex;
                    }
                }
            }
        }
    };

    private final Runnable readChecker = new Runnable() {
        long lastReceiveTime = System.currentTimeMillis();

        @Override
        public void run() {

            long now = System.currentTimeMillis();
            int currentCounter = next.getReceiveCounter();
            int previousCounter = lastReceiveCounter.getAndSet(currentCounter);

            // for the PINGREQ/RESP frames, the currentCounter will be different
            // from previousCounter, and that
            // should be sufficient to indicate the connection is still alive.
            // If there were random data, or something
            // outside the scope of the spec, the wire format unrmarshalling
            // would fail, so we don't need to handle
            // PINGREQ/RESP explicitly here
            if (inReceive.get() || currentCounter != previousCounter) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Command received since last read check.");
                }
                lastReceiveTime = now;
                return;
            }

            if ((now - lastReceiveTime) >= readKeepAliveTime + readGraceTime && readCheckerTask != null && !ASYNC_TASKS.isShutdown()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No message received since last read check for " + MQTTInactivityMonitor.this.toString() + "! Throwing InactivityIOException.");
                }
                try {
                    ASYNC_TASKS.execute(new Runnable() {
                        @Override
                        public void run() {
                            onException(new InactivityIOException("Channel was inactive for too (>" +
                                        (connectionTimeout) + ") long: " + next.getRemoteAddress()));
                        }
                    });
                } catch (RejectedExecutionException ex) {
                    if (!ASYNC_TASKS.isShutdown()) {
                        LOG.error("Async connection timeout task was rejected from the executor: ", ex);
                        throw ex;
                    }
                }
            }
        }
    };

    public MQTTInactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void stop() throws Exception {
        stopReadChecker();
        stopConnectChecker();
        next.stop();
    }

    @Override
    public void onCommand(Object command) {
        inReceive.set(true);
        try {
            transportListener.onCommand(command);
        } finally {
            inReceive.set(false);
        }
    }

    @Override
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

    @Override
    public void onException(IOException error) {
        if (failed.compareAndSet(false, true)) {
            stopConnectChecker();
            stopReadChecker();
            if (protocolConverter != null) {
                protocolConverter.onTransportError();
            }
            transportListener.onException(error);
        }
    }

    public long getReadGraceTime() {
        return readGraceTime;
    }

    public void setReadGraceTime(long readGraceTime) {
        this.readGraceTime = readGraceTime;
    }

    public long getReadKeepAliveTime() {
        return readKeepAliveTime;
    }

    public void setReadKeepAliveTime(long readKeepAliveTime) {
        this.readKeepAliveTime = readKeepAliveTime;
    }

    public void setProtocolConverter(MQTTProtocolConverter protocolConverter) {
        this.protocolConverter = protocolConverter;
    }

    public MQTTProtocolConverter getProtocolConverter() {
        return protocolConverter;
    }

    public synchronized void startConnectChecker(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        if (connectionTimeout > 0 && connectCheckerTask == null) {
            connectCheckerTask = new SchedulerTimerTask(connectChecker);

            long connectionCheckInterval = Math.min(connectionTimeout, 1000);

            synchronized (AbstractInactivityMonitor.class) {
                if (CHECKER_COUNTER == 0) {
                    if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                        ASYNC_TASKS = createExecutor();
                    }
                    READ_CHECK_TIMER = new Timer("InactivityMonitor ReadCheck", true);
                }
                CHECKER_COUNTER++;
                READ_CHECK_TIMER.schedule(connectCheckerTask, connectionCheckInterval, connectionCheckInterval);
            }
        }
    }

    synchronized void startReadChecker() {
        if (readKeepAliveTime > 0 && readCheckerTask == null) {
            readCheckerTask = new SchedulerTimerTask(readChecker);

            synchronized (AbstractInactivityMonitor.class) {
                if (CHECKER_COUNTER == 0) {
                    if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                        ASYNC_TASKS = createExecutor();
                    }
                    READ_CHECK_TIMER = new Timer("InactivityMonitor ReadCheck", true);
                }
                CHECKER_COUNTER++;
                READ_CHECK_TIMER.schedule(readCheckerTask, readKeepAliveTime, readGraceTime);
            }
        }
    }

    synchronized void stopConnectChecker() {
        if (connectCheckerTask != null) {
            connectCheckerTask.cancel();
            connectCheckerTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                READ_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
                if (CHECKER_COUNTER == 0) {
                    READ_CHECK_TIMER.cancel();
                    READ_CHECK_TIMER = null;
                }
            }
        }
    }

    synchronized void stopReadChecker() {
        if (readCheckerTask != null) {
            readCheckerTask.cancel();
            readCheckerTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                READ_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
                if (CHECKER_COUNTER == 0) {
                    READ_CHECK_TIMER.cancel();
                    READ_CHECK_TIMER = null;
                }
            }
        }
    }

    private final ThreadFactory factory = new ThreadFactory() {
        @Override
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
