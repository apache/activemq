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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to make sure that commands are arriving periodically from the peer of
 * the transport.
 */
public abstract class AbstractInactivityMonitor extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInactivityMonitor.class);

    private static final long DEFAULT_CHECK_TIME_MILLS = 30000;

    private static ThreadPoolExecutor ASYNC_TASKS;
    private static int CHECKER_COUNTER;
    private static Timer READ_CHECK_TIMER;
    private static Timer WRITE_CHECK_TIMER;

    private final AtomicBoolean monitorStarted = new AtomicBoolean(false);

    private final AtomicBoolean commandSent = new AtomicBoolean(false);
    private final AtomicBoolean inSend = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);

    private final AtomicBoolean commandReceived = new AtomicBoolean(true);
    private final AtomicBoolean inReceive = new AtomicBoolean(false);
    private final AtomicInteger lastReceiveCounter = new AtomicInteger(0);

    private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();

    private SchedulerTimerTask connectCheckerTask;
    private SchedulerTimerTask writeCheckerTask;
    private SchedulerTimerTask readCheckerTask;

    private long connectAttemptTimeout = DEFAULT_CHECK_TIME_MILLS;
    private long readCheckTime = DEFAULT_CHECK_TIME_MILLS;
    private long writeCheckTime = DEFAULT_CHECK_TIME_MILLS;
    private long initialDelayTime = DEFAULT_CHECK_TIME_MILLS;
    private boolean useKeepAlive = true;
    private boolean keepAliveResponseRequired;

    protected WireFormat wireFormat;

    private final Runnable connectChecker = new Runnable() {

        private final long startTime = System.currentTimeMillis();

        @Override
        public void run() {
            long now = System.currentTimeMillis();

            if ((now - startTime) >= connectAttemptTimeout && connectCheckerTask != null && !ASYNC_TASKS.isShutdown()) {
                LOG.debug("No connection attempt made in time for {}! Throwing InactivityIOException.", AbstractInactivityMonitor.this.toString());
                try {
                    ASYNC_TASKS.execute(new Runnable() {
                        @Override
                        public void run() {
                            onException(new InactivityIOException(
                                "Channel was inactive (no connection attempt made) for too (>" + (connectAttemptTimeout) + ") long: " + next.getRemoteAddress()));
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
        long lastRunTime;

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            long elapsed = (now - lastRunTime);

            if (lastRunTime != 0) {
                LOG.debug("{}ms elapsed since last read check.", elapsed);
            }

            // Perhaps the timer executed a read check late.. and then executes
            // the next read check on time which causes the time elapsed between
            // read checks to be small..

            // If less than 90% of the read check Time elapsed then abort this
            // read check.
            if (!allowReadCheck(elapsed)) {
                LOG.debug("Aborting read check...Not enough time elapsed since last read check.");
                return;
            }

            lastRunTime = now;
            readCheck();
        }

        @Override
        public String toString() {
            return "ReadChecker";
        }
    };

    private boolean allowReadCheck(long elapsed) {
        return elapsed > (readCheckTime * 9 / 10);
    }

    private final Runnable writeChecker = new Runnable() {
        long lastRunTime;

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            if (lastRunTime != 0) {
                LOG.debug("{}: {}ms elapsed since last write check.", this, (now - lastRunTime));
            }
            lastRunTime = now;
            writeCheck();
        }

        @Override
        public String toString() {
            return "WriteChecker";
        }
    };

    public AbstractInactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next);
        this.wireFormat = wireFormat;
    }

    @Override
    public void start() throws Exception {
        next.start();
        startMonitorThreads();
    }

    @Override
    public void stop() throws Exception {
        stopMonitorThreads();
        next.stop();
    }

    final void writeCheck() {
        if (inSend.get()) {
            LOG.trace("Send in progress. Skipping write check.");
            return;
        }

        if (!commandSent.get() && useKeepAlive && monitorStarted.get() && !ASYNC_TASKS.isShutdown()) {
            LOG.trace("{} no message sent since last write check, sending a KeepAliveInfo", this);

            try {
                ASYNC_TASKS.execute(new Runnable() {
                    @Override
                    public void run() {
                        LOG.debug("Running {}", this);
                        if (monitorStarted.get()) {
                            try {
                                // If we can't get the lock it means another
                                // write beat us into the
                                // send and we don't need to heart beat now.
                                if (sendLock.writeLock().tryLock()) {
                                    KeepAliveInfo info = new KeepAliveInfo();
                                    info.setResponseRequired(keepAliveResponseRequired);
                                    doOnewaySend(info);
                                }
                            } catch (IOException e) {
                                onException(e);
                            } finally {
                                if (sendLock.writeLock().isHeldByCurrentThread()) {
                                    sendLock.writeLock().unlock();
                                }
                            }
                        }
                    }

                    @Override
                    public String toString() {
                        return "WriteCheck[" + getRemoteAddress() + "]";
                    };
                });
            } catch (RejectedExecutionException ex) {
                if (!ASYNC_TASKS.isShutdown()) {
                    LOG.error("Async write check was rejected from the executor: ", ex);
                    throw ex;
                }
            }
        } else {
            LOG.trace("{} message sent since last write check, resetting flag.", this);
        }

        commandSent.set(false);
    }

    final void readCheck() {
        int currentCounter = next.getReceiveCounter();
        int previousCounter = lastReceiveCounter.getAndSet(currentCounter);
        if (inReceive.get() || currentCounter != previousCounter) {
            LOG.trace("A receive is in progress, skipping read check.");
            return;
        }
        if (!commandReceived.get() && monitorStarted.get() && !ASYNC_TASKS.isShutdown()) {
            LOG.debug("No message received since last read check for {}. Throwing InactivityIOException.", this);

            try {
                ASYNC_TASKS.execute(new Runnable() {
                    @Override
                    public void run() {
                        LOG.debug("Running {}", this);
                        onException(new InactivityIOException("Channel was inactive for too (>" + readCheckTime + ") long: " + next.getRemoteAddress()));
                    }

                    @Override
                    public String toString() {
                        return "ReadCheck[" + getRemoteAddress() + "]";
                    };
                });
            } catch (RejectedExecutionException ex) {
                if (!ASYNC_TASKS.isShutdown()) {
                    LOG.error("Async read check was rejected from the executor: ", ex);
                    throw ex;
                }
            }
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message received since last read check, resetting flag: ");
            }
        }
        commandReceived.set(false);
    }

    protected abstract void processInboundWireFormatInfo(WireFormatInfo info) throws IOException;

    protected abstract void processOutboundWireFormatInfo(WireFormatInfo info) throws IOException;

    @Override
    public void onCommand(Object command) {
        commandReceived.set(true);
        inReceive.set(true);
        try {
            if (command.getClass() == KeepAliveInfo.class) {
                KeepAliveInfo info = (KeepAliveInfo) command;
                if (info.isResponseRequired()) {
                    sendLock.readLock().lock();
                    try {
                        info.setResponseRequired(false);
                        oneway(info);
                    } catch (IOException e) {
                        onException(e);
                    } finally {
                        sendLock.readLock().unlock();
                    }
                }
            } else {
                if (command.getClass() == WireFormatInfo.class) {
                    synchronized (this) {
                        try {
                            processInboundWireFormatInfo((WireFormatInfo) command);
                        } catch (IOException e) {
                            onException(e);
                        }
                    }
                }

                transportListener.onCommand(command);
            }
        } finally {
            inReceive.set(false);
        }
    }

    @Override
    public void oneway(Object o) throws IOException {
        // To prevent the inactivity monitor from sending a message while we
        // are performing a send we take a read lock. The inactivity monitor
        // sends its Heart-beat commands under a write lock. This means that
        // the MutexTransport is still responsible for synchronizing sends
        sendLock.readLock().lock();
        inSend.set(true);
        try {
            doOnewaySend(o);
        } finally {
            commandSent.set(true);
            inSend.set(false);
            sendLock.readLock().unlock();
        }
    }

    // Must be called under lock, either read or write on sendLock.
    private void doOnewaySend(Object command) throws IOException {
        if (failed.get()) {
            throw new InactivityIOException("Cannot send, channel has already failed: " + next.getRemoteAddress());
        }
        if (command.getClass() == WireFormatInfo.class) {
            synchronized (this) {
                processOutboundWireFormatInfo((WireFormatInfo) command);
            }
        }
        next.oneway(command);
    }

    @Override
    public void onException(IOException error) {
        if (failed.compareAndSet(false, true)) {
            stopMonitorThreads();
            if (sendLock.writeLock().isHeldByCurrentThread()) {
                sendLock.writeLock().unlock();
            }
            transportListener.onException(error);
        }
    }

    public void setUseKeepAlive(boolean val) {
        useKeepAlive = val;
    }

    public long getConnectAttemptTimeout() {
        return connectAttemptTimeout;
    }

    public void setConnectAttemptTimeout(long connectionTimeout) {
        this.connectAttemptTimeout = connectionTimeout;
    }

    public long getReadCheckTime() {
        return readCheckTime;
    }

    public void setReadCheckTime(long readCheckTime) {
        this.readCheckTime = readCheckTime;
    }

    public long getWriteCheckTime() {
        return writeCheckTime;
    }

    public void setWriteCheckTime(long writeCheckTime) {
        this.writeCheckTime = writeCheckTime;
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

    abstract protected boolean configuredOk() throws IOException;

    public synchronized void startConnectCheckTask() {
        startConnectCheckTask(getConnectAttemptTimeout());
    }

    public synchronized void startConnectCheckTask(long connectionTimeout) {
        if (connectionTimeout <= 0) {
            return;
        }

        LOG.trace("Starting connection check task for: {}", this);

        this.connectAttemptTimeout = connectionTimeout;

        if (connectCheckerTask == null) {
            connectCheckerTask = new SchedulerTimerTask(connectChecker);

            synchronized (AbstractInactivityMonitor.class) {
                if (CHECKER_COUNTER == 0) {
                    if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                        ASYNC_TASKS = createExecutor();
                    }
                    if (READ_CHECK_TIMER == null) {
                        READ_CHECK_TIMER = new Timer("ActiveMQ InactivityMonitor ReadCheckTimer", true);
                    }
                }
                CHECKER_COUNTER++;
                READ_CHECK_TIMER.schedule(connectCheckerTask, connectionTimeout);
            }
        }
    }

    public synchronized void stopConnectCheckTask() {
        if (connectCheckerTask != null) {
            LOG.trace("Stopping connection check task for: {}", this);
            connectCheckerTask.cancel();
            connectCheckerTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                READ_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
            }
        }
    }

    protected synchronized void startMonitorThreads() throws IOException {
        if (monitorStarted.get()) {
            return;
        }

        if (!configuredOk()) {
            return;
        }

        if (readCheckTime > 0) {
            readCheckerTask = new SchedulerTimerTask(readChecker);
        }

        if (writeCheckTime > 0) {
            writeCheckerTask = new SchedulerTimerTask(writeChecker);
        }

        if (writeCheckTime > 0 || readCheckTime > 0) {
            monitorStarted.set(true);
            synchronized (AbstractInactivityMonitor.class) {
                if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                    ASYNC_TASKS = createExecutor();
                }
                if (READ_CHECK_TIMER == null) {
                    READ_CHECK_TIMER = new Timer("ActiveMQ InactivityMonitor ReadCheckTimer", true);
                }
                if (WRITE_CHECK_TIMER == null) {
                    WRITE_CHECK_TIMER = new Timer("ActiveMQ InactivityMonitor WriteCheckTimer", true);
                }

                CHECKER_COUNTER++;
                if (readCheckTime > 0) {
                    READ_CHECK_TIMER.schedule(readCheckerTask, initialDelayTime, readCheckTime);
                }
                if (writeCheckTime > 0) {
                    WRITE_CHECK_TIMER.schedule(writeCheckerTask, initialDelayTime, writeCheckTime);
                }
            }
        }
    }

    protected synchronized void stopMonitorThreads() {
        stopConnectCheckTask();
        if (monitorStarted.compareAndSet(true, false)) {
            if (readCheckerTask != null) {
                readCheckerTask.cancel();
            }
            if (writeCheckerTask != null) {
                writeCheckerTask.cancel();
            }

            synchronized (AbstractInactivityMonitor.class) {
                WRITE_CHECK_TIMER.purge();
                READ_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
                if (CHECKER_COUNTER == 0) {
                    WRITE_CHECK_TIMER.cancel();
                    READ_CHECK_TIMER.cancel();
                    WRITE_CHECK_TIMER = null;
                    READ_CHECK_TIMER = null;
                    try {
                        ThreadPoolUtils.shutdownGraceful(ASYNC_TASKS, TimeUnit.SECONDS.toMillis(10));
                    } finally {
                        ASYNC_TASKS = null;
                    }
                }
            }
        }
    }

    private final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "ActiveMQ InactivityMonitor Worker");
            thread.setDaemon(true);
            return thread;
        }
    };

    private ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(0, Integer.MAX_VALUE, getDefaultKeepAliveTime(), TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory);
        exec.allowCoreThreadTimeOut(true);
        return exec;
    }

    private static int getDefaultKeepAliveTime() {
        return Integer.getInteger("org.apache.activemq.transport.AbstractInactivityMonitor.keepAliveTime", 30);
    }
}
