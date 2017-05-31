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

package org.apache.activemq.transport.amqp;

import java.io.IOException;
import java.util.Timer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.thread.SchedulerTimerTask;
import org.apache.activemq.transport.AbstractInactivityMonitor;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpInactivityMonitor extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpInactivityMonitor.class);

    private static ThreadPoolExecutor ASYNC_TASKS;
    private static int CONNECTION_CHECK_TASK_COUNTER;
    private static Timer CONNECTION_CHECK_TASK_TIMER;
    private static int KEEPALIVE_TASK_COUNTER;
    private static Timer KEEPALIVE_TASK_TIMER;

    private final AtomicBoolean failed = new AtomicBoolean(false);
    private AmqpTransport amqpTransport;

    private long connectionTimeout = AmqpWireFormat.DEFAULT_CONNECTION_TIMEOUT;

    private SchedulerTimerTask connectCheckerTask;
    private final Runnable connectChecker = new Runnable() {

        private final long startTime = System.currentTimeMillis();

        @Override
        public void run() {
            long now = System.currentTimeMillis();

            if ((now - startTime) >= connectionTimeout && connectCheckerTask != null && !ASYNC_TASKS.isShutdown()) {
                LOG.debug("No connection attempt made in time for {}! Throwing InactivityIOException.", AmqpInactivityMonitor.this.toString());
                try {
                    ASYNC_TASKS.execute(new Runnable() {
                        @Override
                        public void run() {
                            onException(new InactivityIOException(
                                "Channel was inactive for too (>" + (connectionTimeout) + ") long: " + next.getRemoteAddress()));
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

    private SchedulerTimerTask keepAliveTask;
    private final Runnable keepAlive = new Runnable() {

        @Override
        public void run() {
            if (keepAliveTask != null && !ASYNC_TASKS.isShutdown()) {
                try {
                    ASYNC_TASKS.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                long nextIdleUpdate = amqpTransport.keepAlive();
                                if (nextIdleUpdate > 0) {
                                    synchronized (AmqpInactivityMonitor.this) {
                                        if (keepAliveTask != null) {
                                            keepAliveTask = new SchedulerTimerTask(keepAlive);
                                            KEEPALIVE_TASK_TIMER.schedule(keepAliveTask, nextIdleUpdate);
                                        }
                                    }
                                }
                            } catch (Exception ex) {
                                onException(new InactivityIOException(
                                    "Exception while performing idle checks for connection: " + next.getRemoteAddress()));
                            }
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

    public AmqpInactivityMonitor(Transport next, WireFormat wireFormat) {
        super(next);
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void stop() throws Exception {
        stopConnectionTimeoutChecker();
        stopKeepAliveTask();
        next.stop();
    }

    @Override
    public void onException(IOException error) {
        if (failed.compareAndSet(false, true)) {
            stopConnectionTimeoutChecker();
            if (amqpTransport != null) {
                amqpTransport.onException(error);
            }
            transportListener.onException(error);
        }
    }

    public void setAmqpTransport(AmqpTransport amqpTransport) {
        this.amqpTransport = amqpTransport;
    }

    public AmqpTransport getAmqpTransport() {
        return amqpTransport;
    }

    public synchronized void startConnectionTimeoutChecker(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        if (connectionTimeout > 0 && connectCheckerTask == null) {
            connectCheckerTask = new SchedulerTimerTask(connectChecker);

            long connectionCheckInterval = Math.min(connectionTimeout, 1000);

            synchronized (AbstractInactivityMonitor.class) {
                if (CONNECTION_CHECK_TASK_COUNTER == 0) {
                    if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                        ASYNC_TASKS = createExecutor();
                    }
                    CONNECTION_CHECK_TASK_TIMER = new Timer("AMQP InactivityMonitor State Check", true);
                }
                CONNECTION_CHECK_TASK_COUNTER++;
                CONNECTION_CHECK_TASK_TIMER.schedule(connectCheckerTask, connectionCheckInterval, connectionCheckInterval);
            }
        }
    }

    /**
     * Starts the keep alive task which will run after the given delay.
     *
     * @param nextKeepAliveCheck
     *        time in milliseconds to wait before performing the next keep-alive check.
     */
    public synchronized void startKeepAliveTask(long nextKeepAliveCheck) {
        if (nextKeepAliveCheck > 0 && keepAliveTask == null) {
            keepAliveTask = new SchedulerTimerTask(keepAlive);

            synchronized (AbstractInactivityMonitor.class) {
                if (KEEPALIVE_TASK_COUNTER == 0) {
                    if (ASYNC_TASKS == null || ASYNC_TASKS.isShutdown()) {
                        ASYNC_TASKS = createExecutor();
                    }
                    KEEPALIVE_TASK_TIMER = new Timer("AMQP InactivityMonitor Idle Update", true);
                }
                KEEPALIVE_TASK_COUNTER++;
                KEEPALIVE_TASK_TIMER.schedule(keepAliveTask, nextKeepAliveCheck);
            }
        }
    }

    public synchronized void stopConnectionTimeoutChecker() {
        if (connectCheckerTask != null) {
            connectCheckerTask.cancel();
            connectCheckerTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                CONNECTION_CHECK_TASK_TIMER.purge();
                CONNECTION_CHECK_TASK_COUNTER--;
                if (CONNECTION_CHECK_TASK_COUNTER == 0) {
                    CONNECTION_CHECK_TASK_TIMER.cancel();
                    CONNECTION_CHECK_TASK_TIMER = null;
                }
            }
        }
    }

    public synchronized void stopKeepAliveTask() {
        if (keepAliveTask != null) {
            keepAliveTask.cancel();
            keepAliveTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                KEEPALIVE_TASK_TIMER.purge();
                KEEPALIVE_TASK_COUNTER--;
                if (KEEPALIVE_TASK_COUNTER == 0) {
                    KEEPALIVE_TASK_TIMER.cancel();
                    KEEPALIVE_TASK_TIMER = null;
                }
            }
        }
    }

    private final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "AmqpInactivityMonitor Async Task: " + runnable);
            thread.setDaemon(true);
            return thread;
        }
    };

    private ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 90, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory);
        exec.allowCoreThreadTimeOut(true);
        return exec;
    }
}
