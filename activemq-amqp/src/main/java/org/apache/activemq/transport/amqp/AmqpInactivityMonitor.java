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
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpInactivityMonitor extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpInactivityMonitor.class);

    private static ThreadPoolExecutor ASYNC_TASKS;
    private static int CHECKER_COUNTER;
    private static Timer ACTIVITY_CHECK_TIMER;

    private final AtomicBoolean failed = new AtomicBoolean(false);
    private IAmqpProtocolConverter protocolConverter;

    private long connectionTimeout = AmqpWireFormat.DEFAULT_CONNECTION_TIMEOUT;
    private SchedulerTimerTask connectCheckerTask;
    private final Runnable connectChecker = new Runnable() {

        private final long startTime = System.currentTimeMillis();

        @Override
        public void run() {

            long now = System.currentTimeMillis();

            if ((now - startTime) >= connectionTimeout && connectCheckerTask != null && !ASYNC_TASKS.isTerminating()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No connection attempt made in time for " + AmqpInactivityMonitor.this.toString() + "! Throwing InactivityIOException.");
                }
                ASYNC_TASKS.execute(new Runnable() {
                    @Override
                    public void run() {
                        onException(new InactivityIOException("Channel was inactive for too (>" + (connectionTimeout) + ") long: "
                            + next.getRemoteAddress()));
                    }
                });
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
        stopConnectChecker();
        next.stop();
    }

    @Override
    public void onException(IOException error) {
        if (failed.compareAndSet(false, true)) {
            stopConnectChecker();
            if (protocolConverter != null) {
                protocolConverter.onAMQPException(error);
            }
            transportListener.onException(error);
        }
    }

    public void setProtocolConverter(IAmqpProtocolConverter protocolConverter) {
        this.protocolConverter = protocolConverter;
    }

    public IAmqpProtocolConverter getProtocolConverter() {
        return protocolConverter;
    }

    synchronized void startConnectChecker(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        if (connectionTimeout > 0 && connectCheckerTask == null) {
            connectCheckerTask = new SchedulerTimerTask(connectChecker);

            long connectionCheckInterval = Math.min(connectionTimeout, 1000);

            synchronized (AbstractInactivityMonitor.class) {
                if (CHECKER_COUNTER == 0) {
                    ASYNC_TASKS = createExecutor();
                    ACTIVITY_CHECK_TIMER = new Timer("AMQP InactivityMonitor State Check", true);
                }
                CHECKER_COUNTER++;
                ACTIVITY_CHECK_TIMER.schedule(connectCheckerTask, connectionCheckInterval, connectionCheckInterval);
            }
        }
    }

    synchronized void stopConnectChecker() {
        if (connectCheckerTask != null) {
            connectCheckerTask.cancel();
            connectCheckerTask = null;

            synchronized (AbstractInactivityMonitor.class) {
                ACTIVITY_CHECK_TIMER.purge();
                CHECKER_COUNTER--;
                if (CHECKER_COUNTER == 0) {
                    ACTIVITY_CHECK_TIMER.cancel();
                    ACTIVITY_CHECK_TIMER = null;
                    ThreadPoolUtils.shutdown(ASYNC_TASKS);
                    ASYNC_TASKS = null;
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
        ThreadPoolExecutor exec = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory);
        exec.allowCoreThreadTimeOut(true);
        return exec;
    }
}
