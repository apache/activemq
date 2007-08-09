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
package org.apache.activemq.thread;

import java.util.HashMap;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @version $Revision$
 */
public class Scheduler {

    public static final ScheduledThreadPoolExecutor CLOCK_DAEMON = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "ActiveMQ Scheduler");
            thread.setDaemon(true);
            return thread;
        }
    });
    static {
        CLOCK_DAEMON.setKeepAliveTime(5, TimeUnit.SECONDS);
    }
    private static final HashMap CLOCK_TICKETS = new HashMap();

    public static synchronized void executePeriodically(final Runnable task, long period) {
        ScheduledFuture ticket = CLOCK_DAEMON.scheduleAtFixedRate(task, period, period, TimeUnit.MILLISECONDS);
        CLOCK_TICKETS.put(task, ticket);
    }

    public static synchronized void cancel(Runnable task) {
        ScheduledFuture ticket = (ScheduledFuture)CLOCK_TICKETS.remove(task);
        if (ticket != null) {
            ticket.cancel(false);
            if (ticket instanceof Runnable) {
                CLOCK_DAEMON.remove((Runnable)ticket);
            }
        }
    }

    public static void executeAfterDelay(final Runnable task, long redeliveryDelay) {
        CLOCK_DAEMON.schedule(task, redeliveryDelay, TimeUnit.MILLISECONDS);
    }

}
