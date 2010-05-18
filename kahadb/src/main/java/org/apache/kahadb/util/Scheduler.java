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
package org.apache.kahadb.util;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @version $Revision$
 */
public final class Scheduler {

    

	public static final Timer CLOCK_DAEMON = new Timer("KahaDB Scheduler", true);
    private static final HashMap<Runnable, TimerTask> TIMER_TASKS = new HashMap<Runnable, TimerTask>();

    private Scheduler() {
    }

    public static synchronized void executePeriodically(final Runnable task, long period) {
    	TimerTask timerTask = new SchedulerTimerTask(task);
        CLOCK_DAEMON.scheduleAtFixedRate(timerTask, period, period);
        TIMER_TASKS.put(task, timerTask);
    }

    public static synchronized void cancel(Runnable task) {
    	TimerTask ticket = TIMER_TASKS.remove(task);
        if (ticket != null) {
            ticket.cancel();
            CLOCK_DAEMON.purge();//remove cancelled TimerTasks
        }
    }

    public static void executeAfterDelay(final Runnable task, long redeliveryDelay) {
    	TimerTask timerTask = new SchedulerTimerTask(task);
        CLOCK_DAEMON.schedule(timerTask, redeliveryDelay);
    }
    
    public static void shutdown() {
        CLOCK_DAEMON.cancel();
    }

}
