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
import java.util.Timer;
import java.util.TimerTask;

import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class Scheduler extends ServiceSupport {

    private static final Logger LOG  = LoggerFactory.getLogger(Scheduler.class);
    private final String name;
    private Timer timer;
    private final HashMap<Runnable, TimerTask> timerTasks = new HashMap<Runnable, TimerTask>();

    public Scheduler(String name) {
        this.name = name;
    }

    public synchronized void executePeriodically(final Runnable task, long period) {
//IC see: https://issues.apache.org/jira/browse/AMQ-6555
        TimerTask existing = timerTasks.get(task);
        if (existing != null) {
            LOG.debug("Task {} already scheduled, cancelling and rescheduling", task);
            cancel(task);
        }
//IC see: https://issues.apache.org/jira/browse/AMQ-3031
        TimerTask timerTask = new SchedulerTimerTask(task);
//IC see: https://issues.apache.org/jira/browse/AMQ-2620
//IC see: https://issues.apache.org/jira/browse/AMQ-2568
        timer.schedule(timerTask, period, period);
        timerTasks.put(task, timerTask);
    }

    public synchronized void cancel(Runnable task) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3031
        TimerTask ticket = timerTasks.remove(task);
        if (ticket != null) {
            ticket.cancel();
//IC see: https://issues.apache.org/jira/browse/AMQ-3664
            timer.purge(); // remove cancelled TimerTasks
        }
    }

    public synchronized void executeAfterDelay(final Runnable task, long redeliveryDelay) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3031
        TimerTask timerTask = new SchedulerTimerTask(task);
        timer.schedule(timerTask, redeliveryDelay);
    }

    public void shutdown() {
        timer.cancel();
    }

    @Override
    protected synchronized void doStart() throws Exception {
        this.timer = new Timer(name, true);
    }

    @Override
    protected synchronized void doStop(ServiceStopper stopper) throws Exception {
//IC see: https://issues.apache.org/jira/browse/AMQ-5251
        if (this.timer != null) {
            this.timer.cancel();
        }
    }

    public String getName() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3310
        return name;
    }
}
