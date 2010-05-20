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

/**
 * @version $Revision$
 */
public final class Scheduler extends ServiceSupport { 
    private final String name;
	private Timer timer;
    private final HashMap<Runnable, TimerTask> timerTasks = new HashMap<Runnable, TimerTask>();
    
    public Scheduler (String name) {
        this.name = name;
    }
        
    public void executePeriodically(final Runnable task, long period) {
    	TimerTask timerTask = new SchedulerTimerTask(task);
        timer.scheduleAtFixedRate(timerTask, period, period);
        timerTasks.put(task, timerTask);
    }

    /*
     * execute on rough schedual based on termination of last execution. There is no
     * compensation (two runs in quick succession) for delays
     */
    public synchronized void schedualPeriodically(final Runnable task, long period) {
        TimerTask timerTask = new SchedulerTimerTask(task);
        timer.schedule(timerTask, period, period);
        timerTasks.put(task, timerTask);
    }
    
    public synchronized void cancel(Runnable task) {
    	TimerTask ticket = timerTasks.remove(task);
        if (ticket != null) {
            ticket.cancel();
            timer.purge();//remove cancelled TimerTasks
        }
    }

    public synchronized void executeAfterDelay(final Runnable task, long redeliveryDelay) {
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
       if (this.timer != null) {
           this.timer.cancel();
       }
        
    }
}
