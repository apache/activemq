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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * A TimeTask for a Runnable object
 *
 */
public class SchedulerTimerTask extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerTimerTask.class);

    private final Runnable task;

    public SchedulerTimerTask(Runnable task) {
        this.task = task;
    }

    public void run() {
        try {
            this.task.run();
        } catch (Error error) {
            // Very bad error. Can't swallow this but can log it.
            LOGGER.error("Scheduled task error", error);
            throw error;
        } catch (Exception exception) {
            // It is a known issue of java.util.Timer that if a TimerTask.run() method throws an exception, the
            // Timer's thread exits as if Timer.cancel() had been called. This makes the Timer completely unusable from
            // that point on - whenever the Timer triggers there is an 'IllegalStateException: Timer already cancelled'
            // and the task does not get executed.
            //
            // In practice, this makes the ActiveMQ client unable to refresh connections to brokers. Generally, tasks
            // are well coded to not throw exceptions but if they do, problems ensue...
            //
            // The answer, here, is to log the exception and then carry on without throwing further. This gives the
            // added benefit that, having seen the exception thrown, one can then go and fix the offending task!
            LOGGER.error("Scheduled task exception", exception);
        }
    }
}