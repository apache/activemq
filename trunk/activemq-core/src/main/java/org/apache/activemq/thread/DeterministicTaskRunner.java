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

import org.apache.activemq.util.MDCHelper;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 
 */
public class DeterministicTaskRunner implements TaskRunner {

    private final Executor executor;
    private final Task task;
    private final Runnable runable;
    private boolean shutdown;    
    
    /**Constructor
     * @param executor
     * @param task
     */
    public DeterministicTaskRunner(Executor executor, Task task) {
        this.executor = executor;
        this.task = task;
        final Map context = MDCHelper.getCopyOfContextMap();
        this.runable = new Runnable() {
            public void run() {
                Thread.currentThread();
                if (context != null) {
                    MDCHelper.setContextMap(context);
                }
                runTask();
            }
        };
    }

    /**
     * We Expect MANY wakeup calls on the same TaskRunner - but each
     * needs to run
     */
    public void wakeup() throws InterruptedException {
        synchronized (runable) {

            if (shutdown) {
                return;
            }
            executor.execute(runable);

        }
    }

    /**
     * shut down the task
     * 
     * @throws InterruptedException
     */
    public void shutdown(long timeout) throws InterruptedException {
        synchronized (runable) {
            shutdown = true;
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown(0);
    }

    final void runTask() {

        synchronized (runable) {
            if (shutdown) {
                runable.notifyAll();
                return;
            }
        }
        task.iterate();
    }
}
