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

import java.util.concurrent.Executor;

/**
 * @version $Revision: 1.1 $
 */
class PooledTaskRunner implements TaskRunner {

    private final int maxIterationsPerRun;
    private final Executor executor;
    private final Task task;
    private final Runnable runable;
    private boolean queued;
    private boolean shutdown;
    private boolean iterating;
    private Thread runningThread;

    public PooledTaskRunner(Executor executor, Task task, int maxIterationsPerRun) {
        this.executor = executor;
        this.maxIterationsPerRun = maxIterationsPerRun;
        this.task = task;
        runable = new Runnable() {
            public void run() {
                runningThread = Thread.currentThread();
                runTask();
                runningThread = null;
            }
        };
    }

    /**
     * We Expect MANY wakeup calls on the same TaskRunner.
     */
    public void wakeup() throws InterruptedException {
        synchronized (runable) {

            // When we get in here, we make some assumptions of state:
            // queued=false, iterating=false: wakeup() has not be called and
            // therefore task is not executing.
            // queued=true, iterating=false: wakeup() was called but, task
            // execution has not started yet
            // queued=false, iterating=true : wakeup() was called, which caused
            // task execution to start.
            // queued=true, iterating=true : wakeup() called after task
            // execution was started.

            if (queued || shutdown) {
                return;
            }

            queued = true;

            // The runTask() method will do this for me once we are done
            // iterating.
            if (!iterating) {
                executor.execute(runable);
            }
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
            // the check on the thread is done
            // because a call to iterate can result in
            // shutDown() being called, which would wait forever
            // waiting for iterating to finish
            if (runningThread != Thread.currentThread()) {
                if (iterating) {
                    runable.wait(timeout);
                }
            }
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown(0);
    }

    final void runTask() {

        synchronized (runable) {
            queued = false;
            if (shutdown) {
                iterating = false;
                runable.notifyAll();
                return;
            }
            iterating = true;
        }

        // Don't synchronize while we are iterating so that
        // multiple wakeup() calls can be executed concurrently.
        boolean done = false;
        for (int i = 0; i < maxIterationsPerRun; i++) {
            if (!task.iterate()) {
                done = true;
                break;
            }
        }

        synchronized (runable) {
            iterating = false;
            if (shutdown) {
                queued = false;
                runable.notifyAll();
                return;
            }

            // If we could not iterate all the items
            // then we need to re-queue.
            if (!done) {
                queued = true;
            }

            if (queued) {
                executor.execute(runable);
            }
        }
    }
}
