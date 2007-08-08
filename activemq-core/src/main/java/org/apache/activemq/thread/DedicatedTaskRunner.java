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

/**
 * @version $Revision: 1.1 $
 */
class DedicatedTaskRunner implements TaskRunner {

    private final Task task;
    private final Thread thread;

    private final Object mutex = new Object();
    private boolean threadTerminated;
    private boolean pending;
    private boolean shutdown;

    public DedicatedTaskRunner(Task task, String name, int priority, boolean daemon) {
        this.task = task;
        thread = new Thread(name) {
            public void run() {
                runTask();
            }
        };
        thread.setDaemon(daemon);
        thread.setName(name);
        thread.setPriority(priority);
        thread.start();
    }

    /**
     */
    public void wakeup() throws InterruptedException {
        synchronized (mutex) {
            if (shutdown)
                return;
            pending = true;
            mutex.notifyAll();
        }
    }

    /**
     * shut down the task
     * 
     * @param timeout
     * @throws InterruptedException
     */
    public void shutdown(long timeout) throws InterruptedException {
        synchronized (mutex) {
            shutdown = true;
            pending = true;
            mutex.notifyAll();

            // Wait till the thread stops ( no need to wait if shutdown
            // is called from thread that is shutting down)
            if (Thread.currentThread() != thread && !threadTerminated) {
                mutex.wait(timeout);
            }
        }
    }

    /**
     * shut down the task
     * 
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        shutdown(0);
    }

    final void runTask() {

        try {
            while (true) {

                synchronized (mutex) {
                    pending = false;
                    if (shutdown) {
                        return;
                    }
                }

                if (!task.iterate()) {
                    // wait to be notified.
                    synchronized (mutex) {
                        if (shutdown) {
                            return;
                        }
                        while (!pending) {
                            mutex.wait();
                        }
                    }
                }

            }

        } catch (InterruptedException e) {
            // Someone really wants this thread to die off.
            Thread.currentThread().interrupt();
        } finally {
            // Make sure we notify any waiting threads that thread
            // has terminated.
            synchronized (mutex) {
                threadTerminated = true;
                mutex.notifyAll();
            }
        }
    }
}
