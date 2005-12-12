/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.thread;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Manages the thread pool for long running tasks.
 * 
 * Long running tasks are not always active but when they are active, they may
 * need a few iterations of processing for them to become idle. The manager
 * ensures that each task is processes but that no one task overtakes the
 * system.
 * 
 * This is kina like cooperative multitasking.
 * 
 * @version $Revision: 1.5 $
 */
public class TaskRunnerFactory {

    private Executor executor;
    private int maxIterationsPerRun = 1000;

    public TaskRunnerFactory() {
        setExecutor(createDefaultExecutor("ActiveMQ Task", Thread.NORM_PRIORITY, true));
    }

    public TaskRunnerFactory(String name, int priority, boolean daemon, int maxIterationsPerRun) {
        this.maxIterationsPerRun = maxIterationsPerRun;
        setExecutor(createDefaultExecutor(name, priority, daemon));
    }

    public TaskRunnerFactory(Executor executor, int maxIterationsPerRun) {
        this.executor = executor;
        this.maxIterationsPerRun = maxIterationsPerRun;
    }

    public TaskRunner createTaskRunner(Task task) {
        return new SimpleTaskRunner(executor, task, maxIterationsPerRun);
    }
    
    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public int getMaxIterationsPerRun() {
        return maxIterationsPerRun;
    }

    public void setMaxIterationsPerRun(int maxIterationsPerRun) {
        this.maxIterationsPerRun = maxIterationsPerRun;
    }

    protected Executor createDefaultExecutor(final String name, final int priority, final boolean daemon) {
        ThreadPoolExecutor rc = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, name);
                thread.setDaemon(daemon);
                thread.setPriority(priority);
                return thread;
            }
        });
        rc.allowCoreThreadTimeOut(true);
        return rc;
    }

}
