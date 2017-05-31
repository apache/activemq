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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the thread pool for long running tasks. Long running tasks are not
 * always active but when they are active, they may need a few iterations of
 * processing for them to become idle. The manager ensures that each task is
 * processes but that no one task overtakes the system. This is somewhat like
 * cooperative multitasking.
 *
 * @org.apache.xbean.XBean
 */
public class TaskRunnerFactory implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRunnerFactory.class);
    private final AtomicReference<ExecutorService> executorRef = new AtomicReference<>();
    private int maxIterationsPerRun;
    private String name;
    private int priority;
    private boolean daemon;
    private final AtomicLong id = new AtomicLong(0);
    private boolean dedicatedTaskRunner;
    private long shutdownAwaitTermination = 30000;
    private final AtomicBoolean initDone = new AtomicBoolean(false);
    private int maxThreadPoolSize = getDefaultMaximumPoolSize();
    private RejectedExecutionHandler rejectedTaskHandler = null;
    private ClassLoader threadClassLoader;

    public TaskRunnerFactory() {
        this("ActiveMQ Task");
    }

    public TaskRunnerFactory(String name) {
        this(name, Thread.NORM_PRIORITY, true, 1000);
    }

    private TaskRunnerFactory(String name, int priority, boolean daemon, int maxIterationsPerRun) {
        this(name, priority, daemon, maxIterationsPerRun, false);
    }

    public TaskRunnerFactory(String name, int priority, boolean daemon, int maxIterationsPerRun, boolean dedicatedTaskRunner) {
        this(name, priority, daemon, maxIterationsPerRun, dedicatedTaskRunner, getDefaultMaximumPoolSize());
    }

    public TaskRunnerFactory(String name, int priority, boolean daemon, int maxIterationsPerRun, boolean dedicatedTaskRunner, int maxThreadPoolSize) {
        this.name = name;
        this.priority = priority;
        this.daemon = daemon;
        this.maxIterationsPerRun = maxIterationsPerRun;
        this.dedicatedTaskRunner = dedicatedTaskRunner;
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

    public void init() {
        if (!initDone.get()) {
            // If your OS/JVM combination has a good thread model, you may want to
            // avoid using a thread pool to run tasks and use a DedicatedTaskRunner instead.
            //AMQ-6602 - lock instead of using compareAndSet to prevent threads from seeing a null value
            //for executorRef inside createTaskRunner() on contention and creating a DedicatedTaskRunner
            synchronized(this) {
                //need to recheck if initDone is true under the lock
                if (!initDone.get()) {
                    if (dedicatedTaskRunner || "true".equalsIgnoreCase(System.getProperty("org.apache.activemq.UseDedicatedTaskRunner"))) {
                        executorRef.set(null);
                    } else {
                        executorRef.compareAndSet(null, createDefaultExecutor());
                    }
                    LOG.debug("Initialized TaskRunnerFactory[{}] using ExecutorService: {}", name, executorRef.get());
                    initDone.set(true);
                }
            }
        }
    }

    /**
     * Performs a shutdown only, by which the thread pool is shutdown by not graceful nor aggressively.
     *
     * @see ThreadPoolUtils#shutdown(java.util.concurrent.ExecutorService)
     */
    public void shutdown() {
        ExecutorService executor = executorRef.get();
        if (executor != null) {
            ThreadPoolUtils.shutdown(executor);
        }
        clearExecutor();
    }

    /**
     * Performs a shutdown now (aggressively) on the thread pool.
     *
     * @see ThreadPoolUtils#shutdownNow(java.util.concurrent.ExecutorService)
     */
    public void shutdownNow() {
        ExecutorService executor = executorRef.get();
        if (executor != null) {
            ThreadPoolUtils.shutdownNow(executor);
        }
        clearExecutor();
    }

    /**
     * Performs a graceful shutdown.
     *
     * @see ThreadPoolUtils#shutdownGraceful(java.util.concurrent.ExecutorService)
     */
    public void shutdownGraceful() {
        ExecutorService executor = executorRef.get();
        if (executor != null) {
            ThreadPoolUtils.shutdownGraceful(executor, shutdownAwaitTermination);
        }
        clearExecutor();
    }

    private void clearExecutor() {
        //clear under a lock to prevent threads from seeing initDone == true
        //but then getting null from executorRef
        synchronized(this) {
            executorRef.set(null);
            initDone.set(false);
        }
    }

    public TaskRunner createTaskRunner(Task task, String name) {
        init();
        ExecutorService executor = executorRef.get();
        if (executor != null) {
            return new PooledTaskRunner(executor, task, maxIterationsPerRun);
        } else {
            return new DedicatedTaskRunner(task, name, priority, daemon);
        }
    }

    @Override
    public void execute(Runnable runnable) {
        execute(runnable, name);
    }

    public void execute(Runnable runnable, String name) {
        init();
        LOG.trace("Execute[{}] runnable: {}", name, runnable);
        ExecutorService executor = executorRef.get();
        if (executor != null) {
            executor.execute(runnable);
        } else {
            doExecuteNewThread(runnable, name);
        }
    }

    private void doExecuteNewThread(Runnable runnable, String name) {
        String threadName = name + "-" + id.incrementAndGet();
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(daemon);

        LOG.trace("Created and running thread[{}]: {}", threadName, thread);
        thread.start();
    }

    protected ExecutorService createDefaultExecutor() {
        ThreadPoolExecutor rc = new ThreadPoolExecutor(getDefaultCorePoolSize(), getMaxThreadPoolSize(), getDefaultKeepAliveTime(), TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                String threadName = name + "-" + id.incrementAndGet();
                Thread thread = new Thread(runnable, threadName);
                thread.setDaemon(daemon);
                thread.setPriority(priority);
                if (threadClassLoader != null) {
                    thread.setContextClassLoader(threadClassLoader);
                }
                thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(final Thread t, final Throwable e) {
                        LOG.error("Error in thread '{}'", t.getName(), e);
                    }
                });

                LOG.trace("Created thread[{}]: {}", threadName, thread);
                return thread;
            }
        });

        if (rejectedTaskHandler != null) {
            rc.setRejectedExecutionHandler(rejectedTaskHandler);
        } else {
            rc.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }

        return rc;
    }

    public ExecutorService getExecutor() {
        return executorRef.get();
    }

    public void setExecutor(ExecutorService executor) {
        this.executorRef.set(executor);
    }

    public int getMaxIterationsPerRun() {
        return maxIterationsPerRun;
    }

    public void setMaxIterationsPerRun(int maxIterationsPerRun) {
        this.maxIterationsPerRun = maxIterationsPerRun;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    public boolean isDedicatedTaskRunner() {
        return dedicatedTaskRunner;
    }

    public void setDedicatedTaskRunner(boolean dedicatedTaskRunner) {
        this.dedicatedTaskRunner = dedicatedTaskRunner;
    }

    public int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public void setMaxThreadPoolSize(int maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

    public void setThreadClassLoader(ClassLoader threadClassLoader) {
        this.threadClassLoader = threadClassLoader;
    }

    public RejectedExecutionHandler getRejectedTaskHandler() {
        return rejectedTaskHandler;
    }

    public void setRejectedTaskHandler(RejectedExecutionHandler rejectedTaskHandler) {
        this.rejectedTaskHandler = rejectedTaskHandler;
    }

    public long getShutdownAwaitTermination() {
        return shutdownAwaitTermination;
    }

    public void setShutdownAwaitTermination(long shutdownAwaitTermination) {
        this.shutdownAwaitTermination = shutdownAwaitTermination;
    }

    private static int getDefaultCorePoolSize() {
        return Integer.getInteger("org.apache.activemq.thread.TaskRunnerFactory.corePoolSize", 0);
    }

    private static int getDefaultMaximumPoolSize() {
        return Integer.getInteger("org.apache.activemq.thread.TaskRunnerFactory.maximumPoolSize", Integer.MAX_VALUE);
    }

    private static int getDefaultKeepAliveTime() {
        return Integer.getInteger("org.apache.activemq.thread.TaskRunnerFactory.keepAliveTime", 30);
    }
}
