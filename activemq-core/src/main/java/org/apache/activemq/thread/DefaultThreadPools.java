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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * 
 * @version $Revision$
 */
public final class DefaultThreadPools {

    private static final Executor DEFAULT_POOL;
    static {
        DEFAULT_POOL = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "ActiveMQ Default Thread Pool Thread");
                thread.setDaemon(true);
                return thread;
            }
        });
    }    
    private static final TaskRunnerFactory DEFAULT_TASK_RUNNER_FACTORY = new TaskRunnerFactory();
    
    private DefaultThreadPools() {        
    }
    
    public static Executor getDefaultPool() {
        return DEFAULT_POOL;
    }
    
    public static TaskRunnerFactory getDefaultTaskRunnerFactory() {
        return DEFAULT_TASK_RUNNER_FACTORY;
    }
    
}
