/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.thread;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;

/**
 * 
 * @version $Revision$
 */
public class DefaultThreadPools {

    private static final Executor defaultPool;
    static {
        defaultPool = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "ActiveMQ Default Thread Pool Thread");
                thread.setDaemon(true);
                return thread;
            }
        });
    }
    
    private static final TaskRunnerFactory defaultTaskRunnerFactory = new TaskRunnerFactory();
    
    public static Executor getDefaultPool() {
        return defaultPool;
    }
    
    public static TaskRunnerFactory getDefaultTaskRunnerFactory() {
        return defaultTaskRunnerFactory;
    }
    
}
