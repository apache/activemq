/**
 *
 * Copyright 2004 The Apache Software Foundation
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
 */
package org.activemq.thread;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * 
 * @version $Revision$
 */
public class DefaultThreadPools {

    private static final Executor defaultPool = new ScheduledThreadPoolExecutor(5);
    private static final TaskRunnerFactory defaultTaskRunnerFactory = new TaskRunnerFactory(defaultPool,10);
    
    public static Executor getDeaultPool() {
        return defaultPool;
    }
    
    public static TaskRunnerFactory getDefaultTaskRunnerFactory() {
        return defaultTaskRunnerFactory;
    }
    
}
