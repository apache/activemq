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
 * @deprecated do not use this class. Instead use {@link TaskRunnerFactory}
 */
@Deprecated
public final class DefaultThreadPools {

    private static final TaskRunnerFactory DEFAULT_TASK_RUNNER_FACTORY = new TaskRunnerFactory();
    
    private DefaultThreadPools() {        
    }
    
    @Deprecated
    public static TaskRunnerFactory getDefaultTaskRunnerFactory() {
        return DEFAULT_TASK_RUNNER_FACTORY;
    }

    /**
     * Useful to cleanup when it is known that all brokers and connections are
     * close and stopped, eg: when un deploying from web container.
     */
    public static void shutdown() {
        DEFAULT_TASK_RUNNER_FACTORY.shutdown();
    }
    
}
