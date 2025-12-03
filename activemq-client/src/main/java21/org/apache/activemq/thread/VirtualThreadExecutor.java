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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.annotation.Experimental;
import org.slf4j.Logger;

/**
 * [AMQ-9394] Virtual Thread support
 *
 * JDK 21 introduces experimental virtual thread support which allow detaching of OS threads
 * from JVM threads. This allows the Java VM to continue working on other tasks while waiting
 * for OS operations (specifically I/O) to complete.
 *
 * JDK 25 provides improvements to Virtual Threads to prevent thread pinning in code blocks
 * that use synchronized.
 *
 * ActiveMQ support for Virtual Threads is currently experimental and profiling of various
 * scenarios and end-user feedback is needed to identify hotspots in order to realize
 * the full performance benefit.
 *
 * Additionally, usage to ThreadLocal needs to be removed/refactored to use ScopedValue
 * or another approach altogether. ActiveMQ has only a few ThreadLocal usages, but a key
 * area is SSLContext support. JIRA [AMQ-9753] will SSL Context ThreadLocal usage.
 *
 * Status:
 * v6.2.0 - Experimental Virtual Thread support introduced.
 *
 */
@Experimental("Tech Preview for Virtual Thread support")
public class VirtualThreadExecutor {

    private VirtualThreadExecutor() {}

    public static ExecutorService createVirtualThreadExecutorService(final String name, final AtomicLong id, final Logger LOG) {

        // [AMQ-9394] NOTE: Submitted JDK feature enhancement id: 9076243 to allow AtomicLong thread id param
        // https://bugs.java.com/bugdatabase/view_bug?bug_id=JDK-8320377
        Thread.Builder.OfVirtual threadBuilderOfVirtual = Thread.ofVirtual()
                .name(name)
                .uncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                    public void uncaughtException(final Thread t, final Throwable e) {
                        LOG.error("Error in thread '{}'", t.getName(), e);
                    }
                });

        // [AMQ-9394] Work around to have global thread id increment across ThreadFactories
        ThreadFactory virtualThreadFactory = threadBuilderOfVirtual.factory();
        ThreadFactory atomicThreadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread tmpThread = virtualThreadFactory.newThread(r);
                tmpThread.setName(tmpThread.getName() + id.incrementAndGet());
                return tmpThread;
            }
        };

        return Executors.newThreadPerTaskExecutor(atomicThreadFactory); // [AMQ-9394] Same as newVirtualThreadPerTaskExecutor
    }
}
