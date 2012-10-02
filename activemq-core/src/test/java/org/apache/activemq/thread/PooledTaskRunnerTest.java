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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

public class PooledTaskRunnerTest extends TestCase {
    private ExecutorService executor;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        executor = Executors.newCachedThreadPool(new IgnoreUncaughtExceptionThreadFactory());
    }

    @Override
    protected void tearDown() throws Exception {
        executor.shutdownNow();

        super.tearDown();
    }

    public void testNormalBehavior() throws Exception {
        final CountDownLatch latch = new CountDownLatch( 1 );

        PooledTaskRunner runner = new PooledTaskRunner( executor, new Task() {
            public boolean iterate() {
                latch.countDown();

                return false;
            }
        }, 1 );

        runner.wakeup();

        assertTrue( latch.await( 1, TimeUnit.SECONDS ) );

        runner.shutdown();
    }

    
    public void testWakeupResultsInThreadSafeCalls() throws Exception {
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, getName());
                thread.setDaemon(true);
                thread.setPriority(Thread.NORM_PRIORITY);
                return thread;
            }
        });
        final CountDownLatch doneLatch = new CountDownLatch( 100 );
        final AtomicInteger clashCount = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();


        final PooledTaskRunner runner = new PooledTaskRunner(executor, new Task() {
            String threadUnSafeVal = null;
            public boolean iterate() {
                if (threadUnSafeVal != null) {
                    clashCount.incrementAndGet();
                }
                threadUnSafeVal = Thread.currentThread().getName();
                count.incrementAndGet();
                doneLatch.countDown();
                if (!threadUnSafeVal.equals(Thread.currentThread().getName())) {
                    clashCount.incrementAndGet();
                }
                threadUnSafeVal = null;
                return false;
            }
        }, 1 );

        Runnable doWakeup = new Runnable() {
            public void run() {
                try {
                    runner.wakeup();
                } catch (InterruptedException ignored) {
                }
            }
        };
        
        final int iterations = 1000;
        for (int i=0; i< iterations; i++) {
            if (i%100 == 0) {
                Thread.sleep(10);
            }
            executor.execute(doWakeup);
        }    
        
        doneLatch.await(20, TimeUnit.SECONDS);
        assertEquals("thread safety clash", 0, clashCount.get());
        assertTrue("called more than once", count.get() > 1);
        runner.shutdown();
    }

    public void testShutsDownAfterRunnerFailure() throws Exception {
        Future<Object> future = executor.submit( new Callable<Object>() {
            public Object call() throws Exception {
                final CountDownLatch latch = new CountDownLatch( 1 );

                PooledTaskRunner runner = new PooledTaskRunner( executor, new Task() {
                    public boolean iterate() {
                        latch.countDown();

                        throw new RuntimeException();
                    }
                }, 1 );

                runner.wakeup();

                assertTrue( latch.await( 1, TimeUnit.SECONDS ) );

                runner.shutdown();

                return null;
            }
        } );

        try {
            future.get( 5, TimeUnit.SECONDS );
        } catch( TimeoutException e ) {
            fail( "TaskRunner did not shut down cleanly" );
        }
    }
    
    class IgnoreUncaughtExceptionThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        public Thread newThread(Runnable r) {
            Thread thread = threadFactory.newThread(r);
            thread.setUncaughtExceptionHandler(this);
            return thread;
        }
        
        public void uncaughtException(Thread t, Throwable e) {
            // ignore ie: no printStackTrace that would sully the test console
         }
    }
}