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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

public class TaskRunnerFactoryTest {

    /**
     * AMQ-6602 test
     * Test contention on createTaskRunner() to make sure that all threads end up
     * using a PooledTaskRunner
     *
     * @throws Exception
     */
    @Test
    public void testConcurrentTaskRunnerCreation() throws Exception {

        final TaskRunnerFactory factory = new TaskRunnerFactory();
        final ExecutorService service = Executors.newFixedThreadPool(10);
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(10);
        final List<TaskRunner> runners = Collections.<TaskRunner>synchronizedList(new ArrayList<TaskRunner>(10));

        for (int i = 0; i < 10; i++) {
            service.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        latch1.await();
                    } catch (InterruptedException e) {
                       throw new IllegalStateException(e);
                    }
                    runners.add(factory.createTaskRunner(new Task() {

                        @Override
                        public boolean iterate() {
                            return false;
                        }
                    }, "task"));
                    latch2.countDown();
                }
            });
        }

        latch1.countDown();
        latch2.await();

        for (TaskRunner runner : runners) {
            assertTrue(runner instanceof PooledTaskRunner);
        }
    }
}
