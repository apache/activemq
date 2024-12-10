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
package org.apache.activemq.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This concurrent data structure is used when the calling thread wants to wait until a counter gets to 0 but the counter
 * can go up and down (unlike a CountDownLatch which can only count down)
 */
public class CountdownLock {

    final Object counterMonitor = new Object();
    private final AtomicInteger counter = new AtomicInteger();

    public void doWaitForZero() {
        synchronized(counterMonitor){
            try {
                if (counter.get() > 0) {
                    counterMonitor.wait();
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public void doDecrement() {
        synchronized(counterMonitor){
            if (counter.decrementAndGet() == 0) {
                counterMonitor.notify();
            }
        }
    }

    public void doIncrement() {
        synchronized(counterMonitor){
            counter.incrementAndGet();
        }
    }
}
