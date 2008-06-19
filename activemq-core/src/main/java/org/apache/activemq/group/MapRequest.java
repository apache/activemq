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
package org.apache.activemq.group;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Return information about map update
 * 
 */
public class MapRequest {
    private final AtomicBoolean done = new AtomicBoolean();
    private Object response;

    Object get(int timeout) {
        synchronized (done) {
            if (done.get() == false && response == null) {
                try {
                    done.wait(timeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return this.response;
    }

    void put(Object response) {
        this.response = response;
        cancel();
    }

    void cancel() {
        done.set(true);
        synchronized (done) {
            done.notifyAll();
        }
    }
}
