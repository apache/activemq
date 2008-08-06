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

import java.util.HashSet;
import java.util.Set;

/**
 * Return information about map update
 * 
 */
public class AsyncMapRequest implements RequestCallback{
    private final Object mutex = new Object();
    
    private Set<String> requests = new HashSet<String>();

    public void add(String id, MapRequest request) {
        request.setCallback(this);
        this.requests.add(id);
    }
    
    /**
     * Wait for requests
     * @param timeout
     * @return
     */
    public boolean isSuccess(long timeout) {
        long deadline = System.currentTimeMillis() + timeout;
        while (!this.requests.isEmpty()) {
            synchronized (this.mutex) {
                try {
                    this.mutex.wait(timeout);
                } catch (InterruptedException e) {
                    break;
                }
            }
            timeout = Math.max(deadline - System.currentTimeMillis(), 0);
        }
        return this.requests.isEmpty();
    }

    
    public void finished(String id) {
        this.requests.remove(id);
        
    }
}