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
package org.apache.activemq.transport;

import java.io.IOException;

/**
 * 
 */
public class MutexTransport extends TransportFilter {

    private final Object writeMutex = new Object();

    public MutexTransport(Transport next) {
        super(next);
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        synchronized (writeMutex) {
            return next.asyncRequest(command, null);
        }
    }

    public void oneway(Object command) throws IOException {
        synchronized (writeMutex) {
            next.oneway(command);
        }
    }

    public Object request(Object command) throws IOException {
        synchronized (writeMutex) {
            return next.request(command);
        }
    }

    public Object request(Object command, int timeout) throws IOException {
        synchronized (writeMutex) {
            return next.request(command, timeout);
        }
    }

    public String toString() {
        return next.toString();
    }

}
