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
package org.apache.activemq.transport.tcp;

import org.apache.activemq.command.Response;

/**
 * ResponseHolder utility
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ResponseHolder {
    protected Response response;
    protected Object lock = new Object();
    protected boolean notified;

    /**
     * Construct a receipt holder
     */
    public ResponseHolder() {
    }

    /**
     * Set the Response for this holder
     *
     * @param r
     */
    public void setResponse(Response r) {
        synchronized (lock) {
            this.response = r;
            notified = true;
            lock.notify();
        }
    }

    /**
     * Get the Response
     * 
     * @return the Response or null if it is closed
     */
    public Response getResponse() {
        return getResponse(0);
    }

    /**
     * wait upto <Code>timeout</Code> timeout ms to get a receipt
     *
     * @param timeout
     * @return
     */
    public Response getResponse(int timeout) {
        synchronized (lock) {
            if (!notified) {
                try {
                    lock.wait(timeout);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return this.response;
    }

    /**
     * close this holder
     */
    public void close() {
        synchronized (lock) {
            notified = true;
            lock.notifyAll();
        }
    }
}
