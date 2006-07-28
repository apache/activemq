/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.activemq.command.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

public class FutureResponse {
    private static final Log log = LogFactory.getLog(FutureResponse.class);
           
    private final ResponseCallback responseCallback;
    private final ArrayBlockingQueue responseSlot = new ArrayBlockingQueue(1);
    
    public FutureResponse(ResponseCallback responseCallback) {
        this.responseCallback = responseCallback;
    }

    public Response getResult() throws IOException {
        try {
            return (Response) responseSlot.take();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (log.isDebugEnabled()) {
                log.debug("Operation interupted: " + e, e);
            }
            throw new InterruptedIOException("Interrupted.");
        }
    }
    
    public Response getResult(int timeout) throws IOException {
        try {
            return (Response) responseSlot.poll(timeout,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted.");
        }
    }
    
    public void set(Response result) {
        if( responseSlot.offer(result) ) {
            if( responseCallback !=null ) {
                responseCallback.onCompletion(this);
            }        
        }
    }
}
