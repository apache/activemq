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
import java.io.InterruptedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.command.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FutureResponse {
    private static final Logger LOG = LoggerFactory.getLogger(FutureResponse.class);

    private final ResponseCallback responseCallback;
    private final TransportFilter transportFilter;

    private final ArrayBlockingQueue<Response> responseSlot = new ArrayBlockingQueue<Response>(1);

    public FutureResponse(ResponseCallback responseCallback) {
        this(responseCallback, null);
    }

    public FutureResponse(ResponseCallback responseCallback, TransportFilter transportFilter) {
        this.responseCallback = responseCallback;
        this.transportFilter = transportFilter;
    }

    public Response getResult() throws IOException {
        boolean hasInterruptPending = Thread.interrupted();
        try {
            return responseSlot.take();
        } catch (InterruptedException e) {
            hasInterruptPending = false;
            throw dealWithInterrupt(e);
        } finally {
            if (hasInterruptPending) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private InterruptedIOException dealWithInterrupt(InterruptedException e) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Operation interrupted: " + e, e);
        }
        InterruptedIOException interruptedIOException = new InterruptedIOException(e.getMessage());
        interruptedIOException.initCause(e);
        try {
            if (transportFilter != null) {
                transportFilter.onException(interruptedIOException);
            }
        } finally {
            Thread.currentThread().interrupt();
        }
        return interruptedIOException;
    }

    public Response getResult(int timeout) throws IOException {
        final boolean wasInterrupted = Thread.interrupted();
        try {
            Response result = responseSlot.poll(timeout, TimeUnit.MILLISECONDS);
            if (result == null && timeout > 0) {
                throw new RequestTimedOutIOException();
            }
            return result;
        } catch (InterruptedException e) {
            throw dealWithInterrupt(e);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void set(Response result) {
        if (responseSlot.offer(result)) {
            if (responseCallback != null) {
                responseCallback.onCompletion(this);
            }
        }
    }
}
