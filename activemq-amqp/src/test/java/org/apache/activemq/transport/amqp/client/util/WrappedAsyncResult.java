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
package org.apache.activemq.transport.amqp.client.util;

/**
 * Base class used to wrap one AsyncResult with another.
 */
public abstract class WrappedAsyncResult implements AsyncResult {

    protected final AsyncResult wrapped;

    /**
     * Create a new WrappedAsyncResult for the target AsyncResult
     */
    public WrappedAsyncResult(AsyncResult wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void onFailure(Throwable result) {
        if (wrapped != null) {
            wrapped.onFailure(result);
        }
    }

    @Override
    public void onSuccess() {
        if (wrapped != null) {
            wrapped.onSuccess();
        }
    }

    @Override
    public boolean isComplete() {
        if (wrapped != null) {
            return wrapped.isComplete();
        }

        return false;
    }

    public AsyncResult getWrappedRequest() {
        return wrapped;
    }
}
