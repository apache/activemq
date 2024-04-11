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
package org.apache.activemq.web.async;

import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletRequestWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper object to hold and track Async servlet requests. This is
 * a replacement for the deprecated/removed Jetty Continuation
 * API as that has long been replaced by the Servlet Async api.
 *
 */
public class AsyncServletRequest implements AsyncListener  {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncServletRequest.class);

    private static final String ACTIVEMQ_ASYNC_SERVLET_REQUEST = "activemq.async.servlet.request";

    private final ServletRequest request;
    private final AtomicReference<AsyncContext> contextRef = new AtomicReference<>();
    private final AtomicBoolean dispatched = new AtomicBoolean();
    private final AtomicBoolean expired = new AtomicBoolean();
    private final AtomicLong timeoutMs = new AtomicLong(-1);

    public AsyncServletRequest(ServletRequest request) {
        this.request = request;
    }

    public void complete() {
        final AsyncContext context = getContext();
        context.complete();
    }

    public void startAsync() {
        //Reset previous state
        this.dispatched.set(false);
        this.expired.set(false);

        final AsyncContext context = request.startAsync();
        contextRef.set(context);
        context.setTimeout(timeoutMs.get());
        context.addListener(this);
    }

    public void dispatch() {
        final AsyncContext context = getContext();
        this.dispatched.set(true);
        context.dispatch();
    }

    public void setAttribute(String name, Object attribute) {
        request.setAttribute(name, attribute);
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs.set(timeoutMs);
    }

    public boolean isInitial() {
        return this.request.getDispatcherType() != DispatcherType.ASYNC;
    }

    public boolean isExpired() {
        return this.expired.get();
    }

    public boolean isDispatched() {
        return dispatched.get();
    }

    public AsyncContext getAsyncContext() {
        return contextRef.get();
    }

    @Override
    public void onComplete(AsyncEvent event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ActiveMQAsyncRequest " + event + " completed.");
        }
    }

    @Override
    public void onTimeout(AsyncEvent event) {
        this.expired.set(true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ActiveMQAsyncRequest " + event + " timeout.");
        }

        final AsyncContext context = event.getAsyncContext();
        if (context != null) {
            // We must call complete and then set the status code to prevent a 500
            // error. The spec requires a 500 error on timeout unless complete() is called.
            context.complete();
            final ServletResponse response = context.getResponse();
            if (response instanceof HttpServletResponse) {
                ((HttpServletResponse) response).setStatus(HttpServletResponse.SC_NO_CONTENT);
            }
        }
    }

    @Override
    public void onError(AsyncEvent event) {
        final Throwable error = event.getThrowable();
        if (error != null) {
            LOG.warn("ActiveMQAsyncRequest " + event + " error: {}", error.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug(error.getMessage(), error);
            }
        }
    }

    @Override
    public void onStartAsync(AsyncEvent event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ActiveMQAsyncRequest " + event + " async started.");
        }
    }

    private AsyncContext getContext() {
        final AsyncContext context =  this.contextRef.get();
        if (context == null) {
            throw new IllegalStateException("Async request has not been started.");
        }
        return context;
    }

    /**
     * Look up the existing async request or create/ store a new request that be referenced later
     * @param request the ServletRequest
     * @return the existing or new ActiveMQAsyncRequest
     */
    public static AsyncServletRequest getAsyncRequest(final ServletRequest request) {
        Objects.requireNonNull(request, "ServletRequest must not be null");

        return Optional.ofNullable(request.getAttribute(ACTIVEMQ_ASYNC_SERVLET_REQUEST))
            .map(sr -> (AsyncServletRequest)sr).orElseGet(() -> {
                final AsyncServletRequest asyncRequest = new AsyncServletRequest(unwrap(request));
                request.setAttribute(ACTIVEMQ_ASYNC_SERVLET_REQUEST, asyncRequest);
                return asyncRequest;
        });
    }

    private static ServletRequest unwrap(ServletRequest request) {
        Objects.requireNonNull(request, "ServletRequest must not be null");

        //If it's a wrapper object then unwrap to get the source request
        while (request instanceof ServletRequestWrapper) {
            request = ((ServletRequestWrapper)request).getRequest();
        }

        return request;
    }
}
