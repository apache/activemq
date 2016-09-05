/*
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
package org.apache.activemq.transport.amqp.client;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;

/**
 * AmqpResource specification.
 *
 * All AMQP types should implement this interface to allow for control of state
 * and configuration details.
 */
public interface AmqpResource extends AmqpEventSink {

    /**
     * Perform all the work needed to open this resource and store the request
     * until such time as the remote peer indicates the resource has become active.
     *
     * @param request
     *        The initiating request that triggered this open call.
     */
    void open(AsyncResult request);

    /**
     * @return if the resource has moved to the opened state on the remote.
     */
    boolean isOpen();

    /**
     * Called to indicate that this resource is now remotely opened.  Once opened a
     * resource can start accepting incoming requests.
     */
    void opened();

    /**
     * Perform all work needed to close this resource and store the request
     * until such time as the remote peer indicates the resource has been closed.
     *
     * @param request
     *        The initiating request that triggered this close call.
     */
    void close(AsyncResult request);

    /**
     * Perform all work needed to detach this resource and store the request
     * until such time as the remote peer indicates the resource has been detached.
     *
     * @param request
     *        The initiating request that triggered this detach call.
     */
    void detach(AsyncResult request);

    /**
     * @return if the resource has moved to the closed state on the remote.
     */
    boolean isClosed();

    /**
     * Called to indicate that this resource is now remotely closed.  Once closed a
     * resource can not accept any incoming requests.
     */
    void closed();

    /**
     * Sets the failed state for this Resource and triggers a failure signal for
     * any pending ProduverRequest.
     */
    void failed();

    /**
     * Called to indicate that the remote end has become closed but the resource
     * was not awaiting a close.  This could happen during an open request where
     * the remote does not set an error condition or during normal operation.
     *
     * @param connection
     *        The connection that owns this resource.
     */
    void remotelyClosed(AmqpConnection connection);

    /**
     * Called to indicate that the local end has become closed but the resource
     * was not awaiting a close.  This could happen during an open request where
     * the remote does not set an error condition or during normal operation.
     *
     * @param connection
     *        The connection that owns this resource.
     * @param error
     *        The error that triggered the local close of this resource.
     */
    void locallyClosed(AmqpConnection connection, Exception error);

    /**
     * Sets the failed state for this Resource and triggers a failure signal for
     * any pending ProduverRequest.
     *
     * @param cause
     *        The Exception that triggered the failure.
     */
    void failed(Exception cause);

}
