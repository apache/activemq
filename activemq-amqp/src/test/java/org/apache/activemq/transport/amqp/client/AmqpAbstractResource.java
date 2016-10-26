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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base for all AmqpResource implementations to extend.
 *
 * This abstract class wraps up the basic state management bits so that the concrete
 * object don't have to reproduce it.  Provides hooks for the subclasses to initialize
 * and shutdown.
 */
public abstract class AmqpAbstractResource<E extends Endpoint> implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAbstractResource.class);

    protected AsyncResult openRequest;
    protected AsyncResult closeRequest;

    private AmqpValidator amqpStateInspector;

    private E endpoint;

    @Override
    public void open(AsyncResult request) {
        this.openRequest = request;
        doOpen();
        getEndpoint().setContext(this);
    }

    @Override
    public boolean isOpen() {
        return getEndpoint().getRemoteState() == EndpointState.ACTIVE;
    }

    @Override
    public void opened() {
        if (this.openRequest != null) {
            this.openRequest.onSuccess();
            this.openRequest = null;
        }
    }

    @Override
    public void detach(AsyncResult request) {
        // If already closed signal success or else the caller might never get notified.
        if (getEndpoint().getLocalState() == EndpointState.CLOSED ||
            getEndpoint().getRemoteState() == EndpointState.CLOSED) {

            if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                doDetach();
                getEndpoint().free();
            }

            request.onSuccess();
        } else {
            this.closeRequest = request;
            doDetach();
        }
    }

    @Override
    public void close(AsyncResult request) {
        // If already closed signal success or else the caller might never get notified.
        if (getEndpoint().getLocalState() == EndpointState.CLOSED ||
            getEndpoint().getRemoteState() == EndpointState.CLOSED) {

            if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                doClose();
                getEndpoint().free();
            }

            request.onSuccess();
        } else {
            this.closeRequest = request;
            doClose();
        }
    }

    @Override
    public boolean isClosed() {
        return getEndpoint().getLocalState() == EndpointState.CLOSED;
    }

    @Override
    public void closed() {
        getEndpoint().close();
        getEndpoint().free();

        if (this.closeRequest != null) {
            this.closeRequest.onSuccess();
            this.closeRequest = null;
        }
    }

    @Override
    public void failed() {
        failed(new Exception("Remote request failed."));
    }

    @Override
    public void failed(Exception cause) {
        if (openRequest != null) {
            if (endpoint != null) {
                // TODO: if this is a producer/consumer link then we may only be detached,
                // rather than fully closed, and should respond appropriately.
                endpoint.close();
            }
            openRequest.onFailure(cause);
            openRequest = null;
        }

        if (closeRequest != null) {
            closeRequest.onFailure(cause);
            closeRequest = null;
        }
    }

    @Override
    public void remotelyClosed(AmqpConnection connection) {
        Exception error = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());

        if (endpoint != null) {
            // TODO: if this is a producer/consumer link then we may only be detached,
            // rather than fully closed, and should respond appropriately.
            endpoint.close();
        }

        LOG.info("Resource {} was remotely closed", this);

        connection.fireClientException(error);
    }

    @Override
    public void locallyClosed(AmqpConnection connection, Exception error) {
        if (endpoint != null) {
            // TODO: if this is a producer/consumer link then we may only be detached,
            // rather than fully closed, and should respond appropriately.
            endpoint.close();
        }

        LOG.info("Resource {} was locally closed", this);

        connection.fireClientException(error);
    }

    public E getEndpoint() {
        return this.endpoint;
    }

    public void setEndpoint(E endpoint) {
        this.endpoint = endpoint;
    }

    public AmqpValidator getStateInspector() {
        return amqpStateInspector;
    }

    public void setStateInspector(AmqpValidator stateInspector) {
        if (stateInspector == null) {
            stateInspector = new AmqpValidator();
        }

        this.amqpStateInspector = stateInspector;
    }

    public EndpointState getLocalState() {
        if (getEndpoint() == null) {
            return EndpointState.UNINITIALIZED;
        }
        return getEndpoint().getLocalState();
    }

    public EndpointState getRemoteState() {
        if (getEndpoint() == null) {
            return EndpointState.UNINITIALIZED;
        }
        return getEndpoint().getRemoteState();
    }

    public boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition().getCondition() != null;
    }

    @Override
    public void processRemoteOpen(AmqpConnection connection) throws IOException {
        doOpenInspection();
        doOpenCompletion();
    }

    @Override
    public void processRemoteDetach(AmqpConnection connection) throws IOException {
        doDetachedInspection();
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            closed();
        } else {
            remotelyClosed(connection);
        }
    }

    @Override
    public void processRemoteClose(AmqpConnection connection) throws IOException {
        doClosedInspection();
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            closed();
        } else if (isAwaitingOpen()) {
            // Error on Open, create exception and signal failure.
            LOG.warn("Open of {} failed: ", this);
            Exception openError;
            if (hasRemoteError()) {
                openError = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());
            } else {
                openError = getOpenAbortException();
            }

            failed(openError);
        } else {
            remotelyClosed(connection);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpConnection connection) throws IOException {
    }

    @Override
    public void processFlowUpdates(AmqpConnection connection) throws IOException {
    }

    /**
     * Perform the open operation on the managed endpoint.  A subclass may
     * override this method to provide additional open actions or configuration
     * updates.
     */
    protected void doOpen() {
        getEndpoint().open();
    }

    /**
     * Perform the close operation on the managed endpoint.  A subclass may
     * override this method to provide additional close actions or alter the
     * standard close path such as endpoint detach etc.
     */
    protected void doClose() {
        getEndpoint().close();
    }

    /**
     * Perform the detach operation on the managed endpoint.
     *
     * By default this method throws an UnsupportedOperationException, a subclass
     * must implement this and do a detach if its resource supports that.
     */
    protected void doDetach() {
        throw new UnsupportedOperationException("Endpoint cannot be detached.");
    }

    /**
     * Complete the open operation on the managed endpoint. A subclass may
     * override this method to provide additional verification actions or configuration
     * updates.
     */
    protected void doOpenCompletion() {
        LOG.debug("{} is now open: ", this);
        opened();
    }

    /**
     * When aborting the open operation, and there isnt an error condition,
     * provided by the peer, the returned exception will be used instead.
     * A subclass may override this method to provide alternative behaviour.
     */
    protected Exception getOpenAbortException() {
        return new IOException("Open failed unexpectedly.");
    }

    protected abstract void doOpenInspection();
    protected abstract void doClosedInspection();

    protected void doDetachedInspection() {}

    //----- Private implementation utility methods ---------------------------//

    private boolean isAwaitingOpen() {
        return this.openRequest != null;
    }

    private boolean isAwaitingClose() {
        return this.closeRequest != null;
    }
}
