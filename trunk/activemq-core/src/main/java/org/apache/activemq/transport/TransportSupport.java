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
import java.net.URI;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A useful base class for transport implementations.
 * 
 * 
 */
public abstract class TransportSupport extends ServiceSupport implements Transport {
    private static final Logger LOG = LoggerFactory.getLogger(TransportSupport.class);

    TransportListener transportListener;

    /**
     * Returns the current transport listener
     */
    public TransportListener getTransportListener() {
        return transportListener;
    }

    /**
     * Registers an inbound command listener
     * 
     * @param commandListener
     */
    public void setTransportListener(TransportListener commandListener) {
        this.transportListener = commandListener;
    }

    /**
     * narrow acceptance
     * 
     * @param target
     * @return 'this' if assignable
     */
    public <T> T narrow(Class<T> target) {
        boolean assignableFrom = target.isAssignableFrom(getClass());
        if (assignableFrom) {
            return target.cast(this);
        }
        return null;
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command, int timeout) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    /**
     * Process the inbound command
     */
    public void doConsume(Object command) {
        if (command != null) {
            if (transportListener != null) {
                transportListener.onCommand(command);
            } else {
                LOG.error("No transportListener available to process inbound command: " + command);
            }
        }
    }

    /**
     * Passes any IO exceptions into the transport listener
     */
    public void onException(IOException e) {
        if (transportListener != null) {
            try {
                transportListener.onException(e);
            } catch (RuntimeException e2) {
                // Handle any unexpected runtime exceptions by debug logging
                // them.
                LOG.debug("Unexpected runtime exception: " + e2, e2);
            }
        }
    }

    protected void checkStarted() throws IOException {
        if (!isStarted()) {
            throw new IOException("The transport is not running.");
        }
    }

    public boolean isFaultTolerant() {
        return false;
    }

    public void reconnect(URI uri) throws IOException {
        throw new IOException("Not supported");
    }

    public boolean isReconnectSupported() {
        return false;
    }

    public boolean isUpdateURIsSupported() {
        return false;
    }
    public void updateURIs(boolean reblance,URI[] uris) throws IOException {
        throw new IOException("Not supported");
    }

    public boolean isDisposed() {
        return isStopped();
    }

    public boolean isConnected() {
        return isStarted();
    }

}
