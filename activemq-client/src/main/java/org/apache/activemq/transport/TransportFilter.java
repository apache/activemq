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
import java.security.cert.X509Certificate;

import org.apache.activemq.wireformat.WireFormat;

/**
 *
 */
public class TransportFilter implements TransportListener, Transport {
    protected final Transport next;
    protected TransportListener transportListener;

    public TransportFilter(Transport next) {
        this.next = next;
    }

    @Override
    public TransportListener getTransportListener() {
        return transportListener;
    }

    @Override
    public void setTransportListener(TransportListener channelListener) {
        this.transportListener = channelListener;
        if (channelListener == null) {
            next.setTransportListener(null);
        } else {
            next.setTransportListener(this);
        }
    }

    /**
     * @see org.apache.activemq.Service#start()
     * @throws IOException
     *             if the next channel has not been set.
     */
    @Override
    public void start() throws Exception {
        if (next == null) {
            throw new IOException("The next channel has not been set.");
        }
        if (transportListener == null) {
            throw new IOException("The command listener has not been set.");
        }
        next.start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    @Override
    public void stop() throws Exception {
        next.stop();
    }

    @Override
    public void onCommand(Object command) {
        transportListener.onCommand(command);
    }

    /**
     * @return Returns the next.
     */
    public Transport getNext() {
        return next;
    }

    @Override
    public String toString() {
        return next.toString();
    }

    @Override
    public void oneway(Object command) throws IOException {
        next.oneway(command);
    }

    @Override
    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        return next.asyncRequest(command, null);
    }

    @Override
    public Object request(Object command) throws IOException {
        return next.request(command);
    }

    @Override
    public Object request(Object command, int timeout) throws IOException {
        return next.request(command, timeout);
    }

    @Override
    public void onException(IOException error) {
        transportListener.onException(error);
    }

    @Override
    public void transportInterupted() {
        transportListener.transportInterupted();
    }

    @Override
    public void transportResumed() {
        transportListener.transportResumed();
    }

    @Override
    public <T> T narrow(Class<T> target) {
        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        return next.narrow(target);
    }

    @Override
    public String getRemoteAddress() {
        return next.getRemoteAddress();
    }

    /**
     * @return
     * @see org.apache.activemq.transport.Transport#isFaultTolerant()
     */
    @Override
    public boolean isFaultTolerant() {
        return next.isFaultTolerant();
    }

    @Override
    public boolean isDisposed() {
        return next.isDisposed();
    }

    @Override
    public boolean isConnected() {
        return next.isConnected();
    }

    @Override
    public void reconnect(URI uri) throws IOException {
        next.reconnect(uri);
    }

    @Override
    public int getReceiveCounter() {
        return next.getReceiveCounter();
    }

    @Override
    public boolean isReconnectSupported() {
        return next.isReconnectSupported();
    }

    @Override
    public boolean isUpdateURIsSupported() {
        return next.isUpdateURIsSupported();
    }

    @Override
    public void updateURIs(boolean rebalance,URI[] uris) throws IOException {
        next.updateURIs(rebalance,uris);
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return next.getPeerCertificates();
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        next.setPeerCertificates(certificates);
    }

    @Override
    public WireFormat getWireFormat() {
        return next.getWireFormat();
    }
}
