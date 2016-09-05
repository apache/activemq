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
package org.apache.activemq.transport.mock;

import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;

import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.wireformat.WireFormat;

public class MockTransport extends DefaultTransportListener implements Transport {

    protected Transport next;
    protected TransportListener transportListener;

    public MockTransport(Transport next) {
        this.next = next;
    }

    @Override
    public synchronized void setTransportListener(TransportListener channelListener) {
        this.transportListener = channelListener;
        if (channelListener == null) {
            getNext().setTransportListener(null);
        } else {
            getNext().setTransportListener(this);
        }
    }

    /**
     * @see org.apache.activemq.Service#start()
     * @throws IOException
     *         if the next channel has not been set.
     */
    @Override
    public void start() throws Exception {
        if (getNext() == null) {
            throw new IOException("The next channel has not been set.");
        }
        if (transportListener == null) {
            throw new IOException("The command listener has not been set.");
        }
        getNext().start();
    }

    /**
     * @see org.apache.activemq.Service#stop()
     */
    @Override
    public void stop() throws Exception {
        getNext().stop();
    }

    @Override
    public void onCommand(Object command) {
        getTransportListener().onCommand(command);
    }

    /**
     * @return Returns the getNext().
     */
    public synchronized Transport getNext() {
        return next;
    }

    /**
     * @return Returns the packetListener.
     */
    @Override
    public synchronized TransportListener getTransportListener() {
        return transportListener;
    }

    @Override
    public String toString() {
        return getNext().toString();
    }

    @Override
    public void oneway(Object command) throws IOException {
        getNext().oneway(command);
    }

    @Override
    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        return getNext().asyncRequest(command, null);
    }

    @Override
    public Object request(Object command) throws IOException {
        return getNext().request(command);
    }

    @Override
    public Object request(Object command, int timeout) throws IOException {
        return getNext().request(command, timeout);
    }

    @Override
    public void onException(IOException error) {
        getTransportListener().onException(error);
    }

    @Override
    public <T> T narrow(Class<T> target) {
        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        return getNext().narrow(target);
    }

    public synchronized void setNext(Transport next) {
        this.next = next;
    }

    public void install(TransportFilter filter) {
        filter.setTransportListener(this);
        getNext().setTransportListener(filter);
        setNext(filter);
    }

    @Override
    public String getRemoteAddress() {
        return getNext().getRemoteAddress();
    }

    /**
     * @see org.apache.activemq.transport.Transport#isFaultTolerant()
     */
    @Override
    public boolean isFaultTolerant() {
        return getNext().isFaultTolerant();
    }

    @Override
    public boolean isDisposed() {
        return getNext().isDisposed();
    }

    @Override
    public boolean isConnected() {
        return getNext().isConnected();
    }

    @Override
    public void reconnect(URI uri) throws IOException {
        getNext().reconnect(uri);
    }

    @Override
    public int getReceiveCounter() {
        return getNext().getReceiveCounter();
    }

    @Override
    public boolean isReconnectSupported() {
        return getNext().isReconnectSupported();
    }

    @Override
    public boolean isUpdateURIsSupported() {
        return getNext().isUpdateURIsSupported();
    }

    @Override
    public void updateURIs(boolean reblance, URI[] uris) throws IOException {
        getNext().updateURIs(reblance, uris);
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return getNext().getPeerCertificates();
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        getNext().setPeerCertificates(certificates);
    }

    @Override
    public WireFormat getWireFormat() {
        return getNext().getWireFormat();
    }
}
