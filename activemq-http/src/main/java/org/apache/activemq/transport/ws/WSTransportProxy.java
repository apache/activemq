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
package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.ws.WSTransport.WSTransportSink;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A proxy class that manages sending WebSocket events to the wrapped protocol level
 * WebSocket Transport.
 */
public final class WSTransportProxy extends TransportSupport implements Transport, WebSocketListener, BrokerServiceAware, WSTransportSink {

    private static final Logger LOG = LoggerFactory.getLogger(WSTransportProxy.class);

    private final int ORDERLY_CLOSE_TIMEOUT = 10;

    private final ReentrantLock protocolLock = new ReentrantLock();
    private final CountDownLatch socketTransportStarted = new CountDownLatch(1);
    private final String remoteAddress;

    private final Transport transport;
    private final WSTransport wsTransport;
    private Session session;

    /**
     * Create a WebSocket Transport Proxy instance that will pass
     * along WebSocket event to the underlying protocol level transport.
     *
     * @param remoteAddress
     *      the provided remote address to report being connected to.
     * @param transport
     *      The protocol level WebSocket Transport
     */
    public WSTransportProxy(String remoteAddress, Transport transport) {
        this.remoteAddress = remoteAddress;
        this.transport = transport;
        this.wsTransport = transport.narrow(WSTransport.class);

        if (wsTransport == null) {
            throw new IllegalArgumentException("Provided Transport does not contains a WSTransport implementation");
        } else {
            wsTransport.setTransportSink(this);
        }
    }

    /**
     * @return the sub-protocol of the proxied transport.
     */
    public String getSubProtocol() {
        return wsTransport.getSubProtocol();
    }

    /**
     * Apply any configure Transport options on the wrapped Transport and its contained
     * wireFormat instance.
     */
    public void setTransportOptions(Map<String, Object> options) {
        Map<String, Object> wireFormatOptions = IntrospectionSupport.extractProperties(options, "wireFormat.");

        IntrospectionSupport.setProperties(transport, options);
        IntrospectionSupport.setProperties(transport.getWireFormat(), wireFormatOptions);
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        if (transport instanceof BrokerServiceAware) {
            ((BrokerServiceAware) transport).setBrokerService(brokerService);
        }
    }

    @Override
    public void oneway(Object command) throws IOException {
        protocolLock.lock();
        try {
            transport.oneway(command);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return transport.getPeerCertificates();
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        transport.setPeerCertificates(certificates);
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public WireFormat getWireFormat() {
        return transport.getWireFormat();
    }

    @Override
    public int getReceiveCounter() {
        return transport.getReceiveCounter();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        transport.stop();
        if (session != null && session.isOpen()) {
            session.close();
        }
    }

    @Override
    protected void doStart() throws Exception {
        transport.setTransportListener(getTransportListener());
        socketTransportStarted.countDown();

        transport.start();
    }

    //----- WebSocket methods being proxied to the WS Transport --------------//

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int length) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for WebSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for WebSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        protocolLock.lock();
        try {
            wsTransport.onWebSocketBinary(ByteBuffer.wrap(payload, offset, length));
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public void onWebSocketText(String data) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for WebSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for WebSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        protocolLock.lock();
        try {
            wsTransport.onWebSocketText(data);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        try {
            if (protocolLock.tryLock() || protocolLock.tryLock(ORDERLY_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.debug("WebSocket closed: code[{}] message[{}]", statusCode, reason);
                wsTransport.onWebSocketClosed();
            }
        } catch (Exception e) {
            LOG.debug("Failed to close WebSocket cleanly", e);
        } finally {
            if (protocolLock.isHeldByCurrentThread()) {
                protocolLock.unlock();
            }
        }
    }

    @Override
    public void onWebSocketConnect(Session session) {
        this.session = session;

        if (wsTransport.getMaxFrameSize() > 0) {
            this.session.getPolicy().setMaxBinaryMessageSize(wsTransport.getMaxFrameSize());
            this.session.getPolicy().setMaxTextMessageSize(wsTransport.getMaxFrameSize());
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        onException(IOExceptionSupport.create(cause));
    }

    @Override
    public void onSocketOutboundText(String data) throws IOException {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for WebSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for WebSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        LOG.trace("WS Proxy sending string of size {} out", data.length());
        try {
            session.getRemote().sendStringByFuture(data).get(getDefaultSendTimeOut(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void onSocketOutboundBinary(ByteBuffer data) throws IOException {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for WebSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for WebSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        LOG.trace("WS Proxy sending {} bytes out", data.remaining());
        int limit = data.limit();
        try {
            session.getRemote().sendBytesByFuture(data).get(getDefaultSendTimeOut(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }

        // Reset back to original limit and move position to match limit indicating
        // that we read everything, the websocket sender clears the passed buffer
        // which can make it look as if nothing was written.
        data.limit(limit);
        data.position(limit);
    }

    //----- Internal implementation ------------------------------------------//

    private boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    private static int getDefaultSendTimeOut() {
        return Integer.getInteger("org.apache.activemq.transport.ws.WSTransportProxy.sendTimeout", 30);
    }
}
