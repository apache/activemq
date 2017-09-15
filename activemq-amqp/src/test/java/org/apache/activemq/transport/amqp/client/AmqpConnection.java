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

import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.amqp.client.sasl.SaslAuthenticator;
import org.apache.activemq.transport.amqp.client.transport.NettyTransportListener;
import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.IdGenerator;
import org.apache.activemq.transport.amqp.client.util.NoOpAsyncResult;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableProxy;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

public class AmqpConnection extends AmqpAbstractResource<Connection> implements NettyTransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private static final NoOpAsyncResult NOOP_REQUEST = new NoOpAsyncResult();

    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;
    // NOTE: Limit default channel max to signed short range to deal with
    //       brokers that don't currently handle the unsigned range well.
    private static final int DEFAULT_CHANNEL_MAX = 32767;
    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    public static final long DEFAULT_CONNECT_TIMEOUT = 515000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 30000;
    public static final long DEFAULT_DRAIN_TIMEOUT = 60000;

    private ScheduledThreadPoolExecutor serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicLong sessionIdGenerator = new AtomicLong();
    private final AtomicLong txIdGenerator = new AtomicLong();
    private final Collector protonCollector = new CollectorImpl();
    private final org.apache.activemq.transport.amqp.client.transport.NettyTransport transport;
    private final Transport protonTransport = Transport.Factory.create();

    private final String username;
    private final String password;
    private final URI remoteURI;
    private final String connectionId;
    private List<Symbol> offeredCapabilities = Collections.emptyList();
    private Map<Symbol, Object> offeredProperties = Collections.emptyMap();

    private volatile AmqpFrameValidator sentFrameInspector;
    private volatile AmqpFrameValidator receivedFrameInspector;
    private AmqpConnectionListener listener;
    private SaslAuthenticator authenticator;
    private String mechanismRestriction;
    private String authzid;

    private int idleTimeout = 0;
    private boolean idleProcessingDisabled;
    private String containerId;
    private boolean authenticated;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;
    private long drainTimeout = DEFAULT_DRAIN_TIMEOUT;
    private boolean trace;

    public AmqpConnection(org.apache.activemq.transport.amqp.client.transport.NettyTransport transport, String username, String password) {
        setEndpoint(Connection.Factory.create());
        getEndpoint().collect(protonCollector);

        this.transport = transport;
        this.username = username;
        this.password = password;
        this.connectionId = CONNECTION_ID_GENERATOR.generateId();
        this.remoteURI = transport.getRemoteLocation();

        this.serializer = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(toString());
                return serial;
            }
        });

        // Ensure timely shutdown
        this.serializer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.serializer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        this.transport.setTransportListener(this);
        this.transport.setMaxFrameSize(getMaxFrameSize());
    }

    public void connect() throws Exception {
        if (connected.compareAndSet(false, true)) {
            transport.connect();

            final ClientFuture future = new ClientFuture();
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    getEndpoint().setContainer(safeGetContainerId());
                    getEndpoint().setHostname(remoteURI.getHost());
                    if (!getOfferedCapabilities().isEmpty()) {
                        getEndpoint().setOfferedCapabilities(getOfferedCapabilities().toArray(new Symbol[0]));
                    }
                    if (!getOfferedProperties().isEmpty()) {
                        getEndpoint().setProperties(getOfferedProperties());
                    }

                    if (getIdleTimeout() > 0) {
                        protonTransport.setIdleTimeout(getIdleTimeout());
                    }
                    protonTransport.setMaxFrameSize(getMaxFrameSize());
                    protonTransport.setChannelMax(getChannelMax());
                    protonTransport.bind(getEndpoint());
                    Sasl sasl = protonTransport.sasl();
                    if (sasl != null) {
                        sasl.client();
                    }
                    authenticator = new SaslAuthenticator(sasl, username, password, authzid, mechanismRestriction);
                    ((TransportImpl) protonTransport).setProtocolTracer(new AmqpProtocolTracer(AmqpConnection.this));
                    open(future);

                    pumpToProtonTransport(future);
                }
            });

            try {
                if (connectTimeout <= 0) {
                    future.sync();
                } else {
                    future.sync(connectTimeout, TimeUnit.MILLISECONDS);
                    if (getEndpoint().getRemoteState() != EndpointState.ACTIVE) {
                        throw new IOException("Failed to connect after configured timeout.");
                    }
                }
            } catch (Throwable error) {
                try {
                    close();
                } catch (Throwable ignore) {}

                throw error;
            }
        }
    }

    public boolean isConnected() {
        return transport.isConnected() && connected.get();
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ClientFuture request = new ClientFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        // If we are not connected then there is nothing we can do now
                        // just signal success.
                        if (!transport.isConnected()) {
                            request.onSuccess();
                        }

                        if (getEndpoint() != null) {
                            close(request);
                        } else {
                            request.onSuccess();
                        }

                        pumpToProtonTransport(request);
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection");
                    }
                }
            });

            try {
                if (closeTimeout <= 0) {
                    request.sync();
                } else {
                    request.sync(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            } finally {
                if (transport != null) {
                    try {
                        transport.close();
                    } catch (Exception e) {
                        LOG.debug("Cuaght exception while closing down Transport: {}", e.getMessage());
                    }
                }

                serializer.shutdownNow();
                try {
                    if (!serializer.awaitTermination(10, TimeUnit.SECONDS)) {
                        LOG.warn("Serializer didn't shutdown cleanly");
                    }
                } catch (InterruptedException e) {
                }
            }
        }
    }

    /**
     * Creates a new Session instance used to create AMQP resources like
     * senders and receivers.
     *
     * @return a new AmqpSession that can be used to create links.
     *
     * @throws Exception if an error occurs during creation.
     */
    public AmqpSession createSession() throws Exception {
        checkClosed();

        final AmqpSession session = new AmqpSession(AmqpConnection.this, getNextSessionId());
        final ClientFuture request = new ClientFuture();

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                session.setEndpoint(getEndpoint().session());
                session.setStateInspector(getStateInspector());
                session.open(request);
                pumpToProtonTransport(request);
            }
        });

        request.sync();

        return session;
    }

    //----- Access to low level IO for specific test cases -------------------//

    public void sendRawBytes(final byte[] rawData) throws Exception {
        checkClosed();

        final ClientFuture request = new ClientFuture();

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();
                try {
                    transport.send(Unpooled.wrappedBuffer(rawData));
                } catch (IOException e) {
                    fireClientException(e);
                } finally {
                    request.onSuccess();
                }
            }
        });

        request.sync();
    }

    //----- Configuration accessors ------------------------------------------//

    /**
     * @return the user name that was used to authenticate this connection.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password that was used to authenticate this connection.
     */
    public String getPassword() {
        return password;
    }

    public void setAuthzid(String authzid) {
        this.authzid = authzid;
    }

    public String getAuthzid() {
        return authzid;
    }

    /**
     * @return the URI of the remote peer this connection attached to.
     */
    public URI getRemoteURI() {
        return remoteURI;
    }

    /**
     * @return the container ID that will be set as the container Id.
     */
    public String getContainerId() {
        return this.containerId;
    }

    /**
     * Sets the container Id that will be configured on the connection prior to
     * connecting to the remote peer.  Calling this after connect has no effect.
     *
     * @param containerId
     * 		  the container Id to use on the connection.
     */
    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    /**
     * @return the currently set Max Frame Size value.
     */
    public int getMaxFrameSize() {
        return DEFAULT_MAX_FRAME_SIZE;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public long getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public long getDrainTimeout() {
        return drainTimeout;
    }

    public void setDrainTimeout(long drainTimeout) {
        this.drainTimeout = drainTimeout;
    }

    public List<Symbol> getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public void setOfferedCapabilities(List<Symbol> offeredCapabilities) {
        if (offeredCapabilities != null) {
            offeredCapabilities = Collections.emptyList();
        }

        this.offeredCapabilities = offeredCapabilities;
    }

    public Map<Symbol, Object> getOfferedProperties() {
        return offeredProperties;
    }

    public void setOfferedProperties(Map<Symbol, Object> offeredProperties) {
        if (offeredProperties != null) {
            offeredProperties = Collections.emptyMap();
        }

        this.offeredProperties = offeredProperties;
    }

    public Connection getConnection() {
        return UnmodifiableProxy.connectionProxy(getEndpoint());
    }

    public AmqpConnectionListener getListener() {
        return listener;
    }

    public void setListener(AmqpConnectionListener listener) {
        this.listener = listener;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void setIdleProcessingDisabled(boolean value) {
        this.idleProcessingDisabled = value;
    }

    public boolean isIdleProcessingDisabled() {
        return idleProcessingDisabled;
    }

    /**
     * Sets a restriction on the SASL mechanism to use (if offered by the server).
     *
     * @param mechanismRestriction the mechanism to use
     */
    public void setMechanismRestriction(String mechanismRestriction) {
        this.mechanismRestriction = mechanismRestriction;
    }

    public String getMechanismRestriction() {
        return mechanismRestriction;
    }

    public boolean isTraceFrames() {
        return trace;
    }

    public void setTraceFrames(boolean trace) {
        this.trace = trace;
    }

    public AmqpFrameValidator getSentFrameInspector() {
        return sentFrameInspector;
    }

    public void setSentFrameInspector(AmqpFrameValidator amqpFrameInspector) {
        this.sentFrameInspector = amqpFrameInspector;
    }

    public AmqpFrameValidator getReceivedFrameInspector() {
        return receivedFrameInspector;
    }

    public void setReceivedFrameInspector(AmqpFrameValidator amqpFrameInspector) {
        this.receivedFrameInspector = amqpFrameInspector;
    }

    //----- Internal getters used from the child AmqpResource classes --------//

    ScheduledExecutorService getScheduler() {
        return this.serializer;
    }

    Connection getProtonConnection() {
        return getEndpoint();
    }

    String getConnectionId() {
        return this.connectionId;
    }

    AmqpTransactionId getNextTransactionId() {
        return new AmqpTransactionId(connectionId + ":" + txIdGenerator.incrementAndGet());
    }

    void pumpToProtonTransport() {
        pumpToProtonTransport(NOOP_REQUEST);
    }

    void pumpToProtonTransport(AsyncResult request) {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    ByteBuf outbound = transport.allocateSendBuffer(toWrite.remaining());
                    outbound.writeBytes(toWrite);
                    transport.send(outbound);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireClientException(e);
            request.onFailure(e);
        }
    }

    //----- Transport listener event hooks -----------------------------------//

    @Override
    public void onData(final ByteBuf incoming) {

        // We need to retain until the serializer gets around to processing it.
        ReferenceCountUtil.retain(incoming);

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                ByteBuffer source = incoming.nioBuffer();
                LOG.trace("Client Received from Broker {} bytes:", source.remaining());

                if (protonTransport.isClosed()) {
                    LOG.debug("Ignoring incoming data because transport is closed");
                    return;
                }

                do {
                    ByteBuffer buffer = protonTransport.getInputBuffer();
                    int limit = Math.min(buffer.remaining(), source.remaining());
                    ByteBuffer duplicate = source.duplicate();
                    duplicate.limit(source.position() + limit);
                    buffer.put(duplicate);
                    protonTransport.processInput();
                    source.position(source.position() + limit);
                } while (source.hasRemaining());

                ReferenceCountUtil.release(incoming);

                // Process the state changes from the latest data and then answer back
                // any pending updates to the Broker.
                processUpdates();
                pumpToProtonTransport();
            }
        });
    }

    @Override
    public void onTransportClosed() {
        LOG.debug("The transport has unexpectedly closed");
        failed(getOpenAbortException());
    }

    @Override
    public void onTransportError(Throwable cause) {
        fireClientException(cause);
    }

    //----- Internal implementation ------------------------------------------//

    @Override
    protected void doOpenCompletion() {
        // If the remote indicates that a close is pending, don't open.
        if (getEndpoint().getRemoteProperties() == null ||
            !getEndpoint().getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {

            if (!isIdleProcessingDisabled()) {
                // Using nano time since it is not related to the wall clock, which may change
                long initialNow = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                long initialKeepAliveDeadline = protonTransport.tick(initialNow);
                if (initialKeepAliveDeadline != 0) {

                    getScheduler().schedule(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                                    LOG.debug("Client performing next idle check");
                                    // Using nano time since it is not related to the wall clock, which may change
                                    long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                                    long deadline = protonTransport.tick(now);

                                    pumpToProtonTransport();
                                    if (protonTransport.isClosed()) {
                                        LOG.debug("Transport closed after inactivity check.");
                                        throw new InactivityIOException("Channel was inactive for too long");
                                    } else {
                                        if(deadline != 0) {
                                            getScheduler().schedule(this, deadline - now, TimeUnit.MILLISECONDS);
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                try {
                                    transport.close();
                                } catch (IOException e1) {
                                }
                                fireClientException(e);
                            }
                        }
                    }, initialKeepAliveDeadline - initialNow, TimeUnit.MILLISECONDS);
                }
            }
            super.doOpenCompletion();
        }
    }

    @Override
    protected void doOpenInspection() {
        try {
            getStateInspector().inspectOpenedResource(getConnection());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    @Override
    protected void doClosedInspection() {
        try {
            getStateInspector().inspectClosedResource(getConnection());
        } catch (Throwable error) {
            getStateInspector().markAsInvalid(error.getMessage());
        }
    }

    protected void fireClientException(Throwable ex) {
        AmqpConnectionListener listener = this.listener;
        if (listener != null) {
            listener.onException(ex);
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The Connection is already closed");
        }
    }

    private void processUpdates() {
        try {
            Event protonEvent = null;
            while ((protonEvent = protonCollector.peek()) != null) {
                if (!protonEvent.getType().equals(Type.TRANSPORT)) {
                    LOG.trace("Client: New Proton Event: {}", protonEvent.getType());
                }

                AmqpEventSink amqpEventSink = null;
                switch (protonEvent.getType()) {
                    case CONNECTION_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getConnection().getContext();
                        amqpEventSink.processRemoteClose(this);
                        break;
                    case CONNECTION_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getConnection().getContext();
                        amqpEventSink.processRemoteOpen(this);
                        break;
                    case SESSION_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getSession().getContext();
                        amqpEventSink.processRemoteClose(this);
                        break;
                    case SESSION_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getSession().getContext();
                        amqpEventSink.processRemoteOpen(this);
                        break;
                    case LINK_REMOTE_CLOSE:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        amqpEventSink.processRemoteClose(this);
                        break;
                    case LINK_REMOTE_DETACH:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        amqpEventSink.processRemoteDetach(this);
                        break;
                    case LINK_REMOTE_OPEN:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        amqpEventSink.processRemoteOpen(this);
                        break;
                    case LINK_FLOW:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        amqpEventSink.processFlowUpdates(this);
                        break;
                    case DELIVERY:
                        amqpEventSink = (AmqpEventSink) protonEvent.getLink().getContext();
                        amqpEventSink.processDeliveryUpdates(this);
                        break;
                    default:
                        break;
                }

                protonCollector.pop();
            }

            // We have to do this to pump SASL bytes in as SASL is not event driven yet.
            if (!authenticated) {
                processSaslAuthentication();
            }
        } catch (Exception ex) {
            LOG.warn("Caught Exception during update processing: {}", ex.getMessage(), ex);
            fireClientException(ex);
        }
    }

    private void processSaslAuthentication() {
        if (authenticated || authenticator == null) {
            return;
        }

        try {
            if (authenticator.authenticate()) {
                authenticator = null;
                authenticated = true;
            }
        } catch (SecurityException ex) {
            failed(ex);
        }
    }

    private String getNextSessionId() {
        return connectionId + ":" + sessionIdGenerator.incrementAndGet();
    }

    private String safeGetContainerId() {
        String containerId = getContainerId();
        if (containerId == null || containerId.isEmpty()) {
            containerId = UUID.randomUUID().toString();
        }

        return containerId;
    }

    @Override
    public String toString() {
        return "AmqpConnection { " + connectionId + " }";
    }
}
