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

import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.amqp.client.sasl.SaslAuthenticator;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.ClientTcpTransport;
import org.apache.activemq.transport.amqp.client.util.UnmodifiableConnection;
import org.apache.activemq.util.IdGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AmqpAbstractResource<Connection> implements ClientTcpTransport.TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;
    // NOTE: Limit default channel max to signed short range to deal with
    //       brokers that don't currently handle the unsigned range well.
    private static final int DEFAULT_CHANNEL_MAX = 32767;
    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    public static final long DEFAULT_CONNECT_TIMEOUT = 515000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 30000;

    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicLong sessionIdGenerator = new AtomicLong();
    private final Collector protonCollector = new CollectorImpl();
    private final ClientTcpTransport transport;
    private final Transport protonTransport = Transport.Factory.create();

    private final String username;
    private final String password;
    private final URI remoteURI;
    private final String connectionId;
    private List<Symbol> offeredCapabilities = Collections.emptyList();
    private Map<Symbol, Object> offeredProperties = Collections.emptyMap();

    private AmqpConnectionListener listener;
    private SaslAuthenticator authenticator;

    private int idleTimeout = 0;
    private boolean idleProcessingDisabled;
    private String containerId;
    private boolean authenticated;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;

    public AmqpConnection(ClientTcpTransport transport, String username, String password) {
        setEndpoint(Connection.Factory.create());
        getEndpoint().collect(protonCollector);

        this.transport = transport;
        this.username = username;
        this.password = password;
        this.connectionId = CONNECTION_ID_GENERATOR.generateId();
        this.remoteURI = transport.getRemoteURI();

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(toString());
                return serial;
            }
        });

        this.transport.setTransportListener(this);
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
                    authenticator = new SaslAuthenticator(sasl, username, password);
                    open(future);

                    pumpToProtonTransport();
                }
            });

            if (connectTimeout <= 0) {
                future.sync();
            } else {
                future.sync(connectTimeout, TimeUnit.MILLISECONDS);
                if (getEndpoint().getRemoteState() != EndpointState.ACTIVE) {
                    throw new IOException("Failed to connect after configured timeout.");
                }
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

                        pumpToProtonTransport();
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

                serializer.shutdown();
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
                pumpToProtonTransport();
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
                    transport.send(ByteBuffer.wrap(rawData));
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
        return new UnmodifiableConnection(getEndpoint());
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

    //----- Internal getters used from the child AmqpResource classes --------//

    ScheduledExecutorService getScheduler() {
        return this.serializer;
    }

    Connection getProtonConnection() {
        return getEndpoint();
    }

    void pumpToProtonTransport() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    transport.send(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireClientException(e);
        }
    }

    //----- Transport listener event hooks -----------------------------------//

    @Override
    public void onData(final Buffer input) {
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                ByteBuffer source = input.toByteBuffer();
                LOG.trace("Received from Broker {} bytes:", source.remaining());

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
    }

    @Override
    public void onTransportError(Throwable cause) {
        fireClientException(cause);
    }

    //----- Internal implementation ------------------------------------------//

    @Override
    protected void doOpenCompletion() {
        // If the remote indicates that a close is pending, don't open.
        if (!getEndpoint().getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {

            if (!isIdleProcessingDisabled()) {
                long nextKeepAliveTime = protonTransport.tick(System.currentTimeMillis());
                if (nextKeepAliveTime > 0) {

                    getScheduler().schedule(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                                    LOG.debug("Client performing next idle check");
                                    long rescheduleAt = protonTransport.tick(System.currentTimeMillis()) - System.currentTimeMillis();
                                    pumpToProtonTransport();
                                    if (protonTransport.isClosed()) {
                                        LOG.debug("Transport closed after inactivity check.");
                                        throw new InactivityIOException("Channel was inactive for to long");
                                    }

                                    if (rescheduleAt > 0) {
                                        getScheduler().schedule(this, rescheduleAt, TimeUnit.MILLISECONDS);
                                    }
                                }
                            } catch (Exception e) {
                                transport.close();
                                fireClientException(e);
                            }
                        }
                    }, nextKeepAliveTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                }
            }
            super.doOpenCompletion();
        }
    }

    @Override
    protected void doOpenInspection() {
        getStateInspector().inspectOpenedResource(getConnection());
    }

    @Override
    protected void doClosedInspection() {
        getStateInspector().inspectClosedResource(getConnection());
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
                    LOG.trace("New Proton Event: {}", protonEvent.getType());
                }

                AmqpResource amqpResource = null;
                switch (protonEvent.getType()) {
                    case CONNECTION_REMOTE_CLOSE:
                        amqpResource = (AmqpConnection) protonEvent.getConnection().getContext();
                        amqpResource.processRemoteClose(this);
                        break;
                    case CONNECTION_REMOTE_OPEN:
                        amqpResource = (AmqpConnection) protonEvent.getConnection().getContext();
                        amqpResource.processRemoteOpen(this);
                        break;
                    case SESSION_REMOTE_CLOSE:
                        amqpResource = (AmqpSession) protonEvent.getSession().getContext();
                        amqpResource.processRemoteClose(this);
                        break;
                    case SESSION_REMOTE_OPEN:
                        amqpResource = (AmqpSession) protonEvent.getSession().getContext();
                        amqpResource.processRemoteOpen(this);
                        break;
                    case LINK_REMOTE_CLOSE:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processRemoteClose(this);
                        break;
                    case LINK_REMOTE_DETACH:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processRemoteDetach(this);
                        break;
                    case LINK_REMOTE_OPEN:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processRemoteOpen(this);
                        break;
                    case LINK_FLOW:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processFlowUpdates(this);
                        break;
                    case DELIVERY:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processDeliveryUpdates(this);
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
