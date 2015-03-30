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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.activemq.transport.amqp.AmqpSupport.QUEUE_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_QUEUE_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_TOPIC_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TOPIC_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.contains;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.InvalidClientIDException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.security.AuthenticationBroker;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.transport.amqp.AmqpHeader;
import org.apache.activemq.transport.amqp.AmqpInactivityMonitor;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.transport.amqp.AmqpTransport;
import org.apache.activemq.transport.amqp.AmqpTransportFilter;
import org.apache.activemq.transport.amqp.AmqpWireFormat;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the mechanics of managing a single remote peer connection.
 */
public class AmqpConnection implements AmqpProtocolConverter {

    private static final Logger TRACE_FRAMES = AmqpTransportFilter.TRACE_FRAMES;
    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);
    private static final int CHANNEL_MAX = 32767;

    private final Transport protonTransport = Proton.transport();
    private final Connection protonConnection = Proton.connection();
    private final Collector eventCollector = new CollectorImpl();

    private final AmqpTransport amqpTransport;
    private final AmqpWireFormat amqpWireFormat;
    private final BrokerService brokerService;
    private AuthenticationBroker authenticator;
    private Sasl sasl;

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private final AtomicInteger lastCommandId = new AtomicInteger();
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final ConnectionInfo connectionInfo = new ConnectionInfo();
    private long nextSessionId = 0;
    private long nextTempDestinationId = 0;
    private boolean closing = false;
    private boolean closedSocket = false;

    private final ConcurrentMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final ConcurrentMap<ConsumerId, AmqpSender> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, AmqpSender>();

    public AmqpConnection(AmqpTransport transport, BrokerService brokerService) {
        this.amqpTransport = transport;
        AmqpInactivityMonitor monitor = transport.getInactivityMonitor();
        if (monitor != null) {
            monitor.setProtocolConverter(this);
        }
        this.amqpWireFormat = transport.getWireFormat();
        this.brokerService = brokerService;

        // the configured maxFrameSize on the URI.
        int maxFrameSize = amqpWireFormat.getMaxAmqpFrameSize();
        if (maxFrameSize > AmqpWireFormat.NO_AMQP_MAX_FRAME_SIZE) {
            this.protonTransport.setMaxFrameSize(maxFrameSize);
        }

        this.protonTransport.bind(this.protonConnection);
        this.protonTransport.setChannelMax(CHANNEL_MAX);

        this.protonConnection.collect(eventCollector);

        updateTracer();
    }

    /**
     * Load and return a <code>[]Symbol</code> that contains the connection capabilities
     * offered to new connections
     *
     * @return the capabilities that are offered to new clients on connect.
     */
    protected Symbol[] getConnectionCapabilitiesOffered() {
        return new Symbol[]{ ANONYMOUS_RELAY };
    }

    /**
     * Load and return a <code>Map<Symbol, Object></code> that contains the properties
     * that this connection supplies to incoming connections.
     *
     * @return the properties that are offered to the incoming connection.
     */
    protected Map<Symbol, Object> getConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(QUEUE_PREFIX, "queue://");
        properties.put(TOPIC_PREFIX, "topic://");

        return properties;
    }

    /**
     * Load and return a <code>Map<Symbol, Object></code> that contains the properties
     * that this connection supplies to incoming connections when the open has failed
     * and the remote should expect a close to follow.
     *
     * @return the properties that are offered to the incoming connection.
     */
    protected Map<Symbol, Object> getFailedConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();

        properties.put(CONNECTION_OPEN_FAILED, true);

        return properties;
    }

    @Override
    public void updateTracer() {
        if (amqpTransport.isTrace()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | RECV: {}", AmqpConnection.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | SENT: {}", AmqpConnection.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }
            });
        }
    }

    //----- Connection Properties Accessors ----------------------------------//

    /**
     * @return the amount of credit assigned to AMQP receiver links created from
     *         sender links on the remote peer.
     */
    public int getConfiguredReceiverCredit() {
        return amqpWireFormat.getProducerCredit();
    }

    /**
     * @return the transformer type that was configured for this AMQP transport.
     */
    public String getConfiguredTransformer() {
        return amqpWireFormat.getTransformer();
    }

    /**
     * @return the ActiveMQ ConnectionId that identifies this AMQP Connection.
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * @return the Client ID used to create the connection with ActiveMQ
     */
    public String getClientId() {
        return connectionInfo.getClientId();
    }

    //----- Proton Event handling and IO support -----------------------------//

    void pumpProtonToSocket() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    LOG.trace("Sending {} bytes out", toWrite.limit());
                    amqpTransport.sendToAmqp(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            amqpTransport.onException(e);
        }
    }

    @Override
    public void onAMQPData(Object command) throws Exception {
        Buffer frame;
        if (command.getClass() == AmqpHeader.class) {
            AmqpHeader header = (AmqpHeader) command;

            if (amqpWireFormat.isHeaderValid(header)) {
                LOG.trace("Connection from an AMQP v1.0 client initiated. {}", header);
            } else {
                LOG.warn("Connection attempt from non AMQP v1.0 client. {}", header);
                AmqpHeader reply = amqpWireFormat.getMinimallySupportedHeader();
                amqpTransport.sendToAmqp(reply.getBuffer());
                handleException(new AmqpProtocolException(
                    "Connection from client using unsupported AMQP attempted", true));
            }

            switch (header.getProtocolId()) {
                case 0:
                    break; // nothing to do..
                case 3: // Client will be using SASL for auth..
                    sasl = protonTransport.sasl();
                    sasl.setMechanisms(new String[] { "ANONYMOUS", "PLAIN" });
                    sasl.server();
                    break;
                default:
            }
            frame = header.getBuffer();
        } else {
            frame = (Buffer) command;
        }

        onFrame(frame);
    }

    public void onFrame(Buffer frame) throws Exception {
        while (frame.length > 0) {
            try {
                int count = protonTransport.input(frame.data, frame.offset, frame.length);
                frame.moveHead(count);
            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not decode AMQP frame: " + frame, true, e));
                return;
            }

            try {
                if (sasl != null) {
                    // Lets try to complete the sasl handshake.
                    if (sasl.getRemoteMechanisms().length > 0) {
                        if ("PLAIN".equals(sasl.getRemoteMechanisms()[0])) {
                            byte[] data = new byte[sasl.pending()];
                            sasl.recv(data, 0, data.length);
                            Buffer[] parts = new Buffer(data).split((byte) 0);
                            if (parts.length > 0) {
                                connectionInfo.setUserName(parts[0].utf8().toString());
                            }
                            if (parts.length > 1) {
                                connectionInfo.setPassword(parts[1].utf8().toString());
                            }

                            if (tryAuthenticate(connectionInfo, amqpTransport.getPeerCertificates())) {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            } else {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                            }

                            amqpTransport.getWireFormat().resetMagicRead();
                            sasl = null;
                            LOG.debug("SASL [PLAIN] Handshake complete.");
                        } else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0])) {
                            if (tryAuthenticate(connectionInfo, amqpTransport.getPeerCertificates())) {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            } else {
                                sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                            }
                            amqpTransport.getWireFormat().resetMagicRead();
                            sasl = null;
                            LOG.debug("SASL [ANONYMOUS] Handshake complete.");
                        }
                    }
                }

                Event event = null;
                while ((event = eventCollector.peek()) != null) {
                    if (amqpTransport.isTrace()) {
                        LOG.trace("Processing event: {}", event.getType());
                    }
                    switch (event.getType()) {
                        case CONNECTION_REMOTE_OPEN:
                            processConnectionOpen(event.getConnection());
                            break;
                        case CONNECTION_REMOTE_CLOSE:
                            processConnectionClose(event.getConnection());
                            break;
                        case SESSION_REMOTE_OPEN:
                            processSessionOpen(event.getSession());
                            break;
                        case SESSION_REMOTE_CLOSE:
                            processSessionClose(event.getSession());
                            break;
                        case LINK_REMOTE_OPEN:
                            processLinkOpen(event.getLink());
                            break;
                        case LINK_REMOTE_DETACH:
                            processLinkDetach(event.getLink());
                            break;
                        case LINK_REMOTE_CLOSE:
                            processLinkClose(event.getLink());
                            break;
                        case LINK_FLOW:
                            processLinkFlow(event.getLink());
                            break;
                        case DELIVERY:
                            processDelivery(event.getDelivery());
                            break;
                        default:
                            break;
                    }

                    eventCollector.pop();
                }

            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not process AMQP commands", true, e));
            }

            pumpProtonToSocket();
        }
    }

    protected void processConnectionOpen(Connection connection) throws Exception {

        connectionInfo.setResponseRequired(true);
        connectionInfo.setConnectionId(connectionId);

        configureInactivityMonitor();

        String clientId = protonConnection.getRemoteContainer();
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        }

        connectionInfo.setTransportContext(amqpTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            @Override
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                Throwable exception = null;
                try {
                    if (response.isException()) {
                        protonConnection.setProperties(getFailedConnetionProperties());
                        protonConnection.open();

                        exception = ((ExceptionResponse) response).getException();
                        if (exception instanceof SecurityException) {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                        } else if (exception instanceof InvalidClientIDException) {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.INVALID_FIELD, exception.getMessage()));
                        } else {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, exception.getMessage()));
                        }

                        protonConnection.close();
                    } else {
                        protonConnection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
                        protonConnection.setProperties(getConnetionProperties());
                        protonConnection.open();
                    }
                } finally {
                    pumpProtonToSocket();

                    if (response.isException()) {
                        amqpTransport.onException(IOExceptionSupport.create(exception));
                    }
                }
            }
        });
    }

    protected void processConnectionClose(Connection connection) throws Exception {
        if (!closing) {
            closing = true;
            sendToActiveMQ(new RemoveInfo(connectionId), new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    protonConnection.close();
                    protonConnection.free();

                    if (!closedSocket) {
                        pumpProtonToSocket();
                    }
                }
            });

            sendToActiveMQ(new ShutdownInfo(), null);
        }
    }

    protected void processSessionOpen(Session protonSession) throws Exception {
        new AmqpSession(this, getNextSessionId(), protonSession).open();
    }

    protected void processSessionClose(Session protonSession) throws Exception {
        if (protonSession.getContext() != null) {
            ((AmqpResource) protonSession.getContext()).close();
        } else {
            protonSession.close();
            protonSession.free();
        }
    }

    protected void processLinkOpen(Link link) throws Exception {
        link.setSource(link.getRemoteSource());
        link.setTarget(link.getRemoteTarget());

        AmqpSession session = (AmqpSession) link.getSession().getContext();
        if (link instanceof Receiver) {
            if (link.getRemoteTarget() instanceof Coordinator) {
                session.createCoordinator((Receiver) link);
            } else {
                session.createReceiver((Receiver) link);
            }
        } else {
            session.createSender((Sender) link);
        }
    }

    protected void processLinkDetach(Link link) throws Exception {
        Object context = link.getContext();

        if (context instanceof AmqpLink) {
            ((AmqpLink) context).detach();
        } else {
            link.detach();
            link.free();
        }
    }

    protected void processLinkClose(Link link) throws Exception {
        Object context = link.getContext();

        if (context instanceof AmqpLink) {
            ((AmqpLink) context).close();;
        } else {
            link.close();
            link.free();
        }
    }

    protected void processLinkFlow(Link link) throws Exception {
        Object context = link.getContext();
        if (context instanceof AmqpLink) {
            ((AmqpLink) context).flow();
        }
    }

    protected void processDelivery(Delivery delivery) throws Exception {
        if (!delivery.isPartial()) {
            Object context = delivery.getLink().getContext();
            if (context instanceof AmqpLink) {
                AmqpLink amqpLink = (AmqpLink) context;
                amqpLink.delivery(delivery);
            }
        }
    }

    //----- Event entry points for ActiveMQ commands and errors --------------//

    @Override
    public void onAMQPException(IOException error) {
        closedSocket = true;
        if (!closing) {
            amqpTransport.sendToActiveMQ(error);
        } else {
            try {
                amqpTransport.stop();
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public void onActiveMQCommand(Command command) throws Exception {
        if (command.isResponse()) {
            Response response = (Response) command;
            ResponseHandler rh = resposeHandlers.remove(Integer.valueOf(response.getCorrelationId()));
            if (rh != null) {
                rh.onResponse(this, response);
            } else {
                // Pass down any unexpected errors. Should this close the connection?
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    handleException(exception);
                }
            }
        } else if (command.isMessageDispatch()) {
            MessageDispatch dispatch = (MessageDispatch) command;
            AmqpSender sender = subscriptionsByConsumerId.get(dispatch.getConsumerId());
            if (sender != null) {
                // End of Queue Browse will have no Message object.
                if (dispatch.getMessage() != null) {
                    LOG.trace("Dispatching MessageId: {} to consumer", dispatch.getMessage().getMessageId());
                } else {
                    LOG.trace("Dispatching End of Browse Command to consumer {}", dispatch.getConsumerId());
                }
                sender.onMessageDispatch(dispatch);
                if (dispatch.getMessage() != null) {
                    LOG.trace("Finished Dispatch of MessageId: {} to consumer", dispatch.getMessage().getMessageId());
                }
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError) command).getException();
            handleException(exception);
        } else if (command.isBrokerInfo()) {
            // ignore
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command {}", command);
        }
    }

    //----- Utility methods for connection resources to use ------------------//

    void regosterSender(ConsumerId consumerId, AmqpSender sender) {
        subscriptionsByConsumerId.put(consumerId, sender);
    }

    void unregosterSender(ConsumerId consumerId) {
        subscriptionsByConsumerId.remove(consumerId);
    }

    ActiveMQDestination lookupSubscription(String subscriptionName) throws AmqpProtocolException {
        ActiveMQDestination result = null;
        RegionBroker regionBroker;

        try {
            regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        } catch (Exception e) {
            throw new AmqpProtocolException("Error finding subscription: " + subscriptionName + ": " + e.getMessage(), false, e);
        }

        final TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        DurableTopicSubscription subscription = topicRegion.lookupSubscription(subscriptionName, connectionInfo.getClientId());
        if (subscription != null) {
            result = subscription.getActiveMQDestination();
        }

        return result;
    }

    ActiveMQDestination createTemporaryDestination(final Link link, Symbol[] capabilities) {
        ActiveMQDestination rc = null;
        if (contains(capabilities, TEMP_TOPIC_CAPABILITY)) {
            rc = new ActiveMQTempTopic(connectionId, nextTempDestinationId++);
        } else if (contains(capabilities, TEMP_QUEUE_CAPABILITY)) {
            rc = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
        } else {
            LOG.debug("Dynamic link request with no type capability, defaults to Temporary Queue");
            rc = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
        }

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionId);
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(rc);

        sendToActiveMQ(info, new ResponseHandler() {

            @Override
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                if (response.isException()) {
                    link.setSource(null);

                    Throwable exception = ((ExceptionResponse) response).getException();
                    if (exception instanceof SecurityException) {
                        link.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                    } else {
                        link.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                    }

                    link.close();
                    link.free();
                }
            }
        });

        return rc;
    }

    void deleteTemporaryDestination(ActiveMQTempDestination destination) {
        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionId);
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);

        sendToActiveMQ(info, new ResponseHandler() {

            @Override
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    LOG.debug("Error during temp destination removeal: {}", exception.getMessage());
                }
            }
        });
    }

    void sendToActiveMQ(Command command) {
        sendToActiveMQ(command, null);
    }

    void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(lastCommandId.incrementAndGet());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(Integer.valueOf(command.getCommandId()), handler);
        }
        amqpTransport.sendToActiveMQ(command);
    }

    void handleException(Throwable exception) {
        exception.printStackTrace();
        LOG.debug("Exception detail", exception);
        try {
            amqpTransport.stop();
        } catch (Throwable e) {
            LOG.error("Failed to stop AMQP Transport ", e);
        }
    }

    //----- Internal implementation ------------------------------------------//

    private SessionId getNextSessionId() {
        return new SessionId(connectionId, nextSessionId++);
    }

    private void configureInactivityMonitor() {
        AmqpInactivityMonitor monitor = amqpTransport.getInactivityMonitor();
        if (monitor == null) {
            return;
        }

        monitor.stopConnectChecker();
    }

    private boolean tryAuthenticate(ConnectionInfo info, X509Certificate[] peerCertificates) {
        try {
            if (getAuthenticator().authenticate(info.getUserName(), info.getPassword(), peerCertificates) != null) {
                return true;
            }

            return false;
        } catch (Throwable error) {
            return false;
        }
    }

    private AuthenticationBroker getAuthenticator() {
        if (authenticator == null) {
            try {
                authenticator = (AuthenticationBroker) brokerService.getBroker().getAdaptor(AuthenticationBroker.class);
            } catch (Exception e) {
                LOG.debug("Failed to lookup AuthenticationBroker from Broker, will use a default Noop version.");
            }

            if (authenticator == null) {
                authenticator = new DefaultAuthenticationBroker();
            }
        }

        return authenticator;
    }

    private class DefaultAuthenticationBroker implements AuthenticationBroker {

        @Override
        public SecurityContext authenticate(String username, String password, X509Certificate[] peerCertificates) throws SecurityException {
            return new SecurityContext(username) {

                @Override
                public Set<Principal> getPrincipals() {
                    return null;
                }
            };
        }
    }
}
