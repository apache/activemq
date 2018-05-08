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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.activemq.transport.amqp.AmqpSupport.CONTAINER_ID;
import static org.apache.activemq.transport.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.apache.activemq.transport.amqp.AmqpSupport.INVALID_FIELD;
import static org.apache.activemq.transport.amqp.AmqpSupport.PLATFORM;
import static org.apache.activemq.transport.amqp.AmqpSupport.PRODUCT;
import static org.apache.activemq.transport.amqp.AmqpSupport.QUEUE_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_QUEUE_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TEMP_TOPIC_CAPABILITY;
import static org.apache.activemq.transport.amqp.AmqpSupport.TOPIC_PREFIX;
import static org.apache.activemq.transport.amqp.AmqpSupport.VERSION;
import static org.apache.activemq.transport.amqp.AmqpSupport.contains;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.InvalidClientIDException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.AbstractRegion;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.activemq.transport.amqp.AmqpHeader;
import org.apache.activemq.transport.amqp.AmqpInactivityMonitor;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.transport.amqp.AmqpTransport;
import org.apache.activemq.transport.amqp.AmqpTransportFilter;
import org.apache.activemq.transport.amqp.AmqpWireFormat;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.activemq.transport.amqp.sasl.AmqpAuthenticator;
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
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
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
    private static final String BROKER_VERSION;
    private static final String BROKER_PLATFORM;

    static {
        String javaVersion = System.getProperty("java.version");

        BROKER_PLATFORM = "Java/" + (javaVersion == null ? "unknown" : javaVersion);

        InputStream in = null;
        String version = "<unknown-5.x>";
        if ((in = AmqpConnection.class.getResourceAsStream("/org/apache/activemq/version.txt")) != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            try {
                version = reader.readLine();
            } catch(Exception e) {
            }
        }
        BROKER_VERSION = version;
    }

    private final Transport protonTransport = Proton.transport();
    private final Connection protonConnection = Proton.connection();
    private final Collector eventCollector = new CollectorImpl();

    private final AmqpTransport amqpTransport;
    private final AmqpWireFormat amqpWireFormat;
    private final BrokerService brokerService;

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private final AtomicInteger lastCommandId = new AtomicInteger();
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final ConnectionInfo connectionInfo = new ConnectionInfo();
    private long nextSessionId;
    private long nextTempDestinationId;
    private long nextTransactionId;
    private boolean closing;
    private boolean closedSocket;
    private AmqpAuthenticator authenticator;

    private final Map<TransactionId, AmqpTransactionCoordinator> transactions = new HashMap<>();
    private final ConcurrentMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConsumerId, AmqpSender> subscriptionsByConsumerId = new ConcurrentHashMap<>();

    public AmqpConnection(AmqpTransport transport, BrokerService brokerService) {
        this.amqpTransport = transport;

        AmqpInactivityMonitor monitor = transport.getInactivityMonitor();
        if (monitor != null) {
            monitor.setAmqpTransport(amqpTransport);
        }

        this.amqpWireFormat = transport.getWireFormat();
        this.brokerService = brokerService;

        // the configured maxFrameSize on the URI.
        int maxFrameSize = amqpWireFormat.getMaxAmqpFrameSize();
        if (maxFrameSize > AmqpWireFormat.NO_AMQP_MAX_FRAME_SIZE) {
            this.protonTransport.setMaxFrameSize(maxFrameSize);
            try {
                this.protonTransport.setOutboundFrameSizeLimit(maxFrameSize);
            } catch (Throwable e) {
                // Ignore if older proton-j was injected.
            }
        }

        this.protonTransport.bind(this.protonConnection);
        this.protonTransport.setChannelMax(CHANNEL_MAX);
        this.protonTransport.setEmitFlowEventOnSend(false);

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
        return new Symbol[]{ ANONYMOUS_RELAY, DELAYED_DELIVERY };
    }

    /**
     * Load and return a <code>Map<Symbol, Object></code> that contains the properties
     * that this connection supplies to incoming connections.
     *
     * @return the properties that are offered to the incoming connection.
     */
    protected Map<Symbol, Object> getConnetionProperties() {
        Map<Symbol, Object> properties = new HashMap<>();

        properties.put(QUEUE_PREFIX, "queue://");
        properties.put(TOPIC_PREFIX, "topic://");
        properties.put(PRODUCT, "ActiveMQ");
        properties.put(VERSION, BROKER_VERSION);
        properties.put(PLATFORM, BROKER_PLATFORM);

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
        Map<Symbol, Object> properties = new HashMap<>();

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

    @Override
    public long keepAlive() throws IOException {
        long rescheduleAt = 0l;

        LOG.trace("Performing connection:{} keep-alive processing", amqpTransport.getRemoteAddress());

        if (protonConnection.getLocalState() != EndpointState.CLOSED) {
            // Using nano time since it is not related to the wall clock, which may change
            long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            long deadline = protonTransport.tick(now);
            pumpProtonToSocket();
            if (protonTransport.isClosed()) {
                LOG.debug("Transport closed after inactivity check.");
                throw new InactivityIOException("Channel was inactive for too long");
            } else {
                if(deadline != 0) {
                    // caller treats 0 as no-work, ensure value is at least 1 as there was a deadline
                    rescheduleAt = Math.max(deadline - now, 1);
                }
            }
        }

        LOG.trace("Connection:{} keep alive processing done, next update in {} milliseconds.",
                  amqpTransport.getRemoteAddress(), rescheduleAt);

        return rescheduleAt;
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

    /**
     * @return the configured max frame size allowed for incoming messages.
     */
    public long getMaxFrameSize() {
        return amqpWireFormat.getMaxFrameSize();
    }

    //----- Proton Event handling and IO support -----------------------------//

    void pumpProtonToSocket() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    LOG.trace("Server: Sending {} bytes out", toWrite.limit());
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

    @SuppressWarnings("deprecation")
    @Override
    public void onAMQPData(Object command) throws Exception {
        Buffer frame;
        if (command.getClass() == AmqpHeader.class) {
            AmqpHeader header = (AmqpHeader) command;

            if (amqpWireFormat.isHeaderValid(header, authenticator != null)) {
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
                    authenticator = null;
                    break; // nothing to do..
                case 3: // Client will be using SASL for auth..
                    authenticator = new AmqpAuthenticator(amqpTransport, protonTransport.sasl(), brokerService);
                    break;
                default:
            }
            frame = header.getBuffer();
        } else {
            frame = (Buffer) command;
        }

        if (protonTransport.isClosed()) {
            LOG.debug("Ignoring incoming AMQP data, transport is closed.");
            return;
        }

        LOG.trace("Server: Received from client: {} bytes", frame.getLength());

        while (frame.length > 0) {
            try {
                int count = protonTransport.input(frame.data, frame.offset, frame.length);
                frame.moveHead(count);
            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not decode AMQP frame: " + frame, true, e));
                return;
            }

            if (authenticator != null) {
                processSaslExchange();
            } else {
                processProtonEvents();
            }
        }
    }

    private void processSaslExchange() throws Exception {
        authenticator.processSaslExchange(connectionInfo);
        if (authenticator.isDone()) {
            amqpTransport.getWireFormat().resetMagicRead();
        }
        pumpProtonToSocket();
    }

    private void processProtonEvents() throws Exception {
        try {
            Event event = null;
            while ((event = eventCollector.peek()) != null) {
                if (amqpTransport.isTrace()) {
                    LOG.trace("Server: Processing event: {}", event.getType());
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

    protected void processConnectionOpen(Connection connection) throws Exception {

        stopConnectionTimeoutChecker();

        connectionInfo.setResponseRequired(true);
        connectionInfo.setConnectionId(connectionId);

        String clientId = protonConnection.getRemoteContainer();
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        }

        connectionInfo.setTransportContext(amqpTransport.getPeerCertificates());

        if (connection.getTransport().getRemoteIdleTimeout() > 0 && !amqpTransport.isUseInactivityMonitor()) {
            // We cannot meet the requested Idle processing because the inactivity monitor is
            // disabled so we won't send idle frames to match the request.
            protonConnection.setProperties(getFailedConnetionProperties());
            protonConnection.open();
            protonConnection.setCondition(new ErrorCondition(AmqpError.PRECONDITION_FAILED, "Cannot send idle frames"));
            protonConnection.close();
            pumpProtonToSocket();

            amqpTransport.onException(new IOException(
                "Connection failed, remote requested idle processing but inactivity monitoring is disbaled."));
            return;
        }

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
                            ErrorCondition condition = new ErrorCondition(AmqpError.INVALID_FIELD, exception.getMessage());

                            Map<Symbol, Object> infoMap = new HashMap<> ();
                            infoMap.put(INVALID_FIELD, CONTAINER_ID);
                            condition.setInfo(infoMap);

                            protonConnection.setCondition(condition);
                        } else {
                            protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, exception.getMessage()));
                        }

                        protonConnection.close();
                    } else {
                        if (amqpTransport.isUseInactivityMonitor() && amqpWireFormat.getIdleTimeout() > 0) {
                            LOG.trace("Connection requesting Idle timeout of: {} mills", amqpWireFormat.getIdleTimeout());
                            protonTransport.setIdleTimeout(amqpWireFormat.getIdleTimeout());
                        }

                        protonConnection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
                        protonConnection.setProperties(getConnetionProperties());
                        protonConnection.setContainer(brokerService.getBrokerName());
                        protonConnection.open();

                        configureInactivityMonitor();
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

            sendToActiveMQ(new ShutdownInfo());
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
            try {
                closing = true;
                // Attempt to inform the other end that we are going to close
                // so that the client doesn't wait around forever.
                protonConnection.setCondition(new ErrorCondition(AmqpError.DECODE_ERROR, error.getMessage()));
                protonConnection.close();
                pumpProtonToSocket();
            } catch (Exception ignore) {
            }
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
        } else if (command.isConsumerControl()) {
            ConsumerControl control = (ConsumerControl) command;
            AmqpSender sender = subscriptionsByConsumerId.get(control.getConsumerId());
            if (sender != null) {
                sender.onConsumerControl(control);
            }
        } else if (command.isBrokerInfo()) {
            // ignore
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command {}", command);
        }
    }

    //----- Utility methods for connection resources to use ------------------//

    void registerSender(ConsumerId consumerId, AmqpSender sender) {
        subscriptionsByConsumerId.put(consumerId, sender);
    }

    void unregisterSender(ConsumerId consumerId) {
        subscriptionsByConsumerId.remove(consumerId);
    }

    void registerTransaction(TransactionId txId, AmqpTransactionCoordinator coordinator) {
        transactions.put(txId, coordinator);
    }

    void unregisterTransaction(TransactionId txId) {
        transactions.remove(txId);
    }

    AmqpTransactionCoordinator getTxCoordinator(TransactionId txId) {
        return transactions.get(txId);
    }

    LocalTransactionId getNextTransactionId() {
        return new LocalTransactionId(getConnectionId(), ++nextTransactionId);
    }

    ConsumerInfo lookupSubscription(String subscriptionName) throws AmqpProtocolException {
        ConsumerInfo result = null;
        RegionBroker regionBroker;

        try {
            regionBroker = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        } catch (Exception e) {
            throw new AmqpProtocolException("Error finding subscription: " + subscriptionName + ": " + e.getMessage(), false, e);
        }

        final TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        DurableTopicSubscription subscription = topicRegion.lookupSubscription(subscriptionName, connectionInfo.getClientId());
        if (subscription != null) {
            result = subscription.getConsumerInfo();
        }

        return result;
    }


    Subscription lookupPrefetchSubscription(ConsumerInfo consumerInfo)  {
        Subscription subscription = null;
        try {
            subscription = ((AbstractRegion)((RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class)).getRegion(consumerInfo.getDestination())).getSubscriptions().get(consumerInfo.getConsumerId());
        } catch (Exception e) {
            LOG.warn("Error finding subscription for: " + consumerInfo + ": " + e.getMessage(), false, e);
        }
        return subscription;
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
        LOG.debug("Exception detail", exception);
        if (exception instanceof AmqpProtocolException) {
            onAMQPException((IOException) exception);
        } else {
            try {
                // Must ensure that the broker removes Connection resources.
                sendToActiveMQ(new ShutdownInfo());
                amqpTransport.stop();
            } catch (Throwable e) {
                LOG.error("Failed to stop AMQP Transport ", e);
            }
        }
    }

    //----- Internal implementation ------------------------------------------//

    private SessionId getNextSessionId() {
        return new SessionId(connectionId, nextSessionId++);
    }

    private void stopConnectionTimeoutChecker() {
        AmqpInactivityMonitor monitor = amqpTransport.getInactivityMonitor();
        if (monitor != null) {
            monitor.stopConnectionTimeoutChecker();
        }
    }

    private void configureInactivityMonitor() {
        AmqpInactivityMonitor monitor = amqpTransport.getInactivityMonitor();
        if (monitor == null) {
            return;
        }

        // If either end has idle timeout requirements then the tick method
        // will give us a deadline on the next time we need to tick() in order
        // to meet those obligations.
        // Using nano time since it is not related to the wall clock, which may change
        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long nextIdleCheck = protonTransport.tick(now);
        if (nextIdleCheck != 0) {
            // monitor treats <= 0 as no work, ensure value is at least 1 as there was a deadline
            long delay = Math.max(nextIdleCheck - now, 1);
            LOG.trace("Connection keep-alive processing starts in: {}", delay);
            monitor.startKeepAliveTask(delay);
        } else {
            LOG.trace("Connection does not require keep-alive processing");
        }
    }
}
