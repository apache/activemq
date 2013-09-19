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
package org.apache.activemq.transport.amqp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.EngineFactory;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.EngineFactoryImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.apache.qpid.proton.jms.AMQPNativeInboundTransformer;
import org.apache.qpid.proton.jms.AMQPRawInboundTransformer;
import org.apache.qpid.proton.jms.AutoOutboundTransformer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.jms.InboundTransformer;
import org.apache.qpid.proton.jms.JMSMappingInboundTransformer;
import org.apache.qpid.proton.jms.OutboundTransformer;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmqpProtocolConverter implements IAmqpProtocolConverter {

    static final Logger TRACE_FRAMES = AmqpTransportFilter.TRACE_FRAMES;
    public static final EnumSet<EndpointState> UNINITIALIZED_SET = EnumSet.of(EndpointState.UNINITIALIZED);
    public static final EnumSet<EndpointState> INITIALIZED_SET = EnumSet.complementOf(UNINITIALIZED_SET);
    public static final EnumSet<EndpointState> ACTIVE_STATE = EnumSet.of(EndpointState.ACTIVE);
    public static final EnumSet<EndpointState> CLOSED_STATE = EnumSet.of(EndpointState.CLOSED);
    public static final EnumSet<EndpointState> ALL_STATES = EnumSet.of(EndpointState.CLOSED, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);
    private static final Logger LOG = LoggerFactory.getLogger(AmqpProtocolConverter.class);
    static final public byte[] EMPTY_BYTE_ARRAY = new byte[] {};
    private final AmqpTransport amqpTransport;
    private static final Symbol COPY = Symbol.getSymbol("copy");
    private static final Symbol JMS_SELECTOR = Symbol.valueOf("jms-selector");
    private static final Symbol NO_LOCAL = Symbol.valueOf("no-local");
    private static final Symbol DURABLE_SUBSCRIPTION_ENDED = Symbol.getSymbol("DURABLE_SUBSCRIPTION_ENDED");

    int prefetch = 100;

    EngineFactory engineFactory = new EngineFactoryImpl();
    Transport protonTransport = engineFactory.createTransport();
    Connection protonConnection = engineFactory.createConnection();

    public AmqpProtocolConverter(AmqpTransport transport) {
        this.amqpTransport = transport;
        this.protonTransport.bind(this.protonConnection);
        updateTracer();
    }

    public void updateTracer() {
        if (amqpTransport.isTrace()) {
            ((TransportImpl) protonTransport).setProtocolTracer(new ProtocolTracer() {
                @Override
                public void receivedFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | RECV: {}", AmqpProtocolConverter.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }

                @Override
                public void sentFrame(TransportFrame transportFrame) {
                    TRACE_FRAMES.trace("{} | SENT: {}", AmqpProtocolConverter.this.amqpTransport.getRemoteAddress(), transportFrame.getBody());
                }
            });
        }
    }

    void pumpProtonToSocket() {
        try {
            int size = 1024 * 64;
            byte data[] = new byte[size];
            boolean done = false;
            while (!done) {
                int count = protonTransport.output(data, 0, size);
                if (count > 0) {
                    final Buffer buffer;
                    buffer = new Buffer(data, 0, count);
                    // System.out.println("writing: " + buffer.toString().substring(5).replaceAll("(..)", "$1 "));
                    amqpTransport.sendToAmqp(buffer);
                } else {
                    done = true;
                }
            }
            // System.out.println("write done");
        } catch (IOException e) {
            amqpTransport.onException(e);
        }
    }

    static class AmqpSessionContext {
        private final SessionId sessionId;
        long nextProducerId = 0;
        long nextConsumerId = 0;

        final Map<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

        public AmqpSessionContext(ConnectionId connectionId, long id) {
            sessionId = new SessionId(connectionId, id);
        }
    }

    Sasl sasl;

    /**
     * Convert a AMQP command
     */
    @Override
    public void onAMQPData(Object command) throws Exception {
        Buffer frame;
        if (command.getClass() == AmqpHeader.class) {
            AmqpHeader header = (AmqpHeader) command;
            switch (header.getProtocolId()) {
                case 0:
                    // amqpTransport.sendToAmqp(new AmqpHeader());
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
        // System.out.println("read: " + frame.toString().substring(5).replaceAll("(..)", "$1 "));
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
                            // We can't really auth at this point since we don't know the client id yet.. :(
                            sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            amqpTransport.getWireFormat().magicRead = false;
                            sasl = null;
                        } else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0])) {
                            sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                            amqpTransport.getWireFormat().magicRead = false;
                            sasl = null;
                        }
                    }
                }

                // Handle the amqp open..
                if (protonConnection.getLocalState() == EndpointState.UNINITIALIZED && protonConnection.getRemoteState() != EndpointState.UNINITIALIZED) {
                    onConnectionOpen();
                }

                // Lets map amqp sessions to openwire sessions..
                Session session = protonConnection.sessionHead(UNINITIALIZED_SET, INITIALIZED_SET);
                while (session != null) {
                    onSessionOpen(session);
                    session = protonConnection.sessionHead(UNINITIALIZED_SET, INITIALIZED_SET);
                }

                Link link = protonConnection.linkHead(UNINITIALIZED_SET, INITIALIZED_SET);
                while (link != null) {
                    onLinkOpen(link);
                    link = protonConnection.linkHead(UNINITIALIZED_SET, INITIALIZED_SET);
                }

                Delivery delivery = protonConnection.getWorkHead();
                while (delivery != null) {
                    AmqpDeliveryListener listener = (AmqpDeliveryListener) delivery.getLink().getContext();
                    if (listener != null) {
                        listener.onDelivery(delivery);
                    }
                    delivery = delivery.getWorkNext();
                }

                link = protonConnection.linkHead(ACTIVE_STATE, CLOSED_STATE);
                while (link != null) {
                    ((AmqpDeliveryListener) link.getContext()).onClose();
                    link.close();
                    link = link.next(ACTIVE_STATE, CLOSED_STATE);
                }

                link = protonConnection.linkHead(ACTIVE_STATE, ALL_STATES);
                while (link != null) {
                    ((AmqpDeliveryListener) link.getContext()).drainCheck();
                    link = link.next(ACTIVE_STATE, ALL_STATES);
                }

                session = protonConnection.sessionHead(ACTIVE_STATE, CLOSED_STATE);
                while (session != null) {
                    // TODO - close links?
                    onSessionClose(session);
                    session = session.next(ACTIVE_STATE, CLOSED_STATE);
                }
                if (protonConnection.getLocalState() == EndpointState.ACTIVE && protonConnection.getRemoteState() == EndpointState.CLOSED) {
                    doClose();
                }

            } catch (Throwable e) {
                handleException(new AmqpProtocolException("Could not process AMQP commands", true, e));
            }

            pumpProtonToSocket();
        }
    }

    boolean closing = false;
    boolean closedSocket = false;

    private void doClose() {
        if (!closing) {
            closing = true;
            sendToActiveMQ(new RemoveInfo(connectionId), new ResponseHandler() {
                @Override
                public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                    protonConnection.close();
                    if (!closedSocket) {
                        pumpProtonToSocket();
                    }
                }
            });
        }
    }

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
            MessageDispatch md = (MessageDispatch) command;
            ConsumerContext consumerContext = subscriptionsByConsumerId.get(md.getConsumerId());
            if (consumerContext != null) {
                // End of Queue Browse will have no Message object.
                if (md.getMessage() != null) {
                    LOG.trace("Dispatching MessageId: {} to consumer", md.getMessage().getMessageId());
                } else {
                    LOG.trace("Dispatching End of Browse Command to consumer {}", md.getConsumerId());
                }
                consumerContext.onMessageDispatch(md);
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

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final ConnectionInfo connectionInfo = new ConnectionInfo();
    private long nextSessionId = 0;
    private long nextTempDestinationId = 0;

    static abstract class AmqpDeliveryListener {

        abstract public void onDelivery(Delivery delivery) throws Exception;

        public void onClose() throws Exception {}

        public void drainCheck() {}

        abstract void doCommit() throws Exception;

        abstract void doRollback() throws Exception;
    }

    private void onConnectionOpen() throws AmqpProtocolException {

        connectionInfo.setResponseRequired(true);
        connectionInfo.setConnectionId(connectionId);
        // configureInactivityMonitor(connect.keepAlive());

        String clientId = protonConnection.getRemoteContainer();
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        }

        connectionInfo.setTransportContext(amqpTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            @Override
            public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                protonConnection.open();
                pumpProtonToSocket();

                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    protonConnection.setCondition(new ErrorCondition(AmqpError.UNAUTHORIZED_ACCESS, exception.getMessage()));
                    protonConnection.close();
                    pumpProtonToSocket();
                    amqpTransport.onException(IOExceptionSupport.create(exception));
                    return;
                }

            }
        });
    }

    private void onSessionOpen(Session session) {
        AmqpSessionContext sessionContext = new AmqpSessionContext(connectionId, nextSessionId++);
        session.setContext(sessionContext);
        sendToActiveMQ(new SessionInfo(sessionContext.sessionId), null);
        session.open();
    }

    private void onSessionClose(Session session) {
        AmqpSessionContext sessionContext = (AmqpSessionContext) session.getContext();
        if (sessionContext != null) {
            LOG.trace("Session {} closed", sessionContext.sessionId);
            sendToActiveMQ(new RemoveInfo(sessionContext.sessionId), null);
            session.setContext(null);
        }
        session.close();
    }

    private void onLinkOpen(Link link) {
        link.setSource(link.getRemoteSource());
        link.setTarget(link.getRemoteTarget());

        AmqpSessionContext sessionContext = (AmqpSessionContext) link.getSession().getContext();
        if (link instanceof Receiver) {
            onReceiverOpen((Receiver) link, sessionContext);
        } else {
            onSenderOpen((Sender) link, sessionContext);
        }
    }

    InboundTransformer inboundTransformer;

    protected InboundTransformer getInboundTransformer() {
        if (inboundTransformer == null) {
            String transformer = amqpTransport.getTransformer();
            if (transformer.equals(InboundTransformer.TRANSFORMER_JMS)) {
                inboundTransformer = new JMSMappingInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_NATIVE)) {
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_RAW)) {
                inboundTransformer = new AMQPRawInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else {
                LOG.warn("Unknown transformer type {} using native one instead", transformer);
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            }
        }
        return inboundTransformer;
    }

    abstract class BaseProducerContext extends AmqpDeliveryListener {

        ByteArrayOutputStream current = new ByteArrayOutputStream();

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            Receiver receiver = ((Receiver) delivery.getLink());
            if (!delivery.isReadable()) {
                LOG.debug("Delivery was not readable!");
                return;
            }

            if (current == null) {
                current = new ByteArrayOutputStream();
            }

            int count;
            byte data[] = new byte[1024 * 4];
            while ((count = receiver.recv(data, 0, data.length)) > 0) {
                current.write(data, 0, count);
            }

            // Expecting more deliveries..
            if (count == 0) {
                return;
            }

            receiver.advance();
            Buffer buffer = current.toBuffer();
            current = null;
            onMessage(receiver, delivery, buffer);
        }

        @Override
        void doCommit() throws Exception {}

        @Override
        void doRollback() throws Exception {}

        abstract protected void onMessage(Receiver receiver, Delivery delivery, Buffer buffer) throws Exception;
    }

    class ProducerContext extends BaseProducerContext {
        private final ProducerId producerId;
        private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
        private final ActiveMQDestination destination;

        public ProducerContext(ProducerId producerId, ActiveMQDestination destination) {
            this.producerId = producerId;
            this.destination = destination;
        }

        @Override
        protected void onMessage(final Receiver receiver, final Delivery delivery, Buffer buffer) throws Exception {
            EncodedMessage em = new EncodedMessage(delivery.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
            final ActiveMQMessage message = (ActiveMQMessage) getInboundTransformer().transform(em);
            current = null;

            if (destination != null) {
                message.setJMSDestination(destination);
            }
            message.setProducerId(producerId);

            MessageId messageId = message.getMessageId();
            if (messageId == null) {
                messageId = new MessageId();
                message.setMessageId(messageId);
            }

            messageId.setProducerId(producerId);
            messageId.setProducerSequenceId(messageIdGenerator.getNextSequenceId());

            LOG.trace("Inbound Message:{} from Producer:{}", message.getMessageId(), producerId + ":" + messageId.getProducerSequenceId());

            DeliveryState remoteState = delivery.getRemoteState();
            if (remoteState != null && remoteState instanceof TransactionalState) {
                TransactionalState s = (TransactionalState) remoteState;
                long txid = toLong(s.getTxnId());
                message.setTransactionId(new LocalTransactionId(connectionId, txid));
            }

            message.onSend();
            sendToActiveMQ(message, new ResponseHandler() {
                @Override
                public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                    if (!delivery.remotelySettled()) {
                        if (response.isException()) {
                            ExceptionResponse er = (ExceptionResponse) response;
                            Rejected rejected = new Rejected();
                            ErrorCondition condition = new ErrorCondition();
                            condition.setCondition(Symbol.valueOf("failed"));
                            condition.setDescription(er.getException().getMessage());
                            rejected.setError(condition);
                            delivery.disposition(rejected);
                        }
                    }
                    receiver.flow(1);
                    delivery.settle();
                    pumpProtonToSocket();
                }
            });
        }
    }

    long nextTransactionId = 1;

    class Transaction {
    }

    HashMap<Long, Transaction> transactions = new HashMap<Long, Transaction>();

    public byte[] toBytes(long value) {
        Buffer buffer = new Buffer(8);
        buffer.bigEndianEditor().writeLong(value);
        return buffer.data;
    }

    private long toLong(Binary value) {
        Buffer buffer = new Buffer(value.getArray(), value.getArrayOffset(), value.getLength());
        return buffer.bigEndianEditor().readLong();
    }

    AmqpDeliveryListener coordinatorContext = new BaseProducerContext() {

        @Override
        protected void onMessage(Receiver receiver, final Delivery delivery, Buffer buffer) throws Exception {

            MessageImpl msg = new MessageImpl();
            int offset = buffer.offset;
            int len = buffer.length;
            while (len > 0) {
                final int decoded = msg.decode(buffer.data, offset, len);
                assert decoded > 0 : "Make progress decoding the message";
                offset += decoded;
                len -= decoded;
            }

            final Object action = ((AmqpValue) msg.getBody()).getValue();
            LOG.debug("COORDINATOR received: {}, [{}]", action, buffer);
            if (action instanceof Declare) {
                Declare declare = (Declare) action;
                if (declare.getGlobalId() != null) {
                    throw new Exception("don't know how to handle a declare /w a set GlobalId");
                }

                long txid = nextTransactionId++;
                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), TransactionInfo.BEGIN);
                sendToActiveMQ(txinfo, null);
                LOG.trace("started transaction {}", txid);

                Declared declared = new Declared();
                declared.setTxnId(new Binary(toBytes(txid)));
                delivery.disposition(declared);
                delivery.settle();
            } else if (action instanceof Discharge) {
                Discharge discharge = (Discharge) action;
                long txid = toLong(discharge.getTxnId());

                final byte operation;
                if (discharge.getFail()) {
                    LOG.trace("rollback transaction {}", txid);
                    operation = TransactionInfo.ROLLBACK;
                } else {
                    LOG.trace("commit transaction {}", txid);
                    operation = TransactionInfo.COMMIT_ONE_PHASE;
                }

                AmqpSessionContext context = (AmqpSessionContext) receiver.getSession().getContext();
                for (ConsumerContext consumer : context.consumers.values()) {
                    if (operation == TransactionInfo.ROLLBACK) {
                        consumer.doRollback();
                    } else {
                        consumer.doCommit();
                    }
                }

                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), operation);
                sendToActiveMQ(txinfo, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            ExceptionResponse er = (ExceptionResponse) response;
                            Rejected rejected = new Rejected();
                            rejected.setError(createErrorCondition("failed", er.getException().getMessage()));
                            delivery.disposition(rejected);
                        }
                        LOG.debug("TX: {} settling {}", operation, action);
                        delivery.settle();
                        pumpProtonToSocket();
                    }
                });

                for (ConsumerContext consumer : context.consumers.values()) {
                    if (operation == TransactionInfo.ROLLBACK) {
                        consumer.pumpOutbound();
                    }
                }

            } else {
                throw new Exception("Expected coordinator message type: " + action.getClass());
            }
        }
    };

    void onReceiverOpen(final Receiver receiver, AmqpSessionContext sessionContext) {
        // Client is producing to this receiver object
        org.apache.qpid.proton.amqp.transport.Target remoteTarget = receiver.getRemoteTarget();
        try {
            if (remoteTarget instanceof Coordinator) {
                pumpProtonToSocket();
                receiver.setContext(coordinatorContext);
                receiver.flow(prefetch);
                receiver.open();
                pumpProtonToSocket();
            } else {
                Target target = (Target) remoteTarget;
                ProducerId producerId = new ProducerId(sessionContext.sessionId, sessionContext.nextProducerId++);
                ActiveMQDestination dest;
                if (target.getDynamic()) {
                    dest = createTempQueue();
                    Target actualTarget = new Target();
                    actualTarget.setAddress(dest.getQualifiedName());
                    actualTarget.setDynamic(true);
                    receiver.setTarget(actualTarget);
                } else {
                    dest = createDestination(remoteTarget);
                }

                ProducerContext producerContext = new ProducerContext(producerId, dest);

                receiver.setContext(producerContext);
                receiver.flow(prefetch);
                ProducerInfo producerInfo = new ProducerInfo(producerId);
                producerInfo.setDestination(dest);
                sendToActiveMQ(producerInfo, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            receiver.setTarget(null);
                            Throwable exception = ((ExceptionResponse) response).getException();
                            receiver.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                            receiver.close();
                        } else {
                            receiver.open();
                        }
                        pumpProtonToSocket();
                    }
                });
            }
        } catch (AmqpProtocolException exception) {
            receiver.setTarget(null);
            receiver.setCondition(new ErrorCondition(Symbol.getSymbol(exception.getSymbolicName()), exception.getMessage()));
            receiver.close();
        }
    }

    private ActiveMQDestination createDestination(Object terminus) throws AmqpProtocolException {
        if (terminus == null) {
            return null;
        } else if (terminus instanceof org.apache.qpid.proton.amqp.messaging.Source) {
            org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) terminus;
            if (source.getAddress() == null || source.getAddress().length() == 0) {
                throw new AmqpProtocolException("amqp:invalid-field", "source address not set");
            }
            return ActiveMQDestination.createDestination(source.getAddress(), ActiveMQDestination.QUEUE_TYPE);
        } else if (terminus instanceof org.apache.qpid.proton.amqp.messaging.Target) {
            org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) terminus;
            if (target.getAddress() == null || target.getAddress().length() == 0) {
                throw new AmqpProtocolException("amqp:invalid-field", "target address not set");
            }
            return ActiveMQDestination.createDestination(target.getAddress(), ActiveMQDestination.QUEUE_TYPE);
        } else if (terminus instanceof Coordinator) {
            return null;
        } else {
            throw new RuntimeException("Unexpected terminus type: " + terminus);
        }
    }

    OutboundTransformer outboundTransformer = new AutoOutboundTransformer(ActiveMQJMSVendor.INSTANCE);

    class ConsumerContext extends AmqpDeliveryListener {
        private final ConsumerId consumerId;
        private final Sender sender;
        private final boolean presettle;
        private boolean closed;
        public ConsumerInfo info;
        private boolean endOfBrowse = false;

        protected LinkedList<MessageDispatch> dispatchedInTx = new LinkedList<MessageDispatch>();

        public ConsumerContext(ConsumerId consumerId, Sender sender) {
            this.consumerId = consumerId;
            this.sender = sender;
            this.presettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;
        }

        long nextTagId = 0;
        HashSet<byte[]> tagCache = new HashSet<byte[]>();

        byte[] nextTag() {
            byte[] rc;
            if (tagCache != null && !tagCache.isEmpty()) {
                final Iterator<byte[]> iterator = tagCache.iterator();
                rc = iterator.next();
                iterator.remove();
            } else {
                try {
                    rc = Long.toHexString(nextTagId++).getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            return rc;
        }

        void checkinTag(byte[] data) {
            if (tagCache.size() < 1024) {
                tagCache.add(data);
            }
        }

        @Override
        public void onClose() throws Exception {
            if (!closed) {
                closed = true;

                AmqpSessionContext session = (AmqpSessionContext) sender.getSession().getContext();
                if (session != null) {
                    session.consumers.remove(info.getConsumerId());
                }

                sendToActiveMQ(new RemoveInfo(consumerId), null);
            }
        }

        LinkedList<MessageDispatch> outbound = new LinkedList<MessageDispatch>();

        // called when the connection receives a JMS message from ActiveMQ
        public void onMessageDispatch(MessageDispatch md) throws Exception {
            if (!closed) {
                // Lock to prevent stepping on TX redelivery
                synchronized (outbound) {
                    outbound.addLast(md);
                }
                pumpOutbound();
                pumpProtonToSocket();
            }
        }

        Buffer currentBuffer;
        Delivery currentDelivery;
        final String MESSAGE_FORMAT_KEY = outboundTransformer.getPrefixVendor() + "MESSAGE_FORMAT";

        public void pumpOutbound() throws Exception {
            while (!closed) {

                while (currentBuffer != null) {
                    int sent = sender.send(currentBuffer.data, currentBuffer.offset, currentBuffer.length);
                    if (sent > 0) {
                        currentBuffer.moveHead(sent);
                        if (currentBuffer.length == 0) {
                            if (presettle) {
                                settle(currentDelivery, MessageAck.INDIVIDUAL_ACK_TYPE);
                            } else {
                                sender.advance();
                            }
                            currentBuffer = null;
                            currentDelivery = null;
                        }
                    } else {
                        return;
                    }
                }

                if (outbound.isEmpty()) {
                    return;
                }

                final MessageDispatch md = outbound.removeFirst();
                try {
                    if (md.getMessage() != null) {
                        org.apache.activemq.command.Message message = md.getMessage();
                        if (!message.getProperties().containsKey(MESSAGE_FORMAT_KEY)) {
                            message.setProperty(MESSAGE_FORMAT_KEY, 0);
                        }
                    }
                    final ActiveMQMessage jms = (ActiveMQMessage) md.getMessage();
                    if (jms == null) {
                        // It's the end of browse signal.
                        endOfBrowse = true;
                        drainCheck();
                    } else {
                        jms.setRedeliveryCounter(md.getRedeliveryCounter());
                        jms.setReadOnlyBody(true);
                        final EncodedMessage amqp = outboundTransformer.transform(jms);
                        if (amqp != null && amqp.getLength() > 0) {
                            currentBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
                            if (presettle) {
                                currentDelivery = sender.delivery(EMPTY_BYTE_ARRAY, 0, 0);
                            } else {
                                final byte[] tag = nextTag();
                                currentDelivery = sender.delivery(tag, 0, tag.length);
                            }
                            currentDelivery.setContext(md);
                        } else {
                            // TODO: message could not be generated what now?
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void settle(final Delivery delivery, final int ackType) throws Exception {
            byte[] tag = delivery.getTag();
            if (tag != null && tag.length > 0 && delivery.remotelySettled()) {
                checkinTag(tag);
            }

            if (ackType == -1) {
                // we are going to settle, but redeliver.. we we won't yet ack to ActiveMQ
                delivery.settle();
                onMessageDispatch((MessageDispatch) delivery.getContext());
            } else {
                MessageDispatch md = (MessageDispatch) delivery.getContext();
                MessageAck ack = new MessageAck();
                ack.setConsumerId(consumerId);
                ack.setFirstMessageId(md.getMessage().getMessageId());
                ack.setLastMessageId(md.getMessage().getMessageId());
                ack.setMessageCount(1);
                ack.setAckType((byte) ackType);
                ack.setDestination(md.getDestination());

                DeliveryState remoteState = delivery.getRemoteState();
                if (remoteState != null && remoteState instanceof TransactionalState) {
                    TransactionalState s = (TransactionalState) remoteState;
                    long txid = toLong(s.getTxnId());
                    LocalTransactionId localTxId = new LocalTransactionId(connectionId, txid);
                    ack.setTransactionId(localTxId);

                    // Store the message sent in this TX we might need to re-send on rollback
                    md.getMessage().setTransactionId(localTxId);
                    dispatchedInTx.addFirst(md);
                }

                LOG.trace("Sending Ack to ActiveMQ: {}", ack);

                sendToActiveMQ(ack, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            if (response.isException()) {
                                Throwable exception = ((ExceptionResponse) response).getException();
                                exception.printStackTrace();
                                sender.close();
                            }
                        } else {
                            delivery.settle();
                        }
                        pumpProtonToSocket();
                    }
                });
            }
        }

        @Override
        public void drainCheck() {
            // If we are a browser.. lets not say we are drained until
            // we hit the end of browse message.
            if( info.isBrowser() && !endOfBrowse)
                return;

            if (outbound.isEmpty()) {
                sender.drained();
            }
        }

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            MessageDispatch md = (MessageDispatch) delivery.getContext();
            DeliveryState state = delivery.getRemoteState();

            if (state instanceof TransactionalState) {
                TransactionalState txState = (TransactionalState) state;
                if (txState.getOutcome() instanceof DeliveryState) {

                    LOG.trace("onDelivery: TX delivery state = {}", state);

                    state = (DeliveryState) txState.getOutcome();

                    if (state instanceof Accepted) {
                        if (!delivery.remotelySettled()) {
                            delivery.disposition(new Accepted());
                        }
                        settle(delivery, MessageAck.DELIVERED_ACK_TYPE);
                    }
                }
            } else {
                if (state instanceof Accepted) {
                    LOG.trace("onDelivery: accepted state = {}", state);

                    if (!delivery.remotelySettled()) {
                        delivery.disposition(new Accepted());
                    }
                    settle(delivery, MessageAck.INDIVIDUAL_ACK_TYPE);
                } else if (state instanceof Rejected) {
                    // re-deliver /w incremented delivery counter.
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    LOG.trace("onDelivery: Rejected state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                    settle(delivery, -1);
                } else if (state instanceof Released) {
                    LOG.trace("onDelivery: Released state = {}", state);
                    // re-deliver && don't increment the counter.
                    settle(delivery, -1);
                } else if (state instanceof Modified) {
                    Modified modified = (Modified) state;
                    if (modified.getDeliveryFailed()) {
                        // increment delivery counter..
                        md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    }
                    LOG.trace("onDelivery: Modified state = {}, delivery count now {}", state, md.getRedeliveryCounter());
                    byte ackType = -1;
                    Boolean undeliverableHere = modified.getUndeliverableHere();
                    if (undeliverableHere != null && undeliverableHere) {
                        // receiver does not want the message..
                        // perhaps we should DLQ it?
                        ackType = MessageAck.POSION_ACK_TYPE;
                    }
                    settle(delivery, ackType);
                }
            }
            pumpOutbound();
        }

        @Override
        void doCommit() throws Exception {
            if (!dispatchedInTx.isEmpty()) {

                MessageDispatch md = dispatchedInTx.getFirst();
                MessageAck pendingTxAck = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, dispatchedInTx.size());
                pendingTxAck.setTransactionId(md.getMessage().getTransactionId());
                pendingTxAck.setFirstMessageId(dispatchedInTx.getLast().getMessage().getMessageId());

                LOG.trace("Sending commit Ack to ActiveMQ: {}", pendingTxAck);

                dispatchedInTx.clear();

                sendToActiveMQ(pendingTxAck, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            if (response.isException()) {
                                Throwable exception = ((ExceptionResponse) response).getException();
                                exception.printStackTrace();
                                sender.close();
                            }
                        }
                        pumpProtonToSocket();
                    }
                });
            }
        }

        @Override
        void doRollback() throws Exception {
            synchronized (outbound) {

                LOG.trace("Rolling back {} messages for redelivery. ", dispatchedInTx.size());

                for (MessageDispatch md : dispatchedInTx) {
                    md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                    md.getMessage().setTransactionId(null);
                    outbound.addFirst(md);
                }

                dispatchedInTx.clear();
            }
        }
    }

    private final ConcurrentHashMap<ConsumerId, ConsumerContext> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, ConsumerContext>();

    @SuppressWarnings("rawtypes")
    void onSenderOpen(final Sender sender, final AmqpSessionContext sessionContext) {
        org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) sender.getRemoteSource();

        try {
            final ConsumerId id = new ConsumerId(sessionContext.sessionId, sessionContext.nextConsumerId++);
            final ConsumerContext consumerContext = new ConsumerContext(id, sender);
            sender.setContext(consumerContext);

            String selector = null;
            if (source != null) {
                Map filter = source.getFilter();
                if (filter != null) {
                    DescribedType value = (DescribedType) filter.get(JMS_SELECTOR);
                    if (value != null) {
                        selector = value.getDescribed().toString();
                        // Validate the Selector.
                        try {
                            SelectorParser.parse(selector);
                        } catch (InvalidSelectorException e) {
                            sender.setSource(null);
                            sender.setCondition(new ErrorCondition(AmqpError.INVALID_FIELD, e.getMessage()));
                            sender.close();
                            consumerContext.closed = true;
                            return;
                        }
                    }
                }
            }

            ActiveMQDestination dest;
            if (source == null) {

                source = new org.apache.qpid.proton.amqp.messaging.Source();
                source.setAddress("");
                source.setCapabilities(DURABLE_SUBSCRIPTION_ENDED);
                sender.setSource(source);

                // Looks like durable sub removal.
                RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(connectionId);
                rsi.setSubscriptionName(sender.getName());
                rsi.setClientId(connectionInfo.getClientId());

                consumerContext.closed = true;
                sendToActiveMQ(rsi, new ResponseHandler() {
                    @Override
                    public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                        if (response.isException()) {
                            sender.setSource(null);
                            Throwable exception = ((ExceptionResponse) response).getException();
                            String name = exception.getClass().getName();
                            sender.setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, exception.getMessage()));
                        }
                        sender.open();
                        pumpProtonToSocket();
                    }
                });
                return;
            } else if (contains(source.getCapabilities(), DURABLE_SUBSCRIPTION_ENDED)) {
                consumerContext.closed = true;
                sender.close();
                pumpProtonToSocket();
                return;
            } else if (source.getDynamic()) {
                // lets create a temp dest.
                dest = createTempQueue();
                source = new org.apache.qpid.proton.amqp.messaging.Source();
                source.setAddress(dest.getQualifiedName());
                source.setDynamic(true);
                sender.setSource(source);
            } else {
                dest = createDestination(source);
            }

            subscriptionsByConsumerId.put(id, consumerContext);
            ConsumerInfo consumerInfo = new ConsumerInfo(id);
            consumerContext.info = consumerInfo;
            consumerInfo.setSelector(selector);
            consumerInfo.setNoRangeAcks(true);
            consumerInfo.setDestination(dest);
            consumerInfo.setPrefetchSize(100);
            consumerInfo.setDispatchAsync(true);
            if (source.getDistributionMode() == COPY && dest.isQueue()) {
                consumerInfo.setBrowser(true);
            }
            if (TerminusDurability.UNSETTLED_STATE.equals(source.getDurable()) && dest.isTopic()) {
                consumerInfo.setSubscriptionName(sender.getName());
            }

            Map filter = source.getFilter();
            if (filter != null) {
                DescribedType value = (DescribedType) filter.get(NO_LOCAL);
                if (value != null) {
                    consumerInfo.setNoLocal(true);
                }
            }

            sendToActiveMQ(consumerInfo, new ResponseHandler() {
                @Override
                public void onResponse(IAmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        sender.setSource(null);
                        Throwable exception = ((ExceptionResponse) response).getException();
                        Symbol condition = AmqpError.INTERNAL_ERROR;
                        if (exception instanceof InvalidSelectorException) {
                            condition = AmqpError.INVALID_FIELD;
                        }
                        sender.setCondition(new ErrorCondition(condition, exception.getMessage()));
                        subscriptionsByConsumerId.remove(id);
                        sender.close();
                    } else {
                        sessionContext.consumers.put(id, consumerContext);
                        sender.open();
                    }
                    pumpProtonToSocket();
                }
            });
        } catch (AmqpProtocolException e) {
            sender.setSource(null);
            sender.setCondition(new ErrorCondition(Symbol.getSymbol(e.getSymbolicName()), e.getMessage()));
            sender.close();
        }
    }

    static private boolean contains(Symbol[] haystack, Symbol needle) {
        if (haystack != null) {
            for (Symbol capability : haystack) {
                if (capability == needle) {
                    return true;
                }
            }
        }
        return false;
    }

    private ActiveMQDestination createTempQueue() {
        ActiveMQDestination rc;
        rc = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionId);
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(rc);
        sendToActiveMQ(info, null);
        return rc;
    }

    // //////////////////////////////////////////////////////////////////////////
    //
    // Implementation methods
    //
    // //////////////////////////////////////////////////////////////////////////

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;

    int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }

    private final ConcurrentHashMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();

    void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(generateCommandId());
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

    ErrorCondition createErrorCondition(String name) {
        return createErrorCondition(name, "");
    }

    ErrorCondition createErrorCondition(String name, String description) {
        ErrorCondition condition = new ErrorCondition();
        condition.setCondition(Symbol.valueOf(name));
        condition.setDescription(description);
        return condition;
    }
}
