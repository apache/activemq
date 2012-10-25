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

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.*;
import org.apache.activemq.transport.amqp.transform.*;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.apache.qpid.proton.type.Binary;
import org.apache.qpid.proton.type.DescribedType;
import org.apache.qpid.proton.type.Symbol;
import org.apache.qpid.proton.type.messaging.*;
import org.apache.qpid.proton.type.messaging.Modified;
import org.apache.qpid.proton.type.messaging.Rejected;
import org.apache.qpid.proton.type.messaging.Released;
import org.apache.qpid.proton.type.transaction.*;
import org.apache.qpid.proton.type.transport.DeliveryState;
import org.apache.qpid.proton.type.transport.SenderSettleMode;
import org.apache.qpid.proton.type.transport.Source;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

class AmqpProtocolConverter {

    public static final EnumSet<EndpointState> UNINITIALIZED_SET = EnumSet.of(EndpointState.UNINITIALIZED);
    public static final EnumSet<EndpointState> INITIALIZED_SET = EnumSet.complementOf(UNINITIALIZED_SET);
    public static final EnumSet<EndpointState> ACTIVE_STATE = EnumSet.of(EndpointState.ACTIVE);
    public static final EnumSet<EndpointState> CLOSED_STATE = EnumSet.of(EndpointState.CLOSED);
    public static final EnumSet<EndpointState> ALL_STATES = EnumSet.of(EndpointState.CLOSED, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);
    private static final Logger LOG = LoggerFactory.getLogger(AmqpProtocolConverter.class);
    static final public byte[] EMPTY_BYTE_ARRAY = new byte[]{};
    private final AmqpTransport amqpTransport;

    public AmqpProtocolConverter(AmqpTransport amqpTransport, BrokerContext brokerContext) {
        this.amqpTransport = amqpTransport;
    }

    ReentrantLock lock = new ReentrantLock();

//
//    private static final Buffer PING_RESP_FRAME = new PINGRESP().encode();
//
//
//    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
//    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
//
//    private final ConcurrentHashMap<ConsumerId, AmqpSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, AmqpSubscription>();
//    private final ConcurrentHashMap<UTF8Buffer, AmqpSubscription> amqpSubscriptionByTopic = new ConcurrentHashMap<UTF8Buffer, AmqpSubscription>();
//    private final Map<UTF8Buffer, ActiveMQTopic> activeMQTopicMap = new LRUCache<UTF8Buffer, ActiveMQTopic>();
//    private final Map<Destination, UTF8Buffer> amqpTopicMap = new LRUCache<Destination, UTF8Buffer>();
//    private final Map<Short, MessageAck> consumerAcks = new LRUCache<Short, MessageAck>();
//    private final Map<Short, PUBREC> publisherRecs = new LRUCache<Short, PUBREC>();
//
//    private final AtomicBoolean connected = new AtomicBoolean(false);
//    private CONNECT connect;
//    private String clientId;
//    private final String QOS_PROPERTY_NAME = "QoSPropertyName";


    TransportImpl protonTransport = new TransportImpl();
    ConnectionImpl protonConnection = new ConnectionImpl();

    {
        this.protonTransport.bind(this.protonConnection);
        this.protonTransport.setProtocolTracer(new ProtocolTracer() {
            @Override
            public void receivedFrame(TransportFrame transportFrame) {
                System.out.println(String.format("RECV: %05d | %s", transportFrame.getChannel(), transportFrame.getBody()));
            }

            @Override
            public void sentFrame(TransportFrame transportFrame) {
                System.out.println(String.format("SENT: %05d | %s", transportFrame.getChannel(), transportFrame.getBody()));
            }
        });

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
//                    System.out.println("writing: " + buffer.toString().substring(5).replaceAll("(..)", "$1 "));
                    amqpTransport.sendToAmqp(buffer);
                } else {
                    done = true;
                }
            }
//            System.out.println("write done");
        } catch (IOException e) {
            amqpTransport.onException(e);
        }
    }

    static class AmqpSessionContext {
        private final SessionId sessionId;
        long nextProducerId = 0;
        long nextConsumerId = 0;

        public AmqpSessionContext(ConnectionId connectionId, long id) {
            sessionId = new SessionId(connectionId, id);

        }
    }

    /**
     * Convert a AMQP command
     */
    public void onAMQPData(Buffer frame) throws IOException, JMSException {


        try {
//            System.out.println("reading: " + frame.toString().substring(5).replaceAll("(..)", "$1 "));
            protonTransport.input(frame.data, frame.offset, frame.length);
        } catch (Throwable e) {
            handleException(new AmqpProtocolException("Could not decode AMQP frame: " + frame, true, e));
        }

        try {

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
                ((AmqpDeliveryListener)link.getContext()).onClose();
                link.close();
                link = link.next(ACTIVE_STATE, CLOSED_STATE);
            }

            link = protonConnection.linkHead(ACTIVE_STATE, ALL_STATES);
            while (link != null) {
                ((AmqpDeliveryListener)link.getContext()).drainCheck();
                link = link.next(ACTIVE_STATE, CLOSED_STATE);
            }


            session = protonConnection.sessionHead(ACTIVE_STATE, CLOSED_STATE);
            while (session != null) {
                //TODO - close links?
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

    boolean closing = false;
    boolean closedSocket = false;

    private void doClose() {
        if( !closing ) {
            closing = true;
            sendToActiveMQ(new RemoveInfo(connectionId), new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    protonConnection.close();
                    if( !closedSocket) {
                        pumpProtonToSocket();
                    }
                }
            });
        }
    }


    public void onAMQPException(IOException error) {
        closedSocket = true;
        if( !closing) {
            System.out.println("AMQP client disconnected");
            error.printStackTrace();
        } else {
            doClose();
        }
    }

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
                consumerContext.onMessageDispatch(md);
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError) command).getException();
            handleException(exception);
        } else if (command.isBrokerInfo()) {
            //ignore
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command " + command);
        }
    }

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private ConnectionInfo connectionInfo = new ConnectionInfo();
    private long nextSessionId = 0;
    private long nextTempDestinationId = 0;
    HashMap<Sender, ActiveMQDestination> tempDestinations = new HashMap<Sender, ActiveMQDestination>();

    static abstract class AmqpDeliveryListener {
        abstract public void onDelivery(Delivery delivery) throws Exception;
        public void onClose() throws Exception {}
        public void drainCheck() {}
    }

    private void onConnectionOpen() throws AmqpProtocolException {

        connectionInfo.setResponseRequired(true);
        connectionInfo.setConnectionId(connectionId);
//        configureInactivityMonitor(connect.keepAlive());

        String clientId = protonConnection.getRemoteContainer();
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        } else {
            connectionInfo.setClientId("" + connectionInfo.getConnectionId().toString());
        }


//        String userName = "";
//        if (connect.userName() != null) {
//            userName = connect.userName().toString();
//        }
//        String passswd = "";
//        if (connect.password() != null) {
//            passswd = connect.password().toString();
//        }
//        connectionInfo.setUserName(userName);
//        connectionInfo.setPassword(passswd);

        connectionInfo.setTransportContext(amqpTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                protonConnection.open();
                pumpProtonToSocket();

                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
// TODO: figure out how to close /w an error.
//                    protonConnection.setLocalError(new EndpointError(exception.getClass().getName(), exception.getMessage()));
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
        AmqpSessionContext sessionContext = (AmqpSessionContext)session.getContext();
        if( sessionContext!=null ) {
            System.out.println(sessionContext.sessionId);
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

    protected InboundTransformer getInboundTransformer()  {
        if (inboundTransformer == null) {
            String transformer = amqpTransport.getTransformer();
            if (transformer.equals(InboundTransformer.TRANSFORMER_JMS)) {
                inboundTransformer = new JMSMappingInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_NATIVE)) {
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else if (transformer.equals(InboundTransformer.TRANSFORMER_RAW)) {
                inboundTransformer = new AMQPRawInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            } else {
                LOG.warn("Unknown transformer type " + transformer + ", using native one instead");
                inboundTransformer = new AMQPNativeInboundTransformer(ActiveMQJMSVendor.INSTANCE);
            }
        }
        return inboundTransformer;
    }

    abstract class BaseProducerContext extends AmqpDeliveryListener {

        ByteArrayOutputStream current = new ByteArrayOutputStream();

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            Receiver receiver = ((Receiver)delivery.getLink());
            if( !delivery.isReadable() ) {
                System.out.println("it was not readable!");
//                delivery.settle();
//                receiver.advance();
                return;
            }

            if( current==null ) {
                current = new ByteArrayOutputStream();
            }

            int count;
            byte data[] = new byte[1024*4];
            while( (count = receiver.recv(data, 0, data.length)) > 0 ) {
                current.write(data, 0, count);
            }

            // Expecting more deliveries..
            if( count == 0 ) {
                return;
            }

            receiver.advance();
            delivery.settle();

            Buffer buffer = current.toBuffer();
            current = null;
            onMessage(receiver, delivery, buffer);
        }

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
        protected void onMessage(Receiver receiver, Delivery delivery, Buffer buffer) throws Exception {
            EncodedMessage em = new EncodedMessage(delivery.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
            final ActiveMQMessage message = (ActiveMQMessage) getInboundTransformer().transform(em);
            current = null;

            if( message.getDestination()==null ) {
                message.setJMSDestination(destination);
            }
            message.setProducerId(producerId);
            if( message.getMessageId()==null ) {
                message.setMessageId(new MessageId(producerId, messageIdGenerator.getNextSequenceId()));
            }

            DeliveryState remoteState = delivery.getRemoteState();
            if( remoteState!=null && remoteState instanceof TransactionalState) {
                TransactionalState s = (TransactionalState) remoteState;
                long txid = toLong(s.getTxnId());
                message.setTransactionId(new LocalTransactionId(connectionId, txid));
            }

            message.onSend();
//            sendToActiveMQ(message, createResponseHandler(command));
            sendToActiveMQ(message, null);
        }

    }

    long nextTransactionId = 0;
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

            org.apache.qpid.proton.message.Message msg = new org.apache.qpid.proton.message.Message();

            int offset = buffer.offset;
            int len = buffer.length;
            while( len > 0 ) {
                final int decoded = msg.decode(buffer.data, offset, len);
                assert decoded > 0: "Make progress decoding the message";
                offset += decoded;
                len -= decoded;
            }

            Object action = ((AmqpValue)msg.getBody()).getValue();
            System.out.println("COORDINATOR received: "+action+", ["+buffer+"]");
            if( action instanceof Declare ) {
                Declare declare = (Declare) action;
                if( declare.getGlobalId()!=null ) {
                    throw new Exception("don't know how to handle a declare /w a set GlobalId");
                }

                long txid = nextTransactionId++;
                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), TransactionInfo.BEGIN);
                sendToActiveMQ(txinfo, null);
                System.out.println("started transaction "+txid);

                Declared declared = new Declared();
                declared.setTxnId(new Binary(toBytes(txid)));
                delivery.disposition(declared);
                delivery.settle();

            } else if( action instanceof Discharge) {
                Discharge discharge = (Discharge) action;
                long txid = toLong(discharge.getTxnId());

                byte operation;
                if( discharge.getFail() ) {
                    System.out.println("rollback transaction "+txid);
                    operation = TransactionInfo.ROLLBACK ;
                } else {
                    System.out.println("commit transaction "+txid);
                    operation = TransactionInfo.COMMIT_ONE_PHASE;
                }
                TransactionInfo txinfo = new TransactionInfo(connectionId, new LocalTransactionId(connectionId, txid), operation);
                sendToActiveMQ(txinfo, new ResponseHandler() {
                    @Override
                    public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                        if( response.isException() ) {
                            ExceptionResponse er = (ExceptionResponse)response;
                            Rejected rejected = new Rejected();
                            ArrayList errors = new ArrayList();
                            errors.add(er.getException().getMessage());
                            rejected.setError(errors);
                            delivery.disposition(rejected);
                        }
                        delivery.settle();
                        pumpProtonToSocket();
                    }
                });
                receiver.advance();

            } else {
                throw new Exception("Expected coordinator message type: "+action.getClass());
            }

        }

    };


    void onReceiverOpen(final Receiver receiver, AmqpSessionContext sessionContext) {
        // Client is producing to this receiver object
        org.apache.qpid.proton.type.transport.Target remoteTarget = receiver.getRemoteTarget();
        if( remoteTarget instanceof Coordinator ) {
            pumpProtonToSocket();
            receiver.setContext(coordinatorContext);
            receiver.flow(1024 * 64);
            receiver.open();
            pumpProtonToSocket();
        } else {
            ProducerId producerId = new ProducerId(sessionContext.sessionId, sessionContext.nextProducerId++);
            ActiveMQDestination dest = createDestination(remoteTarget);
            ProducerContext producerContext = new ProducerContext(producerId, dest);

            receiver.setContext(producerContext);
            receiver.flow(1024 * 64);
            ProducerInfo producerInfo = new ProducerInfo(producerId);
            producerInfo.setDestination(dest);
            sendToActiveMQ(producerInfo, new ResponseHandler() {
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    receiver.open();
                    if (response.isException()) {
                        // If the connection attempt fails we close the socket.
                        Throwable exception = ((ExceptionResponse) response).getException();
                        receiver.close();
                    }
                    pumpProtonToSocket();
                }
            });
        }


    }

    private ActiveMQDestination createDestination(Object terminus) {
        if( terminus == null ) {
            return null;
        } else if( terminus instanceof org.apache.qpid.proton.type.messaging.Source) {
            org.apache.qpid.proton.type.messaging.Source source = (org.apache.qpid.proton.type.messaging.Source)terminus;
            return ActiveMQDestination.createDestination(source.getAddress(), ActiveMQDestination.QUEUE_TYPE);
        } else if( terminus instanceof org.apache.qpid.proton.type.messaging.Target) {
            org.apache.qpid.proton.type.messaging.Target target = (org.apache.qpid.proton.type.messaging.Target)terminus;
            return ActiveMQDestination.createDestination(target.getAddress(), ActiveMQDestination.QUEUE_TYPE);
        } else if( terminus instanceof Coordinator ) {
            Coordinator target = (Coordinator)terminus;
            return null;
        } else {
            throw new RuntimeException("Unexpected terminus type: "+terminus);
        }
    }

    private Source createSource(ActiveMQDestination dest) {
        org.apache.qpid.proton.type.messaging.Source rc = new org.apache.qpid.proton.type.messaging.Source();
        rc.setAddress(getInboundTransformer().getVendor().toAddress(dest));
        return rc;
    }

    OutboundTransformer outboundTransformer = new AutoOutboundTransformer(ActiveMQJMSVendor.INSTANCE);


    class ConsumerContext extends AmqpDeliveryListener {
        private final ConsumerId consumerId;
        private final Sender sender;
        private boolean presettle;

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
            if( tagCache.size() < 1024 ) {
                tagCache.add(data);
            }
        }


        @Override
        public void onClose() throws Exception {
            sendToActiveMQ(new RemoveInfo(consumerId), null);
        }

        LinkedList<MessageDispatch> outbound = new LinkedList<MessageDispatch>();

        // called when the connection receives a JMS message from ActiveMQ
        public void onMessageDispatch(MessageDispatch md) throws Exception {
            outbound.addLast(md);
            pumpOutbound();
            pumpProtonToSocket();
        }

        Buffer currentBuffer;
        Delivery currentDelivery;

        public void pumpOutbound() throws Exception {
            while(true) {

                while( currentBuffer !=null ) {
                    int sent = sender.send(currentBuffer.data, currentBuffer.offset, currentBuffer.length);
                    if( sent > 0 ) {
                        currentBuffer.moveHead(sent);
                        if( currentBuffer.length == 0 ) {
                            if( presettle ) {
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

                if( outbound.isEmpty() ) {
                    return;
                }

                final MessageDispatch md = outbound.removeFirst();
                try {
                    final ActiveMQMessage jms = (ActiveMQMessage) md.getMessage();
                    jms.setRedeliveryCounter(md.getRedeliveryCounter());
                    final EncodedMessage amqp = outboundTransformer.transform(jms);
                    if( amqp!=null && amqp.getLength() > 0 ) {

                        currentBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
                        if( presettle ) {
                            currentDelivery = sender.delivery(EMPTY_BYTE_ARRAY, 0, 0);
                        } else {
                            final byte[] tag = nextTag();
                            currentDelivery = sender.delivery(tag, 0, tag.length);
                        }
                        currentDelivery.setContext(md);

                    } else {
                        // TODO: message could not be generated what now?

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void settle(final Delivery delivery, int ackType) throws Exception {
            byte[] tag = delivery.getTag();
            if( tag !=null && tag.length>0 ) {
                checkinTag(tag);
            }

            if( ackType == -1) {
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
                ack.setAckType((byte)ackType);

                DeliveryState remoteState = delivery.getRemoteState();
                if( remoteState!=null && remoteState instanceof TransactionalState) {
                    TransactionalState s = (TransactionalState) remoteState;
                    long txid = toLong(s.getTxnId());
                    ack.setTransactionId(new LocalTransactionId(connectionId, txid));
                }

                sendToActiveMQ(ack, new ResponseHandler() {
                    @Override
                    public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                        delivery.settle();
                        pumpProtonToSocket();
                    }
                });
            }
        }

        @Override
        public void drainCheck() {
            if( outbound.isEmpty() ) {
                sender.drained();
            }
        }

        @Override
        public void onDelivery(Delivery delivery) throws Exception {
            MessageDispatch md = (MessageDispatch) delivery.getContext();
            final DeliveryState state = delivery.getRemoteState();
            if( state instanceof Accepted ) {
                if( !delivery.remotelySettled() ) {
                    delivery.disposition(new Accepted());
                }
                settle(delivery, MessageAck.INDIVIDUAL_ACK_TYPE);
            } else if( state instanceof Rejected) {
                // re-deliver /w incremented delivery counter.
                md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                settle(delivery, -1);
            } else if( state instanceof Released) {
                // re-deliver && don't increment the counter.
                settle(delivery, -1);
            } else if( state instanceof Modified) {
                Modified modified = (Modified) state;
                if ( modified.getDeliveryFailed() ) {
                  // increment delivery counter..
                  md.setRedeliveryCounter(md.getRedeliveryCounter() + 1);
                }
                byte ackType = -1;
                Boolean undeliverableHere = modified.getUndeliverableHere();
                if( undeliverableHere !=null && undeliverableHere ) {
                    // receiver does not want the message..
                    // perhaps we should DLQ it?
                    ackType = MessageAck.POSION_ACK_TYPE;
                }
                settle(delivery, ackType);
            }
            pumpOutbound();
        }

    }

    private final ConcurrentHashMap<ConsumerId, ConsumerContext> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, ConsumerContext>();

    void onSenderOpen(final Sender sender, AmqpSessionContext sessionContext) {
        // sender.get
        ConsumerId id = new ConsumerId(sessionContext.sessionId, sessionContext.nextConsumerId++);
        ConsumerContext consumerContext = new ConsumerContext(id, sender);

        subscriptionsByConsumerId.put(id, consumerContext);

        ActiveMQDestination dest;
        final Source remoteSource = sender.getRemoteSource();
        if( remoteSource != null ) {
            dest = createDestination(remoteSource);
        } else {
            // lets create a temp dest.
//            if (topic) {
//                dest = new ActiveMQTempTopic(info.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());
//            } else {
                dest = new ActiveMQTempQueue(connectionId, nextTempDestinationId++);
//            }

            DestinationInfo info = new DestinationInfo();
            info.setConnectionId(connectionId);
            info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
            info.setDestination(dest);
            sendToActiveMQ(info, null);
            tempDestinations.put(sender, dest);
            sender.setSource(createSource(dest));
        }


        sender.setContext(consumerContext);
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setDestination(dest);
        consumerInfo.setPrefetchSize(100);
        consumerInfo.setDispatchAsync(true);
        Map filter = ((org.apache.qpid.proton.type.messaging.Source)remoteSource).getFilter();
        if (filter != null) {
            DescribedType type = (DescribedType)filter.get(Symbol.valueOf("jms-selector"));
            consumerInfo.setSelector(type.getDescribed().toString());
        }

        sendToActiveMQ(consumerInfo, new ResponseHandler() {
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                sender.open();
                if (response.isException()) {
                    Throwable exception = ((ExceptionResponse) response).getException();
                    sender.close();
                }
                pumpProtonToSocket();
            }
        });

    }

//    void onUnSubscribe(UNSUBSCRIBE command) {
//        UTF8Buffer[] topics = command.topics();
//        if (topics != null) {
//            for (int i = 0; i < topics.length; i++) {
//                onUnSubscribe(topics[i]);
//            }
//        }
//        UNSUBACK ack = new UNSUBACK();
//        ack.messageId(command.messageId());
//        pumpOut(ack.encode());
//
//    }
//
//    void onUnSubscribe(UTF8Buffer topicName) {
//        AmqpSubscription subs = amqpSubscriptionByTopic.remove(topicName);
//        if (subs != null) {
//            ConsumerInfo info = subs.getConsumerInfo();
//            if (info != null) {
//                subscriptionsByConsumerId.remove(info.getConsumerId());
//            }
//            RemoveInfo removeInfo = info.createRemoveCommand();
//            sendToActiveMQ(removeInfo, null);
//        }
//    }
//
//
//    /**
//     * Dispatch a ActiveMQ command
//     */
//
//
//
//    void onAMQPPublish(PUBLISH command) throws IOException, JMSException {
//        checkConnected();
//    }
//
//    void onAMQPPubAck(PUBACK command) {
//        short messageId = command.messageId();
//        MessageAck ack;
//        synchronized (consumerAcks) {
//            ack = consumerAcks.remove(messageId);
//        }
//        if (ack != null) {
//            amqpTransport.sendToActiveMQ(ack);
//        }
//    }
//
//    void onAMQPPubRec(PUBREC commnand) {
//        //from a subscriber - send a PUBREL in response
//        PUBREL pubrel = new PUBREL();
//        pubrel.messageId(commnand.messageId());
//        pumpOut(pubrel.encode());
//    }
//
//    void onAMQPPubRel(PUBREL command) {
//        PUBREC ack;
//        synchronized (publisherRecs) {
//            ack = publisherRecs.remove(command.messageId());
//        }
//        if (ack == null) {
//            LOG.warn("Unknown PUBREL: " + command.messageId() + " received");
//        }
//        PUBCOMP pubcomp = new PUBCOMP();
//        pubcomp.messageId(command.messageId());
//        pumpOut(pubcomp.encode());
//    }
//
//    void onAMQPPubComp(PUBCOMP command) {
//        short messageId = command.messageId();
//        MessageAck ack;
//        synchronized (consumerAcks) {
//            ack = consumerAcks.remove(messageId);
//        }
//        if (ack != null) {
//            amqpTransport.sendToActiveMQ(ack);
//        }
//    }
//
//
//
//
//    public AmqpTransport amqpTransport {
//        return amqpTransport;
//    }
//
//
//
//    void configureInactivityMonitor(short heartBeat) {
//        try {
//
//            int heartBeatMS = heartBeat * 1000;
//            AmqpInactivityMonitor monitor = amqpTransport.getInactivityMonitor();
//            monitor.setProtocolConverter(this);
//            monitor.setReadCheckTime(heartBeatMS);
//            monitor.setInitialDelayTime(heartBeatMS);
//            monitor.startMonitorThread();
//
//        } catch (Exception ex) {
//            LOG.warn("Failed to start AMQP InactivityMonitor ", ex);
//        }
//
//        LOG.debug(getClientId() + " AMQP Connection using heart beat of  " + heartBeat + " secs");
//    }
//
//
//
//    void checkConnected() throws AmqpProtocolException {
//        if (!connected.get()) {
//            throw new AmqpProtocolException("Not connected.");
//        }
//    }
//
//    private String getClientId() {
//        if (clientId == null) {
//            if (connect != null && connect.clientId() != null) {
//                clientId = connect.clientId().toString();
//            }
//        } else {
//            clientId = "";
//        }
//        return clientId;
//    }
//
//    private void stopTransport() {
//        try {
//            amqpTransport.stop();
//        } catch (Throwable e) {
//            LOG.debug("Failed to stop AMQP transport ", e);
//        }
//    }
//
//    ResponseHandler createResponseHandler(final PUBLISH command) {
//
//        if (command != null) {
//            switch (command.qos()) {
//                case AT_LEAST_ONCE:
//                    return new ResponseHandler() {
//                        public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
//                            if (response.isException()) {
//                                LOG.warn("Failed to send AMQP Publish: ", command, ((ExceptionResponse) response).getException());
//                            } else {
//                                PUBACK ack = new PUBACK();
//                                ack.messageId(command.messageId());
//                                converter.amqpTransport.sendToAmqp(ack.encode());
//                            }
//                        }
//                    };
//                case EXACTLY_ONCE:
//                    return new ResponseHandler() {
//                        public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
//                            if (response.isException()) {
//                                LOG.warn("Failed to send AMQP Publish: ", command, ((ExceptionResponse) response).getException());
//                            } else {
//                                PUBREC ack = new PUBREC();
//                                ack.messageId(command.messageId());
//                                synchronized (publisherRecs) {
//                                    publisherRecs.put(command.messageId(), ack);
//                                }
//                                converter.amqpTransport.sendToAmqp(ack.encode());
//                            }
//                        }
//                    };
//                case AT_MOST_ONCE:
//                    break;
//            }
//        }
//        return null;
//    }
//
//    private String convertAMQPToActiveMQ(String name) {
//        String result = name.replace('#', '>');
//        result = result.replace('+', '*');
//        result = result.replace('/', '.');
//        return result;
//    }

    ////////////////////////////////////////////////////////////////////////////
    //
    // Implementation methods
    //
    ////////////////////////////////////////////////////////////////////////////

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exception detail", exception);
        }
        try {
            amqpTransport.stop();
        } catch (Throwable e) {
            LOG.error("Failed to stop AMQP Transport ", e);
        }
    }

}
