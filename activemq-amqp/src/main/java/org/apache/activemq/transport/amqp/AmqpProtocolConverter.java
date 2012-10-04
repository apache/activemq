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
import org.apache.activemq.util.*;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.fusesource.hawtbuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.fusesource.hawtbuf.ByteArrayOutputStream;

class AmqpProtocolConverter {

    public static final EnumSet<EndpointState> UNINITIALIZED_SET = EnumSet.of(EndpointState.UNINITIALIZED);
    public static final EnumSet<EndpointState> INITIALIZED_SET = EnumSet.complementOf(UNINITIALIZED_SET);
    public static final EnumSet<EndpointState> ACTIVE_STATE = EnumSet.of(EndpointState.ACTIVE);
    public static final EnumSet<EndpointState> CLOSED_STATE = EnumSet.of(EndpointState.CLOSED);
    private static final Logger LOG = LoggerFactory.getLogger(AmqpProtocolConverter.class);

    private final AmqpTransport amqpTransport;

    public AmqpProtocolConverter(AmqpTransport amqpTransport, BrokerContext brokerContext) {
        this.amqpTransport = amqpTransport;
    }

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
    }

    void pumpProtonToSocket() {
        try {
            int size = 1024 * 64;
            byte data[] = new byte[size];
            boolean done = false;
            while (!done) {
                int count = protonTransport.output(data, 0, size);
                if (count > 0) {
                    final Buffer buffer = new Buffer(data, 0, count);
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
            sessionId = new SessionId(connectionId, -1);

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

            session = protonConnection.sessionHead(ACTIVE_STATE, CLOSED_STATE);
            while (session != null) {
                //TODO - close links?
                onSessionClose(session);
                session = session.next(ACTIVE_STATE, CLOSED_STATE);
            }
            if (protonConnection.getLocalState() == EndpointState.ACTIVE && protonConnection.getRemoteState() == EndpointState.CLOSED) {
//                listener.onConnectionClose(protonConnection);
                protonConnection.close();
            }

        } catch (Throwable e) {
            handleException(new AmqpProtocolException("Could not process AMQP commands", true, e));
        }

        pumpProtonToSocket();
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

    static abstract class AmqpDeliveryListener {
        abstract public void onDelivery(Delivery delivery) throws Exception;
        public void onClose() throws Exception {}
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
        sendToActiveMQ(new RemoveInfo(sessionContext.sessionId), null);
        session.close();
    }

    private void onLinkOpen(Link link) {
        link.setLocalSourceAddress(link.getRemoteSourceAddress());
        link.setLocalTargetAddress(link.getRemoteTargetAddress());

        AmqpSessionContext sessionContext = (AmqpSessionContext) link.getSession().getContext();
        if (link instanceof Receiver) {
            onReceiverOpen((Receiver) link, sessionContext);
        } else {
            onSenderOpen((Sender) link, sessionContext);
        }
    }

    InboundTransformer inboundTransformer = new JMSMappingInboundTransformer(ActiveMQJMSVendor.INSTANCE);

    class ProducerContext extends AmqpDeliveryListener {
        private final ProducerId producerId;
        private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
        private final ActiveMQDestination destination;
        ByteArrayOutputStream current = new ByteArrayOutputStream();

        public ProducerContext(ProducerId producerId, ActiveMQDestination destination) {
            this.producerId = producerId;
            this.destination = destination;
        }


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

            final Buffer buffer = current.toBuffer();
            EncodedMessage em = new EncodedMessage(delivery.getMessageFormat(), buffer.data, buffer.offset, buffer.length);
            final ActiveMQMessage message = (ActiveMQMessage) inboundTransformer.transform(em);
            current = null;

            if( message.getDestination()==null ) {
                message.setJMSDestination(destination);
            }
            message.setProducerId(producerId);
            if( message.getMessageId()==null ) {
                message.setMessageId(new MessageId(producerId, messageIdGenerator.getNextSequenceId()));
            }
            message.onSend();
//            sendToActiveMQ(message, createResponseHandler(command));
            sendToActiveMQ(message, null);
        }

    }


    void onReceiverOpen(final Receiver receiver, AmqpSessionContext sessionContext) {
        // Client is producing to this receiver object

        ProducerId producerId = new ProducerId(sessionContext.sessionId, sessionContext.nextProducerId++);
        ActiveMQDestination destination = ActiveMQDestination.createDestination(receiver.getRemoteTargetAddress(), ActiveMQDestination.QUEUE_TYPE);
        ProducerContext producerContext = new ProducerContext(producerId, destination);

        receiver.setContext(producerContext);
        receiver.flow(1024 * 64);
        ProducerInfo producerInfo = new ProducerInfo(producerId);
        producerInfo.setDestination(destination);
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

    OutboundTransformer outboundTransformer = new AutoOutboundTransformer(ActiveMQJMSVendor.INSTANCE);

    class ConsumerContext extends AmqpDeliveryListener {
        private final ConsumerId consumerId;
        private final Sender sender;

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

        public ConsumerContext(ConsumerId consumerId, Sender sender) {
            this.consumerId = consumerId;
            this.sender = sender;
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

        public void pumpOutbound() {
            while(true) {

                while( currentBuffer !=null ) {
                    int sent = sender.send(currentBuffer.data, currentBuffer.offset, currentBuffer.length);
                    if( sent > 0 ) {
                        currentBuffer.moveHead(sent);
                        if( currentBuffer.length == 0 ) {
                            currentDelivery.settle();
                            sender.advance();
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
                    final EncodedMessage amqp = outboundTransformer.transform(jms);
                    if( amqp!=null && amqp.getLength() > 0 ) {

                        currentBuffer = new Buffer(amqp.getArray(), amqp.getArrayOffset(), amqp.getLength());
                        final byte[] tag = nextTag();
                        currentDelivery = sender.delivery(tag, 0, tag.length);
                        currentDelivery.setContext(md);

                    } else {
                        // TODO: message could not be generated what now?

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onDelivery(Delivery delivery) throws JMSException {
            if( delivery.remotelySettled() ) {
                MessageDispatch md = (MessageDispatch) delivery.getContext();
                pumpOutbound();
            }
        }

    }

    private final ConcurrentHashMap<ConsumerId, ConsumerContext> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, ConsumerContext>();

    void onSenderOpen(final Sender sender, AmqpSessionContext sessionContext) {

        ConsumerId id = new ConsumerId(sessionContext.sessionId, sessionContext.nextConsumerId++);
        ConsumerContext consumerContext = new ConsumerContext(id, sender);

        subscriptionsByConsumerId.put(id, consumerContext);

        ActiveMQDestination destination = ActiveMQDestination.createDestination(sender.getRemoteSourceAddress(), ActiveMQDestination.QUEUE_TYPE);

        sender.setContext(consumerContext);
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setDestination(destination);
        consumerInfo.setPrefetchSize(100);
        consumerInfo.setDispatchAsync(true);

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
