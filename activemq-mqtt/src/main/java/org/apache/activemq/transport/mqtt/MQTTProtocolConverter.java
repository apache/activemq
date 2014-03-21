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
package org.apache.activemq.transport.mqtt;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.*;
import org.apache.activemq.store.PersistenceAdapterSupport;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.LongSequenceGenerator;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTProtocolConverter.class);

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private static final MQTTFrame PING_RESP_FRAME = new PINGRESP().encode();
    private static final double MQTT_KEEP_ALIVE_GRACE_PERIOD= 0.5;
    static final int DEFAULT_CACHE_SIZE = 5000;
    private static final byte SUBSCRIBE_ERROR = (byte) 0x80;

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);
    private final LongSequenceGenerator publisherIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();

    private final ConcurrentHashMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final ConcurrentHashMap<ConsumerId, MQTTSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, MQTTSubscription>();
    private final ConcurrentHashMap<UTF8Buffer, MQTTSubscription> mqttSubscriptionByTopic = new ConcurrentHashMap<UTF8Buffer, MQTTSubscription>();
    private final Map<UTF8Buffer, ActiveMQTopic> activeMQTopicMap = new LRUCache<UTF8Buffer, ActiveMQTopic>(DEFAULT_CACHE_SIZE);
    private final Map<Destination, UTF8Buffer> mqttTopicMap = new LRUCache<Destination, UTF8Buffer>(DEFAULT_CACHE_SIZE);
    private final Map<Short, MessageAck> consumerAcks = new LRUCache<Short, MessageAck>(DEFAULT_CACHE_SIZE);
    private final Map<Short, PUBREC> publisherRecs = new LRUCache<Short, PUBREC>(DEFAULT_CACHE_SIZE);

    private final MQTTTransport mqttTransport;
    private final BrokerService brokerService;

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final ConnectionInfo connectionInfo = new ConnectionInfo();
    private CONNECT connect;
    private String clientId;
    private long defaultKeepAlive;
    private int activeMQSubscriptionPrefetch=1;
    private final String QOS_PROPERTY_NAME = "QoSPropertyName";
    private final MQTTRetainedMessages retainedMessages;
    private final MQTTPacketIdGenerator packetIdGenerator;

    public MQTTProtocolConverter(MQTTTransport mqttTransport, BrokerService brokerService) {
        this.mqttTransport = mqttTransport;
        this.brokerService = brokerService;
        this.retainedMessages = MQTTRetainedMessages.getMQTTRetainedMessages(brokerService);
        this.packetIdGenerator = MQTTPacketIdGenerator.getMQTTPacketIdGenerator(brokerService);
        this.defaultKeepAlive = 0;
    }

    int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }

    void sendToActiveMQ(Command command, ResponseHandler handler) {

        // Lets intercept message send requests..
        if( command instanceof ActiveMQMessage) {
            ActiveMQMessage msg = (ActiveMQMessage) command;
            if( msg.getDestination().getPhysicalName().startsWith("$") ) {
                // We don't allow users to send to $ prefixed topics to avoid failing MQTT 3.1.1 spec requirements
                if( handler!=null ) {
                    try {
                        handler.onResponse(this, new Response());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return;
            }
        }

        command.setCommandId(generateCommandId());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(command.getCommandId(), handler);
        }
        getMQTTTransport().sendToActiveMQ(command);
    }

    void sendToMQTT(MQTTFrame frame) {
        try {
            mqttTransport.sendToMQTT(frame);
        } catch (IOException e) {
            LOG.warn("Failed to send frame " + frame, e);
        }
    }

    /**
     * Convert a MQTT command
     */
    public void onMQTTCommand(MQTTFrame frame) throws IOException, JMSException {

        switch (frame.messageType()) {
            case PINGREQ.TYPE: {
                LOG.debug("Received a ping from client: " + getClientId());
                sendToMQTT(PING_RESP_FRAME);
                LOG.debug("Sent Ping Response to " + getClientId());
                break;
            }
            case CONNECT.TYPE: {
                CONNECT connect = new CONNECT().decode(frame);
                onMQTTConnect(connect);
                LOG.debug("MQTT Client {} connected. (version: {})", getClientId(), connect.version());
                break;
            }
            case DISCONNECT.TYPE: {
                LOG.debug("MQTT Client {} disconnecting", getClientId());
                onMQTTDisconnect();
                break;
            }
            case SUBSCRIBE.TYPE: {
                onSubscribe(new SUBSCRIBE().decode(frame));
                break;
            }
            case UNSUBSCRIBE.TYPE: {
                onUnSubscribe(new UNSUBSCRIBE().decode(frame));
                break;
            }
            case PUBLISH.TYPE: {
                onMQTTPublish(new PUBLISH().decode(frame));
                break;
            }
            case PUBACK.TYPE: {
                onMQTTPubAck(new PUBACK().decode(frame));
                break;
            }
            case PUBREC.TYPE: {
                onMQTTPubRec(new PUBREC().decode(frame));
                break;
            }
            case PUBREL.TYPE: {
                onMQTTPubRel(new PUBREL().decode(frame));
                break;
            }
            case PUBCOMP.TYPE: {
                onMQTTPubComp(new PUBCOMP().decode(frame));
                break;
            }
            default: {
                handleException(new MQTTProtocolException("Unknown MQTTFrame type: " + frame.messageType(), true), frame);
            }
        }
    }

    void onMQTTConnect(final CONNECT connect) throws MQTTProtocolException {

        if (connected.get()) {
            throw new MQTTProtocolException("Already connected.");
        }
        this.connect = connect;

        String clientId = "";
        if (connect.clientId() != null) {
            clientId = connect.clientId().toString();
        }

        String userName = null;
        if (connect.userName() != null) {
            userName = connect.userName().toString();
        }
        String passswd = null;
        if (connect.password() != null) {
            passswd = connect.password().toString();
        }

        configureInactivityMonitor(connect.keepAlive());

        connectionInfo.setConnectionId(connectionId);
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        } else {
            // Clean Session MUST be set for 0 length Client Id
            if (!connect.cleanSession()) {
                CONNACK ack = new CONNACK();
                ack.code(CONNACK.Code.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                try {
                    getMQTTTransport().sendToMQTT(ack.encode());
                    getMQTTTransport().onException(IOExceptionSupport.create("Invalid Client ID", null));
                } catch (IOException e) {
                    getMQTTTransport().onException(IOExceptionSupport.create(e));
                }
                return;
            }
            connectionInfo.setClientId("" + connectionInfo.getConnectionId().toString());
        }

        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(userName);
        connectionInfo.setPassword(passswd);
        connectionInfo.setTransportContext(mqttTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
            @Override
            public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {

                if (response.isException()) {
                    // If the connection attempt fails we close the socket.
                    Throwable exception = ((ExceptionResponse) response).getException();
                    //let the client know
                    CONNACK ack = new CONNACK();
                    ack.code(CONNACK.Code.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                    getMQTTTransport().sendToMQTT(ack.encode());
                    getMQTTTransport().onException(IOExceptionSupport.create(exception));
                    return;
                }

                final SessionInfo sessionInfo = new SessionInfo(sessionId);
                sendToActiveMQ(sessionInfo, null);

                final ProducerInfo producerInfo = new ProducerInfo(producerId);
                sendToActiveMQ(producerInfo, new ResponseHandler() {
                    @Override
                    public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {

                        if (response.isException()) {
                            // If the connection attempt fails we close the socket.
                            Throwable exception = ((ExceptionResponse) response).getException();
                            CONNACK ack = new CONNACK();
                            ack.code(CONNACK.Code.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD);
                            getMQTTTransport().sendToMQTT(ack.encode());
                            getMQTTTransport().onException(IOExceptionSupport.create(exception));
                            return;
                        }

                        CONNACK ack = new CONNACK();
                        ack.code(CONNACK.Code.CONNECTION_ACCEPTED);
                        connected.set(true);
                        getMQTTTransport().sendToMQTT(ack.encode());

                        List<SubscriptionInfo> subs = PersistenceAdapterSupport.listSubscriptions(brokerService.getPersistenceAdapter(), connectionInfo.getClientId());
                        if( connect.cleanSession() ) {
                            packetIdGenerator.stopClientSession(getClientId());
                            deleteDurableSubs(subs);
                        } else {
                            packetIdGenerator.startClientSession(getClientId());
                            restoreDurableSubs(subs);
                        }
                    }
                });
            }
        });
    }

    public void deleteDurableSubs(List<SubscriptionInfo> subs) {
        try {
            for (SubscriptionInfo sub : subs) {
                RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
                rsi.setConnectionId(connectionId);
                rsi.setSubscriptionName(sub.getSubcriptionName());
                rsi.setClientId(sub.getClientId());
                sendToActiveMQ(rsi, new ResponseHandler() {
                    @Override
                    public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                        // ignore failures..
                    }
                });
            }
        } catch (Throwable e) {
            LOG.warn("Could not delete the MQTT durable subs.", e);
        }
    }

    public void restoreDurableSubs(List<SubscriptionInfo> subs) {
        try {
            for (SubscriptionInfo sub : subs) {
                String name = sub.getSubcriptionName();
                String[] split = name.split(":", 2);
                QoS qoS = QoS.valueOf(split[0]);
                onSubscribe(new Topic(split[1], qoS));
            }
        } catch (IOException e) {
            LOG.warn("Could not restore the MQTT durable subs.", e);
        }
    }

    void onMQTTDisconnect() throws MQTTProtocolException {
        if (connected.get()) {
            connected.set(false);
            sendToActiveMQ(connectionInfo.createRemoveCommand(), null);
            sendToActiveMQ(new ShutdownInfo(), null);
        }
        stopTransport();
    }

    void onSubscribe(SUBSCRIBE command) throws MQTTProtocolException {
        checkConnected();
        Topic[] topics = command.topics();
        if (topics != null) {
            byte[] qos = new byte[topics.length];
            for (int i = 0; i < topics.length; i++) {
                qos[i] = onSubscribe(topics[i]);
            }
            SUBACK ack = new SUBACK();
            ack.messageId(command.messageId());
            ack.grantedQos(qos);
            try {
                getMQTTTransport().sendToMQTT(ack.encode());
            } catch (IOException e) {
                LOG.warn("Couldn't send SUBACK for " + command, e);
            }
            // check retained messages
            for (int i = 0; i < topics.length; i++) {
                if (qos[i] == SUBSCRIBE_ERROR) {
                    // skip this topic if subscribe failed
                    continue;
                }
                final Topic topic = topics[i];
                ActiveMQTopic destination = new ActiveMQTopic(convertMQTTToActiveMQ(topic.name().toString()));
                for (PUBLISH msg : retainedMessages.getMessages(destination)) {
                    if( msg.payload().length > 0 ) {
                        try {
                            PUBLISH retainedCopy = new PUBLISH();
                            retainedCopy.topicName(msg.topicName());
                            retainedCopy.retain(msg.retain());
                            retainedCopy.payload(msg.payload());
                            // set QoS of retained message to maximum of subscription QoS
                            retainedCopy.qos(msg.qos().ordinal() > qos[i] ? QoS.values()[qos[i]] : msg.qos());
                            switch (retainedCopy.qos()) {
                                case AT_LEAST_ONCE:
                                case EXACTLY_ONCE:
                                    retainedCopy.messageId(packetIdGenerator.getNextSequenceId(getClientId()));
                                case AT_MOST_ONCE:
                            }
                            getMQTTTransport().sendToMQTT(retainedCopy.encode());
                        } catch (IOException e) {
                            LOG.warn("Couldn't send retained message " + msg, e);
                        }
                    }
                }
            }
        } else {
            LOG.warn("No topics defined for Subscription " + command);
        }

    }

    byte onSubscribe(final Topic topic) throws MQTTProtocolException {

        if( mqttSubscriptionByTopic.containsKey(topic.name())) {
            if (topic.qos() != mqttSubscriptionByTopic.get(topic.name()).qos()) {
                // remove old subscription as the QoS has changed
                onUnSubscribe(topic.name());
            } else {
                // duplicate SUBSCRIBE packet, nothing to do
                return (byte) topic.qos().ordinal();
            }
        }

        ActiveMQDestination destination = new ActiveMQTopic(convertMQTTToActiveMQ(topic.name().toString()));

        ConsumerId id = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setDestination(destination);
        consumerInfo.setPrefetchSize(getActiveMQSubscriptionPrefetch());
        consumerInfo.setDispatchAsync(true);
        // create durable subscriptions only when cleansession is false
        if ( !connect.cleanSession() && connect.clientId() != null && topic.qos().ordinal() >= QoS.AT_LEAST_ONCE.ordinal() ) {
            consumerInfo.setSubscriptionName(topic.qos()+":"+topic.name().toString());
        }
        MQTTSubscription mqttSubscription = new MQTTSubscription(this, topic.qos(), consumerInfo);

        final byte[] qos = {-1};
        sendToActiveMQ(consumerInfo, new ResponseHandler() {
            @Override
            public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                // validate subscription request
                if (response.isException()) {
                    final Throwable throwable = ((ExceptionResponse) response).getException();
                    LOG.warn("Error subscribing to " + topic.name(), throwable);
                    qos[0] = SUBSCRIBE_ERROR;
                } else {
                    qos[0] = (byte) topic.qos().ordinal();
                }
            }
        });

        if (qos[0] != SUBSCRIBE_ERROR) {
            subscriptionsByConsumerId.put(id, mqttSubscription);
            mqttSubscriptionByTopic.put(topic.name(), mqttSubscription);
        }

        return qos[0];
    }

    void onUnSubscribe(UNSUBSCRIBE command) throws MQTTProtocolException {
        checkConnected();
        UTF8Buffer[] topics = command.topics();
        if (topics != null) {
            for (UTF8Buffer topic : topics) {
                onUnSubscribe(topic);
            }
        }
        UNSUBACK ack = new UNSUBACK();
        ack.messageId(command.messageId());
        sendToMQTT(ack.encode());
    }

    void onUnSubscribe(UTF8Buffer topicName) {
        MQTTSubscription subs = mqttSubscriptionByTopic.remove(topicName);
        if (subs != null) {
            ConsumerInfo info = subs.getConsumerInfo();
            if (info != null) {
                subscriptionsByConsumerId.remove(info.getConsumerId());
            }
            RemoveInfo removeInfo = null;
            if (info != null) {
                removeInfo = info.createRemoveCommand();
            }
            sendToActiveMQ(removeInfo, null);
        }
    }

    /**
     * Dispatch a ActiveMQ command
     */
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
                    handleException(exception, null);
                }
            }
        } else if (command.isMessageDispatch()) {
            MessageDispatch md = (MessageDispatch) command;
            MQTTSubscription sub = subscriptionsByConsumerId.get(md.getConsumerId());
            if (sub != null) {
                MessageAck ack = sub.createMessageAck(md);
                PUBLISH publish = sub.createPublish((ActiveMQMessage) md.getMessage());
                switch (publish.qos()) {
                    case AT_LEAST_ONCE:
                    case EXACTLY_ONCE:
                        publish.dup(publish.dup() ? true : md.getMessage().isRedelivered());
                    case AT_MOST_ONCE:
                }
                if (ack != null && sub.expectAck(publish)) {
                    synchronized (consumerAcks) {
                        consumerAcks.put(publish.messageId(), ack);
                    }
                }
                getMQTTTransport().sendToMQTT(publish.encode());
                if (ack != null && !sub.expectAck(publish)) {
                    getMQTTTransport().sendToActiveMQ(ack);
                }
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError) command).getException();
            handleException(exception, null);
        } else if (command.isBrokerInfo()) {
            //ignore
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command " + command);
        }
    }

    void onMQTTPublish(PUBLISH command) throws IOException, JMSException {
        checkConnected();
        ActiveMQMessage message = convertMessage(command);
        if (command.retain()){
            retainedMessages.addMessage((ActiveMQTopic) message.getDestination(), command);
        }
        message.setProducerId(producerId);
        message.onSend();
        sendToActiveMQ(message, createResponseHandler(command));
    }

    void onMQTTPubAck(PUBACK command) {
        short messageId = command.messageId();
        packetIdGenerator.ackPacketId(getClientId(), messageId);
        MessageAck ack;
        synchronized (consumerAcks) {
            ack = consumerAcks.remove(messageId);
        }
        if (ack != null) {
            getMQTTTransport().sendToActiveMQ(ack);
        }
    }

    void onMQTTPubRec(PUBREC commnand) {
        //from a subscriber - send a PUBREL in response
        PUBREL pubrel = new PUBREL();
        pubrel.messageId(commnand.messageId());
        sendToMQTT(pubrel.encode());
    }

    void onMQTTPubRel(PUBREL command) {
        PUBREC ack;
        synchronized (publisherRecs) {
            ack = publisherRecs.remove(command.messageId());
        }
        if (ack == null) {
            LOG.warn("Unknown PUBREL: " + command.messageId() + " received");
        }
        PUBCOMP pubcomp = new PUBCOMP();
        pubcomp.messageId(command.messageId());
        sendToMQTT(pubcomp.encode());
    }

    void onMQTTPubComp(PUBCOMP command) {
        short messageId = command.messageId();
        packetIdGenerator.ackPacketId(getClientId(), messageId);
        MessageAck ack;
        synchronized (consumerAcks) {
            ack = consumerAcks.remove(messageId);
        }
        if (ack != null) {
            getMQTTTransport().sendToActiveMQ(ack);
        }
    }

    ActiveMQMessage convertMessage(PUBLISH command) throws JMSException {
        ActiveMQBytesMessage msg = new ActiveMQBytesMessage();

        msg.setProducerId(producerId);
        MessageId id = new MessageId(producerId, publisherIdGenerator.getNextSequenceId());
        msg.setMessageId(id);
        msg.setTimestamp(System.currentTimeMillis());
        msg.setPriority((byte) Message.DEFAULT_PRIORITY);
        msg.setPersistent(command.qos() != QoS.AT_MOST_ONCE && !command.retain());
        msg.setIntProperty(QOS_PROPERTY_NAME, command.qos().ordinal());

        ActiveMQTopic topic;
        synchronized (activeMQTopicMap) {
            topic = activeMQTopicMap.get(command.topicName());
            if (topic == null) {
                String topicName = convertMQTTToActiveMQ(command.topicName().toString());
                topic = new ActiveMQTopic(topicName);
                activeMQTopicMap.put(command.topicName(), topic);
            }
        }
        msg.setJMSDestination(topic);
        msg.writeBytes(command.payload().data, command.payload().offset, command.payload().length);
        return msg;
    }

    public PUBLISH convertMessage(ActiveMQMessage message) throws IOException, JMSException, DataFormatException {
        PUBLISH result = new PUBLISH();
        // packet id is set in MQTTSubscription
        QoS qoS;
        if (message.propertyExists(QOS_PROPERTY_NAME)) {
            int ordinal = message.getIntProperty(QOS_PROPERTY_NAME);
            qoS = QoS.values()[ordinal];

        } else {
            qoS = message.isPersistent() ? QoS.AT_MOST_ONCE : QoS.AT_LEAST_ONCE;
        }
        result.qos(qoS);

        UTF8Buffer topicName;
        synchronized (mqttTopicMap) {
            topicName = mqttTopicMap.get(message.getJMSDestination());
            if (topicName == null) {
                topicName = new UTF8Buffer(message.getDestination().getPhysicalName().replace('.', '/'));
                mqttTopicMap.put(message.getJMSDestination(), topicName);
            }
        }
        result.topicName(topicName);

        if (message.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
            ActiveMQTextMessage msg = (ActiveMQTextMessage) message.copy();
            msg.setReadOnlyBody(true);
            String messageText = msg.getText();
            if (messageText != null) {
                result.payload(new Buffer(messageText.getBytes("UTF-8")));
            }
        } else if (message.getDataStructureType() == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {
            ActiveMQBytesMessage msg = (ActiveMQBytesMessage) message.copy();
            msg.setReadOnlyBody(true);
            byte[] data = new byte[(int) msg.getBodyLength()];
            msg.readBytes(data);
            result.payload(new Buffer(data));
        } else if (message.getDataStructureType() == ActiveMQMapMessage.DATA_STRUCTURE_TYPE) {
            ActiveMQMapMessage msg = (ActiveMQMapMessage) message.copy();
            msg.setReadOnlyBody(true);
            Map<String, Object> map = msg.getContentMap();
            if (map != null) {
                result.payload(new Buffer(map.toString().getBytes("UTF-8")));
            }
        } else {
            ByteSequence byteSequence = message.getContent();
            if (byteSequence != null && byteSequence.getLength() > 0) {
                if (message.isCompressed()) {
                    Inflater inflater = new Inflater();
                    inflater.setInput(byteSequence.data, byteSequence.offset, byteSequence.length);
                    byte[] data = new byte[4096];
                    int read;
                    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                    while ((read = inflater.inflate(data)) != 0) {
                        bytesOut.write(data, 0, read);
                    }
                    byteSequence = bytesOut.toByteSequence();
                    bytesOut.close();
                }
                result.payload(new Buffer(byteSequence.data, byteSequence.offset, byteSequence.length));
            }
        }
        return result;
    }

    public MQTTTransport getMQTTTransport() {
        return mqttTransport;
    }

    boolean willSent = false;
    public void onTransportError() {
        if (connect != null) {
            if (connected.get()) {
                if (connect.willTopic() != null && connect.willMessage() != null && !willSent) {
                    willSent = true;
                    try {
                        PUBLISH publish = new PUBLISH();
                        publish.topicName(connect.willTopic());
                        publish.qos(connect.willQos());
                        publish.messageId(packetIdGenerator.getNextSequenceId(getClientId()));
                        publish.payload(connect.willMessage());
                        ActiveMQMessage message = convertMessage(publish);
                        message.setProducerId(producerId);
                        message.onSend();

                        sendToActiveMQ(message, null);
                    } catch (Exception e) {
                        LOG.warn("Failed to publish Will Message " + connect.willMessage());
                    }
                }
                // remove connection info
                sendToActiveMQ(connectionInfo.createRemoveCommand(), null);
            }
        }
    }

    void configureInactivityMonitor(short keepAliveSeconds) {
        MQTTInactivityMonitor monitor = getMQTTTransport().getInactivityMonitor();

        // If the user specifically shuts off the InactivityMonitor with transport.useInactivityMonitor=false,
        // then ignore configuring it because it won't exist
        if (monitor == null) {
            return;
        }

        long keepAliveMS = keepAliveSeconds * 1000;

        if (LOG.isDebugEnabled()) {
            LOG.debug("MQTT Client " + getClientId() + " requests heart beat of  " + keepAliveMS + " ms");
        }

        try {
            // if we have a default keep-alive value, and the client is trying to turn off keep-alive,

            // we'll observe the server-side configured default value (note, no grace period)
            if (keepAliveMS == 0 && defaultKeepAlive > 0) {
                keepAliveMS = defaultKeepAlive;
            }

            long readGracePeriod = (long) (keepAliveMS * MQTT_KEEP_ALIVE_GRACE_PERIOD);

            monitor.setProtocolConverter(this);
            monitor.setReadKeepAliveTime(keepAliveMS);
            monitor.setReadGraceTime(readGracePeriod);
            monitor.startMonitorThread();

            if (LOG.isDebugEnabled()) {
                LOG.debug("MQTT Client " + getClientId() +
                        " established heart beat of  " + keepAliveMS +
                        " ms (" + keepAliveMS + "ms + " + readGracePeriod +
                        "ms grace period)");
            }
        } catch (Exception ex) {
            LOG.warn("Failed to start MQTT InactivityMonitor ", ex);
        }
    }

    void handleException(Throwable exception, MQTTFrame command) {
        LOG.warn("Exception occurred processing: \n" + command + ": " + exception.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exception detail", exception);
        }

        if (connected.get() && connectionInfo != null) {
            connected.set(false);
            sendToActiveMQ(connectionInfo.createRemoveCommand(), null);
        }
        stopTransport();
    }

    void checkConnected() throws MQTTProtocolException {
        if (!connected.get()) {
            throw new MQTTProtocolException("Not connected.");
        }
    }

    String getClientId() {
        if (clientId == null) {
            if (connect != null && connect.clientId() != null) {
                clientId = connect.clientId().toString();
            }
            else {
                clientId = "";
            }
        }
        return clientId;
    }

    private void stopTransport() {
        try {
            getMQTTTransport().stop();
        } catch (Throwable e) {
            LOG.debug("Failed to stop MQTT transport ", e);
        }
    }

    ResponseHandler createResponseHandler(final PUBLISH command) {

        if (command != null) {
            switch (command.qos()) {
                case AT_LEAST_ONCE:
                    return new ResponseHandler() {
                        @Override
                        public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                            if (response.isException()) {
                                LOG.warn("Failed to send MQTT Publish: ", command, ((ExceptionResponse) response).getException());
                            } else {
                                PUBACK ack = new PUBACK();
                                ack.messageId(command.messageId());
                                converter.getMQTTTransport().sendToMQTT(ack.encode());
                            }
                        }
                    };
                case EXACTLY_ONCE:
                    return new ResponseHandler() {
                        @Override
                        public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                            if (response.isException()) {
                                LOG.warn("Failed to send MQTT Publish: ", command, ((ExceptionResponse) response).getException());
                            } else {
                                PUBREC ack = new PUBREC();
                                ack.messageId(command.messageId());
                                synchronized (publisherRecs) {
                                    publisherRecs.put(command.messageId(), ack);
                                }
                                converter.getMQTTTransport().sendToMQTT(ack.encode());
                            }
                        }
                    };
                case AT_MOST_ONCE:
                    break;
            }
        }
        return null;
    }

    private String convertMQTTToActiveMQ(String name) {
        char[] chars = name.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch(chars[i]) {

                case '#':
                    chars[i] = '>';
                    break;
                case '>':
                    chars[i] = '#';
                    break;

                case '+':
                    chars[i] = '*';
                    break;
                case '*':
                    chars[i] = '+';
                    break;

                case '/':
                    chars[i] = '.';
                    break;
                case '.':
                    chars[i] = '/';
                    break;

            }
        }
        String rc = new String(chars);
        return rc;
    }

    public long getDefaultKeepAlive() {
        return defaultKeepAlive;
    }

    /**
     * Set the default keep alive time (in milliseconds) that would be used if configured on server side
     * and the client sends a keep-alive value of 0 (zero) on a CONNECT frame
     * @param keepAlive the keepAlive in milliseconds
     */
    public void setDefaultKeepAlive(long keepAlive) {
        this.defaultKeepAlive = keepAlive;
    }

    public int getActiveMQSubscriptionPrefetch() {
        return activeMQSubscriptionPrefetch;
    }

    /**
     * set the default prefetch size when mapping the MQTT subscription to an ActiveMQ one
     * The default = 1
     * @param activeMQSubscriptionPrefetch set the prefetch for the corresponding ActiveMQ subscription
     */

    public void setActiveMQSubscriptionPrefetch(int activeMQSubscriptionPrefetch) {
        this.activeMQSubscriptionPrefetch = activeMQSubscriptionPrefetch;
    }

    public MQTTPacketIdGenerator getPacketIdGenerator() {
        return packetIdGenerator;
    }
}
