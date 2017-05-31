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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.jms.Destination;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.security.auth.login.CredentialException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.region.policy.RetainedMessageSubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.mqtt.strategy.MQTTSubscriptionStrategy;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.LongSequenceGenerator;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PINGRESP;
import org.fusesource.mqtt.codec.PUBACK;
import org.fusesource.mqtt.codec.PUBCOMP;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.PUBREC;
import org.fusesource.mqtt.codec.PUBREL;
import org.fusesource.mqtt.codec.SUBACK;
import org.fusesource.mqtt.codec.SUBSCRIBE;
import org.fusesource.mqtt.codec.UNSUBACK;
import org.fusesource.mqtt.codec.UNSUBSCRIBE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTProtocolConverter.class);

    public static final String QOS_PROPERTY_NAME = "ActiveMQ.MQTT.QoS";
    public static final int V3_1 = 3;
    public static final int V3_1_1 = 4;

    public static final String SINGLE_LEVEL_WILDCARD = "+";
    public static final String MULTI_LEVEL_WILDCARD = "#";

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private static final MQTTFrame PING_RESP_FRAME = new PINGRESP().encode();
    private static final double MQTT_KEEP_ALIVE_GRACE_PERIOD = 0.5;
    static final int DEFAULT_CACHE_SIZE = 5000;

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);
    private final LongSequenceGenerator publisherIdGenerator = new LongSequenceGenerator();

    private final ConcurrentMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final Map<String, ActiveMQDestination> activeMQDestinationMap = new LRUCache<String, ActiveMQDestination>(DEFAULT_CACHE_SIZE);
    private final Map<Destination, String> mqttTopicMap = new LRUCache<Destination, String>(DEFAULT_CACHE_SIZE);

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
    private int activeMQSubscriptionPrefetch = -1;
    private final MQTTPacketIdGenerator packetIdGenerator;
    private boolean publishDollarTopics;

    public int version;

    private final FactoryFinder STRATAGY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/strategies/");

    /*
     * Subscription strategy configuration element.
     *   > mqtt-default-subscriptions
     *   > mqtt-virtual-topic-subscriptions
     */
    private String subscriptionStrategyName = "mqtt-default-subscriptions";
    private MQTTSubscriptionStrategy subsciptionStrategy;

    public MQTTProtocolConverter(MQTTTransport mqttTransport, BrokerService brokerService) {
        this.mqttTransport = mqttTransport;
        this.brokerService = brokerService;
        this.packetIdGenerator = MQTTPacketIdGenerator.getMQTTPacketIdGenerator(brokerService);
        this.defaultKeepAlive = 0;
    }

    int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }

    public void sendToActiveMQ(Command command, ResponseHandler handler) {

        // Lets intercept message send requests..
        if (command instanceof ActiveMQMessage) {
            ActiveMQMessage msg = (ActiveMQMessage) command;
            try {
                if (!getPublishDollarTopics() && findSubscriptionStrategy().isControlTopic(msg.getDestination())) {
                    // We don't allow users to send to $ prefixed topics to avoid failing MQTT 3.1.1
                    // specification requirements for system assigned destinations.
                    if (handler != null) {
                        try {
                            handler.onResponse(this, new Response());
                        } catch (IOException e) {
                            LOG.warn("Failed to send command " + command, e);
                        }
                    }
                    return;
                }
            } catch (IOException e) {
                LOG.warn("Failed to send command " + command, e);
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
            case PINGREQ.TYPE:
                LOG.debug("Received a ping from client: " + getClientId());
                checkConnected();
                sendToMQTT(PING_RESP_FRAME);
                LOG.debug("Sent Ping Response to " + getClientId());
                break;
            case CONNECT.TYPE:
                CONNECT connect = new CONNECT().decode(frame);
                onMQTTConnect(connect);
                LOG.debug("MQTT Client {} connected. (version: {})", getClientId(), connect.version());
                break;
            case DISCONNECT.TYPE:
                LOG.debug("MQTT Client {} disconnecting", getClientId());
                onMQTTDisconnect();
                break;
            case SUBSCRIBE.TYPE:
                onSubscribe(new SUBSCRIBE().decode(frame));
                break;
            case UNSUBSCRIBE.TYPE:
                onUnSubscribe(new UNSUBSCRIBE().decode(frame));
                break;
            case PUBLISH.TYPE:
                onMQTTPublish(new PUBLISH().decode(frame));
                break;
            case PUBACK.TYPE:
                onMQTTPubAck(new PUBACK().decode(frame));
                break;
            case PUBREC.TYPE:
                onMQTTPubRec(new PUBREC().decode(frame));
                break;
            case PUBREL.TYPE:
                onMQTTPubRel(new PUBREL().decode(frame));
                break;
            case PUBCOMP.TYPE:
                onMQTTPubComp(new PUBCOMP().decode(frame));
                break;
            default:
                handleException(new MQTTProtocolException("Unknown MQTTFrame type: " + frame.messageType(), true), frame);
        }
    }

    void onMQTTConnect(final CONNECT connect) throws MQTTProtocolException {
        if (connected.get()) {
            throw new MQTTProtocolException("Already connected.");
        }
        this.connect = connect;

        // The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01
        // (unacceptable protocol level) and then disconnect the Client if the Protocol Level
        // is not supported by the Server [MQTT-3.1.2-2].
        if (connect.version() < 3 || connect.version() > 4) {
            CONNACK ack = new CONNACK();
            ack.code(CONNACK.Code.CONNECTION_REFUSED_UNACCEPTED_PROTOCOL_VERSION);
            try {
                getMQTTTransport().sendToMQTT(ack.encode());
                getMQTTTransport().onException(IOExceptionSupport.create("Unsupported or invalid protocol version", null));
            } catch (IOException e) {
                getMQTTTransport().onException(IOExceptionSupport.create(e));
            }
            return;
        }

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

            if (userName == null && connect.version() != V3_1) {
                // [MQTT-3.1.2-22]: If the user name is not present then the
                // password must also be absent.
                // [MQTT-3.1.4-1]: would seem to imply we don't send a CONNACK here.
                getMQTTTransport().onException(IOExceptionSupport.create("Password given without a user name", null));
                return;
            }

            passswd = connect.password().toString();
        }

        version = connect.version();

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
                    if (exception instanceof InvalidClientIDException) {
                        ack.code(CONNACK.Code.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                    } else if (exception instanceof SecurityException) {
                        ack.code(CONNACK.Code.CONNECTION_REFUSED_NOT_AUTHORIZED);
                    } else if (exception instanceof CredentialException) {
                        ack.code(CONNACK.Code.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD);
                    } else {
                        ack.code(CONNACK.Code.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                    }
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

                        if (connect.cleanSession()) {
                            packetIdGenerator.stopClientSession(getClientId());
                        } else {
                            packetIdGenerator.startClientSession(getClientId());
                        }

                        findSubscriptionStrategy().onConnect(connect);
                    }
                });
            }
        });
    }

    void onMQTTDisconnect() throws MQTTProtocolException {
        if (connected.compareAndSet(true, false)) {
            sendToActiveMQ(connectionInfo.createRemoveCommand(), null);
            sendToActiveMQ(new ShutdownInfo(), null);
        }
        stopTransport();
    }

    void onSubscribe(SUBSCRIBE command) throws MQTTProtocolException {
        checkConnected();
        LOG.trace("MQTT SUBSCRIBE message:{} client:{} connection:{}",
                  command.messageId(), clientId, connectionInfo.getConnectionId());
        Topic[] topics = command.topics();
        if (topics != null) {
            byte[] qos = new byte[topics.length];
            for (int i = 0; i < topics.length; i++) {
                MQTTProtocolSupport.validate(topics[i].name().toString());
                try {
                    qos[i] = findSubscriptionStrategy().onSubscribe(topics[i]);
                } catch (IOException e) {
                    throw new MQTTProtocolException("Failed to process subscription request", true, e);
                }
            }
            SUBACK ack = new SUBACK();
            ack.messageId(command.messageId());
            ack.grantedQos(qos);
            try {
                getMQTTTransport().sendToMQTT(ack.encode());
            } catch (IOException e) {
                LOG.warn("Couldn't send SUBACK for " + command, e);
            }
        } else {
            LOG.warn("No topics defined for Subscription " + command);
            throw new MQTTProtocolException("SUBSCRIBE command received with no topic filter");
        }
    }

    public void onUnSubscribe(UNSUBSCRIBE command) throws MQTTProtocolException {
        checkConnected();
        if (command.qos() != QoS.AT_LEAST_ONCE && (version != V3_1 || publishDollarTopics != true)) {
            throw new MQTTProtocolException("Failed to process unsubscribe request", true, new Exception("UNSUBSCRIBE frame not properly formatted, QoS"));
        }
        UTF8Buffer[] topics = command.topics();
        if (topics != null) {
            for (UTF8Buffer topic : topics) {
                MQTTProtocolSupport.validate(topic.toString());
                try {
                    findSubscriptionStrategy().onUnSubscribe(topic.toString());
                } catch (IOException e) {
                    throw new MQTTProtocolException("Failed to process unsubscribe request", true, e);
                }
            }
            UNSUBACK ack = new UNSUBACK();
            ack.messageId(command.messageId());
            sendToMQTT(ack.encode());
        } else {
            LOG.warn("No topics defined for Subscription " + command);
            throw new MQTTProtocolException("UNSUBSCRIBE command received with no topic filter");
        }
    }

    /**
     * Dispatch an ActiveMQ command
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
            MQTTSubscription sub = findSubscriptionStrategy().getSubscription(md.getConsumerId());
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
                LOG.trace("MQTT Snd PUBLISH message:{} client:{} connection:{}",
                          publish.messageId(), clientId, connectionInfo.getConnectionId());
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
            LOG.debug("Do not know how to process ActiveMQ Command {}", command);
        }
    }

    void onMQTTPublish(PUBLISH command) throws IOException, JMSException {
        checkConnected();
        LOG.trace("MQTT Rcv PUBLISH message:{} client:{} connection:{}",
                  command.messageId(), clientId, connectionInfo.getConnectionId());
        //Both version 3.1 and 3.1.1 do not allow the topic name to contain a wildcard in the publish packet
        if (containsMqttWildcard(command.topicName().toString())) {
            // [MQTT-3.3.2-2]: The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters
            getMQTTTransport().onException(IOExceptionSupport.create("The topic name must not contain wildcard characters.", null));
            return;
        }
        ActiveMQMessage message = convertMessage(command);
        message.setProducerId(producerId);
        message.onSend();
        sendToActiveMQ(message, createResponseHandler(command));
    }

    void onMQTTPubAck(PUBACK command) {
        short messageId = command.messageId();
        LOG.trace("MQTT Rcv PUBACK message:{} client:{} connection:{}",
                  messageId, clientId, connectionInfo.getConnectionId());
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
            LOG.warn("Unknown PUBREL: {} received", command.messageId());
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
        LOG.trace("MQTT-->ActiveMQ: MQTT_MSGID:{} client:{} connection:{} ActiveMQ_MSGID:{}",
                command.messageId(), clientId, connectionInfo.getConnectionId(), msg.getMessageId());
        msg.setTimestamp(System.currentTimeMillis());
        msg.setPriority((byte) Message.DEFAULT_PRIORITY);
        msg.setPersistent(command.qos() != QoS.AT_MOST_ONCE && !command.retain());
        msg.setIntProperty(QOS_PROPERTY_NAME, command.qos().ordinal());
        if (command.retain()) {
            msg.setBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAIN_PROPERTY, true);
        }

        ActiveMQDestination destination;
        synchronized (activeMQDestinationMap) {
            destination = activeMQDestinationMap.get(command.topicName());
            if (destination == null) {
                String topicName = MQTTProtocolSupport.convertMQTTToActiveMQ(command.topicName().toString());
                try {
                    destination = findSubscriptionStrategy().onSend(topicName);
                } catch (IOException e) {
                    throw JMSExceptionSupport.create(e);
                }

                activeMQDestinationMap.put(command.topicName().toString(), destination);
            }
        }

        msg.setJMSDestination(destination);
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
        if (message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY)) {
            result.retain(true);
        }

        String topicName;
        synchronized (mqttTopicMap) {
            topicName = mqttTopicMap.get(message.getJMSDestination());
            if (topicName == null) {
                String amqTopicName = findSubscriptionStrategy().onSend(message.getDestination());
                topicName = MQTTProtocolSupport.convertActiveMQToMQTT(amqTopicName);
                mqttTopicMap.put(message.getJMSDestination(), topicName);
            }
        }
        result.topicName(new UTF8Buffer(topicName));

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
        LOG.trace("ActiveMQ-->MQTT:MQTT_MSGID:{} client:{} connection:{} ActiveMQ_MSGID:{}",
                result.messageId(), clientId, connectionInfo.getConnectionId(), message.getMessageId());
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
                        publish.retain(connect.willRetain());
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

        // Client has sent a valid CONNECT frame, we can stop the connect checker.
        monitor.stopConnectChecker();

        long keepAliveMS = keepAliveSeconds * 1000;

        LOG.debug("MQTT Client {} requests heart beat of {} ms", getClientId(), keepAliveMS);

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
            monitor.startReadChecker();

            LOG.debug("MQTT Client {} established heart beat of  {} ms ({} ms + {} ms grace period)",
                      new Object[] { getClientId(), keepAliveMS, keepAliveMS, readGracePeriod });
        } catch (Exception ex) {
            LOG.warn("Failed to start MQTT InactivityMonitor ", ex);
        }
    }

    void handleException(Throwable exception, MQTTFrame command) {
        LOG.warn("Exception occurred processing: \n" + command + ": " + exception.toString());
        LOG.debug("Exception detail", exception);

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

    private void stopTransport() {
        try {
            getMQTTTransport().stop();
        } catch (Throwable e) {
            LOG.debug("Failed to stop MQTT transport ", e);
        }
    }

    ResponseHandler createResponseHandler(final PUBLISH command) {
        if (command != null) {
            return new ResponseHandler() {
                @Override
                public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        Throwable error = ((ExceptionResponse) response).getException();
                        LOG.warn("Failed to send MQTT Publish: ", command, error.getMessage());
                        LOG.trace("Error trace: {}", error);
                    }

                    switch (command.qos()) {
                        case AT_LEAST_ONCE:
                            PUBACK ack = new PUBACK();
                            ack.messageId(command.messageId());
                            LOG.trace("MQTT Snd PUBACK message:{} client:{} connection:{}",
                                      command.messageId(), clientId, connectionInfo.getConnectionId());
                            converter.getMQTTTransport().sendToMQTT(ack.encode());
                            break;
                        case EXACTLY_ONCE:
                            PUBREC req = new PUBREC();
                            req.messageId(command.messageId());
                            synchronized (publisherRecs) {
                                publisherRecs.put(command.messageId(), req);
                            }
                            LOG.trace("MQTT Snd PUBREC message:{} client:{} connection:{}",
                                      command.messageId(), clientId, connectionInfo.getConnectionId());
                            converter.getMQTTTransport().sendToMQTT(req.encode());
                            break;
                        default:
                            break;
                    }
                }
            };
        }
        return null;
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
     *
     * @param activeMQSubscriptionPrefetch
     *        set the prefetch for the corresponding ActiveMQ subscription
     */
    public void setActiveMQSubscriptionPrefetch(int activeMQSubscriptionPrefetch) {
        this.activeMQSubscriptionPrefetch = activeMQSubscriptionPrefetch;
    }

    public MQTTPacketIdGenerator getPacketIdGenerator() {
        return packetIdGenerator;
    }

    public void setPublishDollarTopics(boolean publishDollarTopics) {
        this.publishDollarTopics = publishDollarTopics;
    }

    public boolean getPublishDollarTopics() {
        return publishDollarTopics;
    }

    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public SessionId getSessionId() {
        return sessionId;
    }

    public boolean isCleanSession() {
        return this.connect.cleanSession();
    }

    public String getSubscriptionStrategy() {
        return subscriptionStrategyName;
    }

    public void setSubscriptionStrategy(String name) {
        this.subscriptionStrategyName = name;
    }

    public String getClientId() {
        if (clientId == null) {
            if (connect != null && connect.clientId() != null) {
                clientId = connect.clientId().toString();
            } else {
                clientId = "";
            }
        }
        return clientId;
    }

    protected boolean containsMqttWildcard(String value) {
        return value != null && (value.contains(SINGLE_LEVEL_WILDCARD) ||
                value.contains(MULTI_LEVEL_WILDCARD));
    }

    protected MQTTSubscriptionStrategy findSubscriptionStrategy() throws IOException {
        if (subsciptionStrategy == null) {
            synchronized (STRATAGY_FINDER) {
                if (subsciptionStrategy != null) {
                    return subsciptionStrategy;
                }

                MQTTSubscriptionStrategy strategy = null;
                if (subscriptionStrategyName != null && !subscriptionStrategyName.isEmpty()) {
                    try {
                        strategy = (MQTTSubscriptionStrategy) STRATAGY_FINDER.newInstance(subscriptionStrategyName);
                        LOG.debug("MQTT Using subscription strategy: {}", subscriptionStrategyName);
                        if (strategy instanceof BrokerServiceAware) {
                            ((BrokerServiceAware)strategy).setBrokerService(brokerService);
                        }
                        strategy.initialize(this);
                    } catch (Exception e) {
                        throw IOExceptionSupport.create(e);
                    }
                } else {
                    throw new IOException("Invalid subscription strategy name given: " + subscriptionStrategyName);
                }

                this.subsciptionStrategy = strategy;
            }
        }
        return subsciptionStrategy;
    }
}
