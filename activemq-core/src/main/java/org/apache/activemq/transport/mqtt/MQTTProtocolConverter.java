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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.*;
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

class MQTTProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTProtocolConverter.class);

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();
    private static final MQTTFrame PING_RESP_FRAME = new PINGRESP().encode();


    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();

    private final ConcurrentHashMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final ConcurrentHashMap<ConsumerId, MQTTSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, MQTTSubscription>();
    private final ConcurrentHashMap<UTF8Buffer, MQTTSubscription> mqttSubscriptionByTopic = new ConcurrentHashMap<UTF8Buffer, MQTTSubscription>();
    private final Map<UTF8Buffer, ActiveMQTopic> activeMQTopicMap = new LRUCache<UTF8Buffer, ActiveMQTopic>();
    private final Map<Destination, UTF8Buffer> mqttTopicMap = new LRUCache<Destination, UTF8Buffer>();
    private final Map<Short, MessageAck> consumerAcks = new LRUCache<Short, MessageAck>();
    private final Map<Short, PUBREC> publisherRecs = new LRUCache<Short, PUBREC>();
    private final MQTTTransport mqttTransport;

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private ConnectionInfo connectionInfo = new ConnectionInfo();
    private CONNECT connect;
    private String clientId;
    private final String QOS_PROPERTY_NAME = "QoSPropertyName";

    public MQTTProtocolConverter(MQTTTransport mqttTransport, BrokerContext brokerContext) {
        this.mqttTransport = mqttTransport;
    }

    int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }


    void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(generateCommandId());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(Integer.valueOf(command.getCommandId()), handler);
        }
        mqttTransport.sendToActiveMQ(command);
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
                mqttTransport.sendToMQTT(PING_RESP_FRAME);
                LOG.debug("Sent Ping Response to " + getClientId());
                break;
            }
            case CONNECT.TYPE: {
                onMQTTConnect(new CONNECT().decode(frame));
                LOG.debug("MQTT Client " + getClientId() + " connected.");
                break;
            }
            case DISCONNECT.TYPE: {
                LOG.debug("MQTT Client " + getClientId() + " disconnecting");
                stopTransport();
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
            default:
                handleException(new MQTTProtocolException("Unknown MQTTFrame type: " + frame.messageType(), true), frame);
        }

    }


    void onMQTTConnect(final CONNECT connect) throws MQTTProtocolException {

        if (connected.get()) {
            throw new MQTTProtocolException("All ready connected.");
        }
        this.connect = connect;

        String clientId = "";
        if (connect.clientId() != null) {
            clientId = connect.clientId().toString();
        }

        String userName = "";
        if (connect.userName() != null) {
            userName = connect.userName().toString();
        }
        String passswd = "";
        if (connect.password() != null) {
            passswd = connect.password().toString();
        }

        configureInactivityMonitor(connect.keepAlive());


        connectionInfo.setConnectionId(connectionId);
        if (clientId != null && !clientId.isEmpty()) {
            connectionInfo.setClientId(clientId);
        } else {
            connectionInfo.setClientId("" + connectionInfo.getConnectionId().toString());
        }

        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(userName);
        connectionInfo.setPassword(passswd);
        connectionInfo.setTransportContext(mqttTransport.getPeerCertificates());

        sendToActiveMQ(connectionInfo, new ResponseHandler() {
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
                    public void onResponse(MQTTProtocolConverter converter, Response response) throws IOException {

                        if (response.isException()) {
                            // If the connection attempt fails we close the socket.
                            Throwable exception = ((ExceptionResponse) response).getException();
                            CONNACK ack = new CONNACK();
                            ack.code(CONNACK.Code.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD);
                            getMQTTTransport().sendToMQTT(ack.encode());
                            getMQTTTransport().onException(IOExceptionSupport.create(exception));
                        }

                        CONNACK ack = new CONNACK();
                        ack.code(CONNACK.Code.CONNECTION_ACCEPTED);
                        connected.set(true);
                        getMQTTTransport().sendToMQTT(ack.encode());

                    }
                });

            }
        });
    }

    void onSubscribe(SUBSCRIBE command) throws MQTTProtocolException {
        checkConnected();
        SUBACK result = new SUBACK();
        Topic[] topics = command.topics();
        if (topics != null) {
            byte[] qos = new byte[topics.length];
            for (int i = 0; i < topics.length; i++) {
                qos[i] = (byte) onSubscribe(command, topics[i]).ordinal();
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
        }
    }

    QoS onSubscribe(SUBSCRIBE command, Topic topic) throws MQTTProtocolException {
        ActiveMQDestination destination = new ActiveMQTopic(convertMQTTToActiveMQ(topic.name().toString()));


        if (destination == null) {
            throw new MQTTProtocolException("Invalid Destination.");
        }

        ConsumerId id = new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo(id);
        consumerInfo.setDestination(destination);
        consumerInfo.setPrefetchSize(1000);
        consumerInfo.setDispatchAsync(true);
        if (!connect.cleanSession() && (connect.clientId() != null)) {
            //by default subscribers are persistent
            consumerInfo.setSubscriptionName(connect.clientId().toString());
        }

        MQTTSubscription mqttSubscription = new MQTTSubscription(this, topic.qos(), consumerInfo);


        subscriptionsByConsumerId.put(id, mqttSubscription);
        mqttSubscriptionByTopic.put(topic.name(), mqttSubscription);

        sendToActiveMQ(consumerInfo, null);
        return topic.qos();
    }

    void onUnSubscribe(UNSUBSCRIBE command) {
        UTF8Buffer[] topics = command.topics();
        if (topics != null) {
            for (int i = 0; i < topics.length; i++) {
                onUnSubscribe(topics[i]);
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
            RemoveInfo removeInfo = info.createRemoveCommand();
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
                if (ack != null && sub.expectAck()) {
                    synchronized (consumerAcks) {
                        consumerAcks.put(publish.messageId(), ack);
                    }
                }
                getMQTTTransport().sendToMQTT(publish.encode());
                if (ack != null && !sub.expectAck()) {
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
        message.setProducerId(producerId);
        message.onSend();
        sendToActiveMQ(message, createResponseHandler(command));
    }

    void onMQTTPubAck(PUBACK command) {
        short messageId = command.messageId();
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
        MessageId id = new MessageId(producerId, messageIdGenerator.getNextSequenceId());
        msg.setMessageId(id);
        msg.setTimestamp(System.currentTimeMillis());
        msg.setPriority((byte) Message.DEFAULT_PRIORITY);
        msg.setPersistent(command.qos() != QoS.AT_MOST_ONCE);
        msg.setIntProperty(QOS_PROPERTY_NAME, command.qos().ordinal());

        ActiveMQTopic topic = null;
        synchronized (activeMQTopicMap) {
            topic = activeMQTopicMap.get(command.topicName());
            if (topic == null) {
                String topicName = command.topicName().toString().replaceAll("/", ".");
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
        short id = (short) message.getMessageId().getProducerSequenceId();
        result.messageId(id);
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

        ByteSequence byteSequence = message.getContent();
        if (message.isCompressed()) {
            Inflater inflater = new Inflater();
            inflater.setInput(byteSequence.data, byteSequence.offset, byteSequence.length);
            byte[] data = new byte[4096];
            int read;
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            while ((read = inflater.inflate(data, 0, data.length)) != 0) {
                bytesOut.write(data, 0, read);
            }
            byteSequence = bytesOut.toByteSequence();
        }

        if (message.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
            if (byteSequence.getLength() > 4) {
                byte[] content = new byte[byteSequence.getLength() - 4];
                System.arraycopy(byteSequence.data, 4, content, 0, content.length);
                result.payload(new Buffer(content));
            } else {
                ActiveMQTextMessage msg = (ActiveMQTextMessage) message.copy();
                String messageText = msg.getText();
                if (messageText != null) {
                    result.payload(new Buffer(msg.getText().getBytes("UTF-8")));
                }
            }

        } else if (message.getDataStructureType() == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {

            ActiveMQBytesMessage msg = (ActiveMQBytesMessage) message.copy();
            msg.setReadOnlyBody(true);
            byte[] data = new byte[(int) msg.getBodyLength()];
            msg.readBytes(data);
            result.payload(new Buffer(data));
        } else {
            if (byteSequence != null && byteSequence.getLength() > 0) {
                result.payload(new Buffer(byteSequence.data, byteSequence.offset, byteSequence.length));
            }
        }
        return result;
    }


    public MQTTTransport getMQTTTransport() {
        return mqttTransport;
    }

    public void onTransportError() {
        if (connect != null) {
            if (connect.willTopic() != null && connect.willMessage() != null) {
                try {
                    PUBLISH publish = new PUBLISH();
                    publish.topicName(connect.willTopic());
                    publish.qos(connect.willQos());
                    publish.payload(connect.willMessage());
                    ActiveMQMessage message = convertMessage(publish);
                    message.setProducerId(producerId);
                    message.onSend();
                    sendToActiveMQ(message, null);
                } catch (Exception e) {
                    LOG.warn("Failed to publish Will Message " + connect.willMessage());
                }

            }
        }
    }


    void configureInactivityMonitor(short heartBeat) {
        try {

            int heartBeatMS = heartBeat * 1000;
            MQTTInactivityMonitor monitor = getMQTTTransport().getInactivityMonitor();
            monitor.setProtocolConverter(this);
            monitor.setReadCheckTime(heartBeatMS);
            monitor.setInitialDelayTime(heartBeatMS);
            monitor.startMonitorThread();

        } catch (Exception ex) {
            LOG.warn("Failed to start MQTT InactivityMonitor ", ex);
        }

        LOG.debug(getClientId() + " MQTT Connection using heart beat of  " + heartBeat + " secs");
    }


    void handleException(Throwable exception, MQTTFrame command) {
        LOG.warn("Exception occurred processing: \n" + command + ": " + exception.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Exception detail", exception);
        }

        try {
            getMQTTTransport().stop();
        } catch (Throwable e) {
            LOG.error("Failed to stop MQTTT Transport ", e);
        }
    }

    void checkConnected() throws MQTTProtocolException {
        if (!connected.get()) {
            throw new MQTTProtocolException("Not connected.");
        }
    }

    private String getClientId() {
        if (clientId == null) {
            if (connect != null && connect.clientId() != null) {
                clientId = connect.clientId().toString();
            }
        } else {
            clientId = "";
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
        String result = name.replace('>', '#');
        result = result.replace('*', '+');
        result = result.replace('.', '/');
        return result;
    }
}
