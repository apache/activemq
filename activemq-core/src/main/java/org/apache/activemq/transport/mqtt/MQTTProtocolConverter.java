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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Destination;
import javax.jms.JMSException;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.command.*;
import org.apache.activemq.transport.stomp.FrameTranslator;
import org.apache.activemq.transport.stomp.LegacyFrameTranslator;
import org.apache.activemq.transport.stomp.ProtocolException;
import org.apache.activemq.transport.stomp.StompSubscription;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.LongSequenceGenerator;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PINGRESP;
import org.fusesource.mqtt.codec.PUBLISH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MQTTProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTProtocolConverter.class);

    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    private static final String BROKER_VERSION;
    private static final MQTTFrame PING_RESP_FRAME = new PINGRESP().encode();

    static {
        InputStream in = null;
        String version = "5.6.0";
        if ((in = MQTTProtocolConverter.class.getResourceAsStream("/org/apache/activemq/version.txt")) != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            try {
                version = reader.readLine();
            } catch (Exception e) {
            }
        }
        BROKER_VERSION = version;
    }

    private final ConnectionId connectionId = new ConnectionId(CONNECTION_ID_GENERATOR.generateId());
    private final SessionId sessionId = new SessionId(connectionId, -1);
    private final ProducerId producerId = new ProducerId(sessionId, 1);

    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator transactionIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator tempDestinationGenerator = new LongSequenceGenerator();

    private final ConcurrentHashMap<Integer, ResponseHandler> resposeHandlers = new ConcurrentHashMap<Integer, ResponseHandler>();
    private final ConcurrentHashMap<ConsumerId, StompSubscription> subscriptionsByConsumerId = new ConcurrentHashMap<ConsumerId, StompSubscription>();
    private final ConcurrentHashMap<String, StompSubscription> subscriptions = new ConcurrentHashMap<String, StompSubscription>();
    private final ConcurrentHashMap<String, ActiveMQDestination> tempDestinations = new ConcurrentHashMap<String, ActiveMQDestination>();
    private final ConcurrentHashMap<String, String> tempDestinationAmqToStompMap = new ConcurrentHashMap<String, String>();
    private final Map<String, LocalTransactionId> transactions = new ConcurrentHashMap<String, LocalTransactionId>();
    private final Map<UTF8Buffer, ActiveMQTopic> activeMQTopicMap = new LRUCache<UTF8Buffer, ActiveMQTopic>();
    private final Map<Destination, UTF8Buffer> mqttTopicMap = new LRUCache<Destination, UTF8Buffer>();
    private final MQTTTransport mqttTransport;

    private final Object commnadIdMutex = new Object();
    private int lastCommandId;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final FrameTranslator frameTranslator = new LegacyFrameTranslator();
    private final FactoryFinder FRAME_TRANSLATOR_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/frametranslator/");
    private final BrokerContext brokerContext;
    private String version = "1.0";
    ConnectionInfo connectionInfo = new ConnectionInfo();
    private CONNECT connect;
    private String clientId;

    public MQTTProtocolConverter(MQTTTransport mqttTransport, BrokerContext brokerContext) {
        this.mqttTransport = mqttTransport;
        this.brokerContext = brokerContext;
    }

    protected int generateCommandId() {
        synchronized (commnadIdMutex) {
            return lastCommandId++;
        }
    }


    protected void sendToActiveMQ(Command command, ResponseHandler handler) {
        command.setCommandId(generateCommandId());
        if (handler != null) {
            command.setResponseRequired(true);
            resposeHandlers.put(Integer.valueOf(command.getCommandId()), handler);
        }
        mqttTransport.sendToActiveMQ(command);
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
                break;
            }
            case DISCONNECT.TYPE: {
                LOG.debug("MQTT Client " + getClientId() + " disconnecting");
                stopTransport();
                break;
            }
            default:
                handleException(new MQTTProtocolException("Unknown MQTTFrame type: " + frame.messageType(), true), frame);
        }

    }


    protected void onMQTTConnect(final CONNECT connect) throws ProtocolException {

        if (connected.get()) {
            throw new ProtocolException("All ready connected.");
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
        if (clientId != null && clientId.isEmpty() == false) {
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
                        getMQTTTransport().sendToMQTT(ack.encode());

                    }
                });

            }
        });
    }


    /**
     * Dispatch a ActiveMQ command
     */


    public void onActiveMQCommand(Command command) throws IOException, JMSException {
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
            StompSubscription sub = subscriptionsByConsumerId.get(md.getConsumerId());
            if (sub != null) {
                //sub.onMessageDispatch(md);
            }
        } else if (command.getDataStructureType() == ConnectionError.DATA_STRUCTURE_TYPE) {
            // Pass down any unexpected async errors. Should this close the connection?
            Throwable exception = ((ConnectionError) command).getException();
            handleException(exception, null);
        } else {
            LOG.debug("Do not know how to process ActiveMQ Command " + command);
        }
    }


    public ActiveMQMessage convertMessage(PUBLISH command) throws IOException, JMSException {
        ActiveMQBytesMessage msg = new ActiveMQBytesMessage();
        StringBuilder msgId = new StringBuilder();
        msgId.append("ID:").append(getClientId()).append(":").append(command.messageId());
        msg.setJMSMessageID(msgId.toString());
        msg.setJMSPriority(4);

        //ActiveMQTopic topic = new ActiveMQTopic(topicName);
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

    public MQTTFrame convertMessage(ActiveMQMessage message) throws IOException, JMSException {
        PUBLISH result = new PUBLISH();
        String msgId = message.getJMSMessageID();
        int offset = msgId.lastIndexOf(':');

        short id = 0;
        if (offset > 0) {
            Short.parseShort(msgId.substring(offset, msgId.length() - 1));
        }
        result.messageId(id);

        UTF8Buffer topicName = null;
        synchronized (mqttTopicMap) {
            topicName = mqttTopicMap.get(message.getJMSDestination());
            if (topicName == null) {
                topicName = new UTF8Buffer(message.getDestination().getPhysicalName().replaceAll(".", "/"));
                mqttTopicMap.put(message.getJMSDestination(), topicName);
            }
        }
        result.topicName(topicName);

        if (message.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {

            if (!message.isCompressed() && message.getContent() != null) {
                ByteSequence msgContent = message.getContent();
                if (msgContent.getLength() > 4) {
                    byte[] content = new byte[msgContent.getLength() - 4];
                    System.arraycopy(msgContent.data, 4, content, 0, content.length);
                    result.payload(new Buffer(content));
                }
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
            LOG.debug("Cannot convert " + message + " to a MQTT PUBLISH");
        }
        return result.encode();
    }


    public MQTTTransport getMQTTTransport() {
        return mqttTransport;
    }

    public ActiveMQDestination createTempDestination(String name, boolean topic) {
        ActiveMQDestination rc = tempDestinations.get(name);
        if (rc == null) {
            if (topic) {
                rc = new ActiveMQTempTopic(connectionId, tempDestinationGenerator.getNextSequenceId());
            } else {
                rc = new ActiveMQTempQueue(connectionId, tempDestinationGenerator.getNextSequenceId());
            }
            sendToActiveMQ(new DestinationInfo(connectionId, DestinationInfo.ADD_OPERATION_TYPE, rc), null);
            tempDestinations.put(name, rc);
            tempDestinationAmqToStompMap.put(rc.getQualifiedName(), name);
        }
        return rc;
    }

    public String getCreatedTempDestinationName(ActiveMQDestination destination) {
        return tempDestinationAmqToStompMap.get(destination.getQualifiedName());
    }


    protected void configureInactivityMonitor(short heartBeat) throws ProtocolException {
        try {

            int heartBeatMS = heartBeat * 1000;
            MQTTInactivityMonitor monitor = getMQTTTransport().getInactivityMonitor();

            monitor.setReadCheckTime(heartBeatMS);
            monitor.setInitialDelayTime(heartBeatMS);

            monitor.startMonitorThread();

        } catch (Exception ex) {

        }

        LOG.debug(getClientId() + " MQTT Connection using heart beat of  " + heartBeat + " secs");
    }


    protected void handleException(Throwable exception, MQTTFrame command) throws IOException {
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
}
