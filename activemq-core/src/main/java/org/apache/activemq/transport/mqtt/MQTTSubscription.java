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
import java.util.LinkedHashMap;
import java.util.LinkedList;

import javax.jms.JMSException;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.fusesource.mqtt.codec.MQTTFrame;

/**
 * Keeps track of the STOMP subscription so that acking is correctly done.
 */
public class MQTTSubscription {


    protected final MQTTProtocolConverter protocolConverter;
    protected final String subscriptionId;
    protected final ConsumerInfo consumerInfo;

    protected final LinkedHashMap<MessageId, MessageDispatch> dispatchedMessage = new LinkedHashMap<MessageId, MessageDispatch>();
    protected final LinkedList<MessageDispatch> unconsumedMessage = new LinkedList<MessageDispatch>();

    protected ActiveMQDestination destination;
    protected String transformation;

    public MQTTSubscription(MQTTProtocolConverter protocolConverter, String subscriptionId, ConsumerInfo consumerInfo, String transformation) {
        this.protocolConverter = protocolConverter;
        this.subscriptionId = subscriptionId;
        this.consumerInfo = consumerInfo;
        this.transformation = transformation;
    }

    void onMessageDispatch(MessageDispatch md) throws IOException, JMSException {
        ActiveMQMessage message = (ActiveMQMessage) md.getMessage();
        /*
        if (ackMode == CLIENT_ACK) {
            synchronized (this) {
                dispatchedMessage.put(message.getMessageId(), md);
            }
        } else if (ackMode == INDIVIDUAL_ACK) {
            synchronized (this) {
                dispatchedMessage.put(message.getMessageId(), md);
            }
        } else if (ackMode == AUTO_ACK) {
            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
            protocolConverter.getStompTransport().sendToActiveMQ(ack);
        }
        */
        MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
        protocolConverter.getMQTTTransport().sendToActiveMQ(ack);

        MQTTFrame command = protocolConverter.convertMessage(message);
        protocolConverter.getMQTTTransport().sendToMQTT(command);
    }


    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }
}
