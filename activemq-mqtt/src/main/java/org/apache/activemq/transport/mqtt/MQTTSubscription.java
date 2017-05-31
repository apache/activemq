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
import java.util.zip.DataFormatException;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.codec.PUBLISH;

/**
 * Keeps track of the MQTT client subscription so that acking is correctly done.
 */
public class MQTTSubscription {

    private final MQTTProtocolConverter protocolConverter;

    private final ConsumerInfo consumerInfo;
    private final String topicName;
    private final QoS qos;

    public MQTTSubscription(MQTTProtocolConverter protocolConverter, String topicName, QoS qos, ConsumerInfo consumerInfo) {
        this.protocolConverter = protocolConverter;
        this.consumerInfo = consumerInfo;
        this.qos = qos;
        this.topicName = topicName;
    }

    /**
     * Create a {@link MessageAck} that will acknowledge the given {@link MessageDispatch}.
     *
     * @param md
     *        the {@link MessageDispatch} to acknowledge.
     *
     * @return a new {@link MessageAck} command to acknowledge the message.
     */
    public MessageAck createMessageAck(MessageDispatch md) {
        return new MessageAck(md, MessageAck.INDIVIDUAL_ACK_TYPE, 1);
    }

    /**
     * Creates a PUBLISH command that can be sent to a remote client from an
     * incoming {@link ActiveMQMessage} instance.
     *
     * @param message
     *        the message to convert to a PUBLISH command.
     *
     * @return a new PUBLISH command that is populated from the {@link ActiveMQMessage}.
     *
     * @throws DataFormatException
     * @throws IOException
     * @throws JMSException
     */
    public PUBLISH createPublish(ActiveMQMessage message) throws DataFormatException, IOException, JMSException {
        PUBLISH publish = protocolConverter.convertMessage(message);
        if (publish.qos().ordinal() > this.qos.ordinal()) {
            publish.qos(this.qos);
        }
        switch (publish.qos()) {
            case AT_LEAST_ONCE:
            case EXACTLY_ONCE:
                // set packet id, and optionally dup flag
                protocolConverter.getPacketIdGenerator().setPacketId(protocolConverter.getClientId(), this, message, publish);
            case AT_MOST_ONCE:
        }
        return publish;
    }

    /**
     * Given a PUBLISH command determine if it will expect an ACK based on the
     * QoS of the Publish command and the QoS of this subscription.
     *
     * @param publish
     *        The publish command to inspect.
     *
     * @return true if the client will expect an PUBACK for this PUBLISH.
     */
    public boolean expectAck(PUBLISH publish) {
        QoS publishQoS = publish.qos();
        if (publishQoS.compareTo(this.qos) > 0){
            publishQoS = this.qos;
        }
        return !publishQoS.equals(QoS.AT_MOST_ONCE);
    }

    /**
     * @returns the original topic name value the client used when subscribing.
     */
    public String getTopicName() {
        return this.topicName;
    }

    /**
     * The real {@link ActiveMQDestination} that this subscription is assigned.
     *
     * @return the real {@link ActiveMQDestination} assigned to this subscription.
     */
    public ActiveMQDestination getDestination() {
        return consumerInfo.getDestination();
    }

    /**
     * Gets the {@link ConsumerInfo} that describes the subscription sent to ActiveMQ.
     *
     * @return the {@link ConsumerInfo} used to create this subscription.
     */
    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }

    /**
     * @return the assigned QoS value for this subscription.
     */
    public QoS getQoS() {
        return qos;
    }

    @Override
    public String toString() {
        return "MQTT Sub: topic[" + topicName + "] -> [" + consumerInfo.getDestination() + "]";
    }
}
