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
class MQTTSubscription {
    private final MQTTProtocolConverter protocolConverter;

    private final ConsumerInfo consumerInfo;
    private ActiveMQDestination destination;
    private final QoS qos;

    public MQTTSubscription(MQTTProtocolConverter protocolConverter, QoS qos, ConsumerInfo consumerInfo) {
        this.protocolConverter = protocolConverter;
        this.consumerInfo = consumerInfo;
        this.qos = qos;
    }

    MessageAck createMessageAck(MessageDispatch md) {
        return new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
    }

    PUBLISH createPublish(ActiveMQMessage message) throws DataFormatException, IOException, JMSException {
        PUBLISH publish = protocolConverter.convertMessage(message);
        if (publish.qos().ordinal() > this.qos.ordinal()) {
            publish.qos(this.qos);
        }
        return publish;
    }

    public boolean expectAck(PUBLISH publish) {
        QoS publishQoS = publish.qos();
        if (publishQoS.compareTo(this.qos) > 0){
            publishQoS = this.qos;
        }
        return !publishQoS.equals(QoS.AT_MOST_ONCE);
    }

    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }
}
