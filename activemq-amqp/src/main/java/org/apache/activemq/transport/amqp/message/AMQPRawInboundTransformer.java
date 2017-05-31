/*
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
package org.apache.activemq.transport.amqp.message;

import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_MESSAGE_FORMAT;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_NATIVE;

import javax.jms.Message;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;

public class AMQPRawInboundTransformer extends InboundTransformer {

    @Override
    public String getTransformerName() {
        return TRANSFORMER_RAW;
    }

    @Override
    public InboundTransformer getFallbackTransformer() {
        return null;  // No fallback from full raw transform, message likely dropped.
    }

    @Override
    protected ActiveMQMessage doTransform(EncodedMessage amqpMessage) throws Exception {
        ActiveMQBytesMessage result = new ActiveMQBytesMessage();
        result.setContent(new ByteSequence(amqpMessage.getArray(), amqpMessage.getArrayOffset(), amqpMessage.getLength()));

        // We cannot decode the message headers to check so err on the side of caution
        // and mark all messages as persistent.
        result.setPersistent(true);
        result.setPriority((byte) Message.DEFAULT_PRIORITY);

        final long now = System.currentTimeMillis();
        result.setTimestamp(now);

        if (amqpMessage.getMessageFormat() != 0) {
            result.setLongProperty(JMS_AMQP_MESSAGE_FORMAT, amqpMessage.getMessageFormat());
        }

        result.setBooleanProperty(JMS_AMQP_NATIVE, true);

        return result;
    }
}
