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
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.getBinaryFromMessageBody;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.message.ProtonJMessage;

public class AMQPNativeOutboundTransformer implements OutboundTransformer {

    @Override
    public EncodedMessage transform(ActiveMQMessage message) throws Exception {
        if (message == null || !(message instanceof ActiveMQBytesMessage)) {
            return null;
        }

        return transform(this, (ActiveMQBytesMessage) message);
    }

    static EncodedMessage transform(OutboundTransformer options, ActiveMQBytesMessage message) throws JMSException {
        long messageFormat;
        try {
            messageFormat = message.getLongProperty(JMS_AMQP_MESSAGE_FORMAT);
        } catch (MessageFormatException e) {
            return null;
        }

        Binary encodedMessage = getBinaryFromMessageBody(message);
        byte encodedData[] = encodedMessage.getArray();
        int encodedSize = encodedMessage.getLength();

        int count = message.getRedeliveryCounter();
        if (count >= 1) {

            // decode...
            ProtonJMessage amqp = (ProtonJMessage) org.apache.qpid.proton.message.Message.Factory.create();
            int offset = 0;
            int len = encodedSize;
            while (len > 0) {
                final int decoded = amqp.decode(encodedData, offset, len);
                assert decoded > 0 : "Make progress decoding the message";
                offset += decoded;
                len -= decoded;
            }

            // Update the DeliveryCount header...
            // The AMQP delivery-count field only includes prior failed delivery attempts,
            // whereas JMSXDeliveryCount includes the first/current delivery attempt. Subtract 1.
            if (amqp.getHeader() == null) {
                amqp.setHeader(new Header());
            }

            amqp.getHeader().setDeliveryCount(new UnsignedInteger(count));

            // Re-encode...
            final AmqpWritableBuffer buffer = new AmqpWritableBuffer();
            int written = amqp.encode(buffer);

            encodedData = buffer.getArray();
            encodedSize = written;
        }

        return new EncodedMessage(messageFormat, encodedData, 0, encodedSize);
    }
}
