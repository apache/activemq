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
package org.apache.activemq.transport.amqp.message;

import java.nio.ByteBuffer;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.codec.CompositeWritableBuffer;
import org.apache.qpid.proton.codec.DroppingWritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.ProtonJMessage;

public class AMQPNativeOutboundTransformer extends OutboundTransformer {

    public AMQPNativeOutboundTransformer(JMSVendor vendor) {
        super(vendor);
    }

    @Override
    public EncodedMessage transform(Message msg) throws Exception {
        if (msg == null || !(msg instanceof BytesMessage)) {
            return null;
        }

        try {
            if (!msg.getBooleanProperty(prefixVendor + "NATIVE")) {
                return null;
            }
        } catch (MessageFormatException e) {
            return null;
        }

        return transform(this, (BytesMessage) msg);
    }

    static EncodedMessage transform(OutboundTransformer options, BytesMessage msg) throws JMSException {
        long messageFormat;
        try {
            messageFormat = msg.getLongProperty(options.prefixVendor + "MESSAGE_FORMAT");
        } catch (MessageFormatException e) {
            return null;
        }
        byte data[] = new byte[(int) msg.getBodyLength()];
        int dataSize = data.length;
        msg.readBytes(data);
        msg.reset();

        try {
            int count = msg.getIntProperty("JMSXDeliveryCount");
            if (count > 1) {

                // decode...
                ProtonJMessage amqp = (ProtonJMessage) org.apache.qpid.proton.message.Message.Factory.create();
                int offset = 0;
                int len = data.length;
                while (len > 0) {
                    final int decoded = amqp.decode(data, offset, len);
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

                amqp.getHeader().setDeliveryCount(new UnsignedInteger(count - 1));

                // Re-encode...
                ByteBuffer buffer = ByteBuffer.wrap(new byte[1024 * 4]);
                final DroppingWritableBuffer overflow = new DroppingWritableBuffer();
                int c = amqp.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
                if (overflow.position() > 0) {
                    buffer = ByteBuffer.wrap(new byte[1024 * 4 + overflow.position()]);
                    c = amqp.encode(new WritableBuffer.ByteBufferWrapper(buffer));
                }
                data = buffer.array();
                dataSize = c;
            }
        } catch (JMSException e) {
        }

        return new EncodedMessage(messageFormat, data, 0, dataSize);
    }
}
