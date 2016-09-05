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

import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_DATA;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_NULL;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_ORIGINAL_ENCODING_KEY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_MAP;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_NULL;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.getCharsetForTextualContent;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.isContentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import javax.jms.StreamMessage;

import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class JMSMappingInboundTransformer extends InboundTransformer {

    public JMSMappingInboundTransformer(ActiveMQJMSVendor vendor) {
        super(vendor);
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_JMS;
    }

    @Override
    public InboundTransformer getFallbackTransformer() {
        return new AMQPNativeInboundTransformer(getVendor());
    }

    @Override
    protected javax.jms.Message doTransform(EncodedMessage amqpMessage) throws Exception {
        Message amqp = amqpMessage.decode();

        javax.jms.Message result = createMessage(amqp, amqpMessage);

        result.setJMSDeliveryMode(defaultDeliveryMode);
        result.setJMSPriority(defaultPriority);
        result.setJMSExpiration(defaultTtl);

        populateMessage(result, amqp);

        result.setLongProperty(prefixVendor + "MESSAGE_FORMAT", amqpMessage.getMessageFormat());
        result.setBooleanProperty(prefixVendor + "NATIVE", false);

        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private javax.jms.Message createMessage(Message message, EncodedMessage original) throws Exception {

        Section body = message.getBody();
        javax.jms.Message result;

        if (body == null) {
            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                result = vendor.createObjectMessage();
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message) || isContentType(null, message)) {
                result = vendor.createBytesMessage();
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (charset != null) {
                    result = vendor.createTextMessage();
                } else {
                    result = vendor.createMessage();
                }
            }

            result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_NULL);
        } else if (body instanceof Data) {
            Binary payload = ((Data) body).getValue();

            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                result = vendor.createObjectMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message)) {
                result = vendor.createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (StandardCharsets.UTF_8.equals(charset)) {
                    ByteBuffer buf = ByteBuffer.wrap(payload.getArray(), payload.getArrayOffset(), payload.getLength());

                    try {
                        CharBuffer chars = charset.newDecoder().decode(buf);
                        result = vendor.createTextMessage(String.valueOf(chars));
                    } catch (CharacterCodingException e) {
                        result = vendor.createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                    }
                } else {
                    result = vendor.createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                }
            }

            result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_DATA);
        } else if (body instanceof AmqpSequence) {
            AmqpSequence sequence = (AmqpSequence) body;
            StreamMessage m = vendor.createStreamMessage();
            for (Object item : sequence.getValue()) {
                m.writeObject(item);
            }

            result = m;
            result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_SEQUENCE);
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value == null || value instanceof String) {
                result = vendor.createTextMessage((String) value);

                result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, value == null ? AMQP_VALUE_NULL : AMQP_VALUE_STRING);
            } else if (value instanceof Binary) {
                Binary payload = (Binary) value;

                if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                    result = vendor.createObjectMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                } else {
                    result = vendor.createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                }

                result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_VALUE_BINARY);
            } else if (value instanceof List) {
                StreamMessage m = vendor.createStreamMessage();
                for (Object item : (List<Object>) value) {
                    m.writeObject(item);
                }
                result = m;
                result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_VALUE_LIST);
            } else if (value instanceof Map) {
                result = vendor.createMapMessage((Map<String, Object>) value);
                result.setShortProperty(AMQP_ORIGINAL_ENCODING_KEY, AMQP_VALUE_MAP);
            } else {
                // Trigger fall-back to native encoder which generates BytesMessage with the
                // original message stored in the message body.
                throw new AmqpProtocolException("Unable to encode to ActiveMQ JMS Message", false);
            }
        } else {
            throw new RuntimeException("Unexpected body type: " + body.getClass());
        }

        return result;
    }
}
