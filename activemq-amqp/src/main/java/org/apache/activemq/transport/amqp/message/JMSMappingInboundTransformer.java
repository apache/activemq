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
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_MAP;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_NULL;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_MESSAGE_FORMAT;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.getCharsetForTextualContent;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.isContentType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.util.ByteSequence;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class JMSMappingInboundTransformer extends InboundTransformer {

    @Override
    public String getTransformerName() {
        return TRANSFORMER_JMS;
    }

    @Override
    public InboundTransformer getFallbackTransformer() {
        return new AMQPNativeInboundTransformer();
    }

    @Override
    protected ActiveMQMessage doTransform(EncodedMessage amqpMessage) throws Exception {
        Message amqp = amqpMessage.decode();

        ActiveMQMessage result = createMessage(amqp, amqpMessage);

        populateMessage(result, amqp);

        if (amqpMessage.getMessageFormat() != 0) {
            result.setLongProperty(JMS_AMQP_MESSAGE_FORMAT, amqpMessage.getMessageFormat());
        }

        return result;
    }

    @SuppressWarnings({ "unchecked" })
    private ActiveMQMessage createMessage(Message message, EncodedMessage original) throws Exception {

        Section body = message.getBody();
        ActiveMQMessage result;

        if (body == null) {
            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                result = new ActiveMQObjectMessage();
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message) || isContentType(null, message)) {
                result = new ActiveMQBytesMessage();
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (charset != null) {
                    result = new ActiveMQTextMessage();
                } else {
                    result = new ActiveMQMessage();
                }
            }

            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_NULL);
        } else if (body instanceof Data) {
            Binary payload = ((Data) body).getValue();

            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                result = createObjectMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, message)) {
                result = createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
            } else {
                Charset charset = getCharsetForTextualContent(message.getContentType());
                if (StandardCharsets.UTF_8.equals(charset)) {
                    ByteBuffer buf = ByteBuffer.wrap(payload.getArray(), payload.getArrayOffset(), payload.getLength());

                    try {
                        CharBuffer chars = charset.newDecoder().decode(buf);
                        result = createTextMessage(String.valueOf(chars));
                    } catch (CharacterCodingException e) {
                        result = createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                    }
                } else {
                    result = createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                }
            }

            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
        } else if (body instanceof AmqpSequence) {
            AmqpSequence sequence = (AmqpSequence) body;
            ActiveMQStreamMessage m = new ActiveMQStreamMessage();
            for (Object item : sequence.getValue()) {
                m.writeObject(item);
            }

            result = m;
            result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value == null || value instanceof String) {
                result = createTextMessage((String) value);

                result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, value == null ? AMQP_VALUE_NULL : AMQP_VALUE_STRING);
            } else if (value instanceof Binary) {
                Binary payload = (Binary) value;

                if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, message)) {
                    result = createObjectMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                } else {
                    result = createBytesMessage(payload.getArray(), payload.getArrayOffset(), payload.getLength());
                }

                result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
            } else if (value instanceof List) {
                ActiveMQStreamMessage m = new ActiveMQStreamMessage();
                for (Object item : (List<Object>) value) {
                    m.writeObject(item);
                }
                result = m;
                result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_LIST);
            } else if (value instanceof Map) {
                result = createMapMessage((Map<String, Object>) value);
                result.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_MAP);
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

    private static ActiveMQBytesMessage createBytesMessage(byte[] content, int offset, int length) {
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(new ByteSequence(content, offset, length));
        return message;
    }

    public static ActiveMQTextMessage createTextMessage(String text) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        try {
            message.setText(text);
        } catch (MessageNotWriteableException ex) {}

        return message;
    }

    public static ActiveMQObjectMessage createObjectMessage(byte[] content, int offset, int length) {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setContent(new ByteSequence(content, offset, length));
        return message;
    }

    public static ActiveMQMapMessage createMapMessage(Map<String, Object> content) throws JMSException {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        final Set<Map.Entry<String, Object>> set = content.entrySet();
        for (Map.Entry<String, Object> entry : set) {
            Object value = entry.getValue();
            if (value instanceof Binary) {
                Binary binary = (Binary) value;
                value = Arrays.copyOfRange(binary.getArray(), binary.getArrayOffset(), binary.getLength());
            }
            message.setObject(entry.getKey(), value);
        }
        return message;
    }
}
