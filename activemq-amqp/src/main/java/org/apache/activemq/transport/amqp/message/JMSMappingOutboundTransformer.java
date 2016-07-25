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
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.EMPTY_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.CompositeWritableBuffer;
import org.apache.qpid.proton.codec.DroppingWritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.ProtonJMessage;

public class JMSMappingOutboundTransformer extends OutboundTransformer {

    public static final Symbol JMS_DEST_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-dest");
    public static final Symbol JMS_REPLY_TO_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-reply-to");

    public static final byte QUEUE_TYPE = 0x00;
    public static final byte TOPIC_TYPE = 0x01;
    public static final byte TEMP_QUEUE_TYPE = 0x02;
    public static final byte TEMP_TOPIC_TYPE = 0x03;

    // Deprecated legacy values used by old QPid AMQP 1.0 JMS client.

    public static final Symbol LEGACY_JMS_DEST_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-to-type");
    public static final Symbol LEGACY_JMS_REPLY_TO_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-reply-type");

    public static final String LEGACY_QUEUE_TYPE = "queue";
    public static final String LEGACY_TOPIC_TYPE = "topic";
    public static final String LEGACY_TEMP_QUEUE_TYPE = "temporary,queue";
    public static final String LEGACY_TEMP_TOPIC_TYPE = "temporary,topic";

    public JMSMappingOutboundTransformer(ActiveMQJMSVendor vendor) {
        super(vendor);
    }

    @Override
    public EncodedMessage transform(Message msg) throws Exception {
        if (msg == null) {
            return null;
        }

        try {
            if (msg.getBooleanProperty(prefixVendor + "NATIVE")) {
                return null;
            }
        } catch (MessageFormatException e) {
            return null;
        }
        ProtonJMessage amqp = convert(msg);

        long messageFormat;
        try {
            messageFormat = msg.getLongProperty(this.messageFormatKey);
        } catch (MessageFormatException e) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[1024 * 4]);
        final DroppingWritableBuffer overflow = new DroppingWritableBuffer();
        int c = amqp.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
        if (overflow.position() > 0) {
            buffer = ByteBuffer.wrap(new byte[1024 * 4 + overflow.position()]);
            c = amqp.encode(new WritableBuffer.ByteBufferWrapper(buffer));
        }

        return new EncodedMessage(messageFormat, buffer.array(), 0, c);
    }

    /**
     * Perform the conversion between JMS Message and Proton Message without
     * re-encoding it to array. This is needed because some frameworks may elect
     * to do this on their own way.
     *
     * @param message
     *      The message to transform into an AMQP version for dispatch.
     *
     * @return an AMQP Message that represents the given JMS Message.
     *
     * @throws Exception if an error occurs during the conversion.
     */
    public ProtonJMessage convert(Message message) throws JMSException, UnsupportedEncodingException {
        Header header = new Header();
        Properties props = new Properties();

        Map<Symbol, Object> daMap = null;
        Map<Symbol, Object> maMap = null;
        Map<String,Object> apMap = null;
        Map<Object, Object> footerMap = null;
        Section body = null;

        body = convertBody(message);

        header.setDurable(message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? true : false);
        header.setPriority(new UnsignedByte((byte) message.getJMSPriority()));
        if (message.getJMSType() != null) {
            props.setSubject(message.getJMSType());
        }
        if (message.getJMSMessageID() != null) {
            props.setMessageId(vendor.getOriginalMessageId(message));
        }
        if (message.getJMSDestination() != null) {
            props.setTo(vendor.toAddress(message.getJMSDestination()));
            if (maMap == null) {
                maMap = new HashMap<Symbol, Object>();
            }
            maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(message.getJMSDestination()));

            // Deprecated: used by legacy QPid AMQP 1.0 JMS client
            maMap.put(LEGACY_JMS_DEST_TYPE_MSG_ANNOTATION, destinationAttributes(message.getJMSDestination()));
        }
        if (message.getJMSReplyTo() != null) {
            props.setReplyTo(vendor.toAddress(message.getJMSReplyTo()));
            if (maMap == null) {
                maMap = new HashMap<Symbol, Object>();
            }
            maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(message.getJMSReplyTo()));

            // Deprecated: used by legacy QPid AMQP 1.0 JMS client
            maMap.put(LEGACY_JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationAttributes(message.getJMSReplyTo()));
        }
        if (message.getJMSCorrelationID() != null) {
            String correlationId = message.getJMSCorrelationID();
            try {
                props.setCorrelationId(AMQPMessageIdHelper.INSTANCE.toIdObject(correlationId));
            } catch (AmqpProtocolException e) {
                props.setCorrelationId(correlationId);
            }
        }
        if (message.getJMSExpiration() != 0) {
            long ttl = message.getJMSExpiration() - System.currentTimeMillis();
            if (ttl < 0) {
                ttl = 1;
            }
            header.setTtl(new UnsignedInteger((int) ttl));

            props.setAbsoluteExpiryTime(new Date(message.getJMSExpiration()));
        }
        if (message.getJMSTimestamp() != 0) {
            props.setCreationTime(new Date(message.getJMSTimestamp()));
        }

        @SuppressWarnings("unchecked")
        final Enumeration<String> keys = message.getPropertyNames();

        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            if (key.equals(messageFormatKey) || key.equals(nativeKey) || key.equals(AMQP_ORIGINAL_ENCODING_KEY)) {
                // skip transformer appended properties
            } else if (key.equals(firstAcquirerKey)) {
                header.setFirstAcquirer(message.getBooleanProperty(key));
            } else if (key.startsWith("JMSXDeliveryCount")) {
                // The AMQP delivery-count field only includes prior failed delivery attempts,
                // whereas JMSXDeliveryCount includes the first/current delivery attempt.
                int amqpDeliveryCount = message.getIntProperty(key) - 1;
                if (amqpDeliveryCount > 0) {
                    header.setDeliveryCount(new UnsignedInteger(amqpDeliveryCount));
                }
            } else if (key.startsWith("JMSXUserID")) {
                String value = message.getStringProperty(key);
                props.setUserId(new Binary(value.getBytes("UTF-8")));
            } else if (key.startsWith("JMSXGroupID")) {
                String value = message.getStringProperty(key);
                props.setGroupId(value);
                if (apMap == null) {
                    apMap = new HashMap<String, Object>();
                }
                apMap.put(key, value);
            } else if (key.startsWith("JMSXGroupSeq")) {
                UnsignedInteger value = new UnsignedInteger(message.getIntProperty(key));
                props.setGroupSequence(value);
                if (apMap == null) {
                    apMap = new HashMap<String, Object>();
                }
                apMap.put(key, value);
            } else if (key.startsWith(prefixDeliveryAnnotationsKey)) {
                if (daMap == null) {
                    daMap = new HashMap<Symbol, Object>();
                }
                String name = key.substring(prefixDeliveryAnnotationsKey.length());
                daMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
            } else if (key.startsWith(prefixMessageAnnotationsKey)) {
                if (maMap == null) {
                    maMap = new HashMap<Symbol, Object>();
                }
                String name = key.substring(prefixMessageAnnotationsKey.length());
                maMap.put(Symbol.valueOf(name), message.getObjectProperty(key));
            } else if (key.equals(contentTypeKey)) {
                props.setContentType(Symbol.getSymbol(message.getStringProperty(key)));
            } else if (key.equals(contentEncodingKey)) {
                props.setContentEncoding(Symbol.getSymbol(message.getStringProperty(key)));
            } else if (key.equals(replyToGroupIDKey)) {
                props.setReplyToGroupId(message.getStringProperty(key));
            } else if (key.startsWith(prefixFooterKey)) {
                if (footerMap == null) {
                    footerMap = new HashMap<Object, Object>();
                }
                String name = key.substring(prefixFooterKey.length());
                footerMap.put(name, message.getObjectProperty(key));
            } else {
                if (apMap == null) {
                    apMap = new HashMap<String, Object>();
                }
                apMap.put(key, message.getObjectProperty(key));
            }
        }

        MessageAnnotations ma = null;
        if (maMap != null) {
            ma = new MessageAnnotations(maMap);
        }
        DeliveryAnnotations da = null;
        if (daMap != null) {
            da = new DeliveryAnnotations(daMap);
        }
        ApplicationProperties ap = null;
        if (apMap != null) {
            ap = new ApplicationProperties(apMap);
        }
        Footer footer = null;
        if (footerMap != null) {
            footer = new Footer(footerMap);
        }

        return (ProtonJMessage) org.apache.qpid.proton.message.Message.Factory.create(header, da, ma, props, ap, body, footer);
    }

    private Section convertBody(Message message) throws JMSException {

        Section body = null;
        short orignalEncoding = AMQP_UNKNOWN;

        if (message.propertyExists(AMQP_ORIGINAL_ENCODING_KEY)) {
            try {
                orignalEncoding = message.getShortProperty(AMQP_ORIGINAL_ENCODING_KEY);
            } catch (Exception ex) {
            }
        }

        if (message instanceof BytesMessage) {
            Binary payload = vendor.getBinaryFromMessageBody((BytesMessage) message);

            if (payload == null) {
                payload = EMPTY_BINARY;
            }

            switch (orignalEncoding) {
                case AMQP_NULL:
                    break;
                case AMQP_VALUE_BINARY:
                    body = new AmqpValue(payload);
                    break;
                case AMQP_DATA:
                case AMQP_UNKNOWN:
                default:
                    body = new Data(payload);
                    break;
            }
        } else if (message instanceof TextMessage) {
            switch (orignalEncoding) {
                case AMQP_NULL:
                    break;
                case AMQP_DATA:
                    body = new Data(vendor.getBinaryFromMessageBody((TextMessage) message));
                    break;
                case AMQP_VALUE_STRING:
                case AMQP_UNKNOWN:
                default:
                    body = new AmqpValue(((TextMessage) message).getText());
                    break;
            }
        } else if (message instanceof MapMessage) {
            body = new AmqpValue(vendor.getMapFromMessageBody((MapMessage) message));
        } else if (message instanceof StreamMessage) {
            ArrayList<Object> list = new ArrayList<Object>();
            final StreamMessage m = (StreamMessage) message;
            try {
                while (true) {
                    list.add(m.readObject());
                }
            } catch (MessageEOFException e) {
            }

            switch (orignalEncoding) {
                case AMQP_SEQUENCE:
                    body = new AmqpSequence(list);
                    break;
                case AMQP_VALUE_LIST:
                case AMQP_UNKNOWN:
                default:
                    body = new AmqpValue(list);
                    break;
            }
        } else if (message instanceof ObjectMessage) {
            Binary payload = vendor.getBinaryFromMessageBody((ObjectMessage) message);

            if (payload == null) {
                payload = EMPTY_BINARY;
            }

            switch (orignalEncoding) {
                case AMQP_VALUE_BINARY:
                    body = new AmqpValue(payload);
                    break;
                case AMQP_DATA:
                case AMQP_UNKNOWN:
                default:
                    body = new Data(payload);
                    break;
            }

            // For a non-AMQP message we tag the outbound content type as containing
            // a serialized Java object so that an AMQP client has a hint as to what
            // we are sending it.
            if (!message.propertyExists(contentTypeKey)) {
                vendor.setMessageProperty(message, contentTypeKey, SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
            }
        }

        return body;
    }

    private static byte destinationType(Destination destination) {
        if (destination instanceof Queue) {
            if (destination instanceof TemporaryQueue) {
                return TEMP_QUEUE_TYPE;
            } else {
                return QUEUE_TYPE;
            }
        } else if (destination instanceof Topic) {
            if (destination instanceof TemporaryTopic) {
                return TEMP_TOPIC_TYPE;
            } else {
                return TOPIC_TYPE;
            }
        }

        throw new IllegalArgumentException("Unknown Destination Type passed to JMS Transformer.");
    }

    // Used by legacy QPid AMQP 1.0 JMS client.
    @Deprecated
    private static String destinationAttributes(Destination destination) {
        if (destination instanceof Queue) {
            if (destination instanceof TemporaryQueue) {
                return LEGACY_TEMP_QUEUE_TYPE;
            } else {
                return LEGACY_QUEUE_TYPE;
            }
        } else if (destination instanceof Topic) {
            if (destination instanceof TemporaryTopic) {
                return LEGACY_TEMP_TOPIC_TYPE;
            } else {
                return LEGACY_TOPIC_TYPE;
            }
        }

        throw new IllegalArgumentException("Unknown Destination Type passed to JMS Transformer.");
    }
}
