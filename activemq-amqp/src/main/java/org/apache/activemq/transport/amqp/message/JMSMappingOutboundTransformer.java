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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;

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

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
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

    public JMSMappingOutboundTransformer(JMSVendor vendor) {
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
     * to do this on their own way (Netty for instance using Nettybuffers)
     *
     * @param msg
     * @return
     * @throws Exception
     */
    public ProtonJMessage convert(Message msg) throws JMSException, UnsupportedEncodingException {
        Header header = new Header();
        Properties props = new Properties();
        HashMap<Symbol, Object> daMap = null;
        HashMap<Symbol, Object> maMap = null;
        HashMap apMap = null;
        Section body = null;
        HashMap footerMap = null;
        if (msg instanceof BytesMessage) {
            BytesMessage m = (BytesMessage) msg;
            byte data[] = new byte[(int) m.getBodyLength()];
            m.readBytes(data);
            m.reset(); // Need to reset after readBytes or future readBytes
                       // calls (ex: redeliveries) will fail and return -1
            body = new Data(new Binary(data));
        }
        if (msg instanceof TextMessage) {
            body = new AmqpValue(((TextMessage) msg).getText());
        }
        if (msg instanceof MapMessage) {
            final HashMap<String, Object> map = new HashMap<String, Object>();
            final MapMessage m = (MapMessage) msg;
            final Enumeration<String> names = m.getMapNames();
            while (names.hasMoreElements()) {
                String key = names.nextElement();
                map.put(key, m.getObject(key));
            }
            body = new AmqpValue(map);
        }
        if (msg instanceof StreamMessage) {
            ArrayList<Object> list = new ArrayList<Object>();
            final StreamMessage m = (StreamMessage) msg;
            try {
                while (true) {
                    list.add(m.readObject());
                }
            } catch (MessageEOFException e) {
            }
            body = new AmqpSequence(list);
        }
        if (msg instanceof ObjectMessage) {
            body = new AmqpValue(((ObjectMessage) msg).getObject());
        }

        header.setDurable(msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? true : false);
        header.setPriority(new UnsignedByte((byte) msg.getJMSPriority()));
        if (msg.getJMSType() != null) {
            props.setSubject(msg.getJMSType());
        }
        if (msg.getJMSMessageID() != null) {
            ActiveMQMessage amqMsg = (ActiveMQMessage) msg;

            MessageId msgId = amqMsg.getMessageId();
            if (msgId.getTextView() != null) {
                props.setMessageId(msgId.getTextView());
            } else {
                props.setMessageId(msgId.toString());
            }
        }
        if (msg.getJMSDestination() != null) {
            props.setTo(vendor.toAddress(msg.getJMSDestination()));
            if (maMap == null) {
                maMap = new HashMap<Symbol, Object>();
            }
            maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(msg.getJMSDestination()));

            // Deprecated: used by legacy QPid AMQP 1.0 JMS client
            maMap.put(LEGACY_JMS_DEST_TYPE_MSG_ANNOTATION, destinationAttributes(msg.getJMSDestination()));
        }
        if (msg.getJMSReplyTo() != null) {
            props.setReplyTo(vendor.toAddress(msg.getJMSReplyTo()));
            if (maMap == null) {
                maMap = new HashMap<Symbol, Object>();
            }
            maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(msg.getJMSReplyTo()));

            // Deprecated: used by legacy QPid AMQP 1.0 JMS client
            maMap.put(LEGACY_JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationAttributes(msg.getJMSReplyTo()));
        }
        if (msg.getJMSCorrelationID() != null) {
            props.setCorrelationId(msg.getJMSCorrelationID());
        }
        if (msg.getJMSExpiration() != 0) {
            long ttl = msg.getJMSExpiration() - System.currentTimeMillis();
            if (ttl < 0) {
                ttl = 1;
            }
            header.setTtl(new UnsignedInteger((int) ttl));

            props.setAbsoluteExpiryTime(new Date(msg.getJMSExpiration()));
        }
        if (msg.getJMSTimestamp() != 0) {
            props.setCreationTime(new Date(msg.getJMSTimestamp()));
        }

        final Enumeration<String> keys = msg.getPropertyNames();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            if (key.equals(messageFormatKey) || key.equals(nativeKey)) {
                // skip..
            } else if (key.equals(firstAcquirerKey)) {
                header.setFirstAcquirer(msg.getBooleanProperty(key));
            } else if (key.startsWith("JMSXDeliveryCount")) {
                // The AMQP delivery-count field only includes prior failed delivery attempts,
                // whereas JMSXDeliveryCount includes the first/current delivery attempt.
                int amqpDeliveryCount = msg.getIntProperty(key) - 1;
                if (amqpDeliveryCount > 0) {
                    header.setDeliveryCount(new UnsignedInteger(amqpDeliveryCount));
                }
            } else if (key.startsWith("JMSXUserID")) {
                String value = msg.getStringProperty(key);
                props.setUserId(new Binary(value.getBytes("UTF-8")));
            } else if (key.startsWith("JMSXGroupID")) {
                String value = msg.getStringProperty(key);
                props.setGroupId(value);
                if (apMap == null) {
                    apMap = new HashMap();
                }
                apMap.put(key, value);
            } else if (key.startsWith("JMSXGroupSeq")) {
                UnsignedInteger value = new UnsignedInteger(msg.getIntProperty(key));
                props.setGroupSequence(value);
                if (apMap == null) {
                    apMap = new HashMap();
                }
                apMap.put(key, value);
            } else if (key.startsWith(prefixDeliveryAnnotationsKey)) {
                if (daMap == null) {
                    daMap = new HashMap<Symbol, Object>();
                }
                String name = key.substring(prefixDeliveryAnnotationsKey.length());
                daMap.put(Symbol.valueOf(name), msg.getObjectProperty(key));
            } else if (key.startsWith(prefixMessageAnnotationsKey)) {
                if (maMap == null) {
                    maMap = new HashMap<Symbol, Object>();
                }
                String name = key.substring(prefixMessageAnnotationsKey.length());
                maMap.put(Symbol.valueOf(name), msg.getObjectProperty(key));
            } else if (key.equals(contentTypeKey)) {
                props.setContentType(Symbol.getSymbol(msg.getStringProperty(key)));
            } else if (key.equals(contentEncodingKey)) {
                props.setContentEncoding(Symbol.getSymbol(msg.getStringProperty(key)));
            } else if (key.equals(replyToGroupIDKey)) {
                props.setReplyToGroupId(msg.getStringProperty(key));
            } else if (key.startsWith(prefixFooterKey)) {
                if (footerMap == null) {
                    footerMap = new HashMap();
                }
                String name = key.substring(prefixFooterKey.length());
                footerMap.put(name, msg.getObjectProperty(key));
            } else {
                if (apMap == null) {
                    apMap = new HashMap();
                }
                apMap.put(key, msg.getObjectProperty(key));
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
