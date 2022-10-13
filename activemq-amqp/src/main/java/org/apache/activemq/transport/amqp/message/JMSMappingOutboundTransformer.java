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
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.AMQP_VALUE_STRING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.CONTENT_ENCODING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.DELIVERY_ANNOTATION_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.EMPTY_BINARY;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.FIRST_ACQUIRER;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.FOOTER_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.HEADER;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_DELIVERY_ANNOTATION_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_PREFIX_LENGTH;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.MESSAGE_FORMAT;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.NATIVE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.ORIGINAL_ENCODING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.PROPERTIES;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.REPLYTO_GROUP_ID;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.getBinaryFromMessageBody;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.getMapFromMessageBody;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.TypeConversionSupport;
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
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

public class JMSMappingOutboundTransformer implements OutboundTransformer {

    public static final Symbol JMS_DEST_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-dest");
    public static final Symbol JMS_REPLY_TO_TYPE_MSG_ANNOTATION = Symbol.valueOf("x-opt-jms-reply-to");

    private static final String AMQ_SCHEDULED_MESSAGE_PREFIX = "AMQ_SCHEDULED_";

    public static final byte QUEUE_TYPE = 0x00;
    public static final byte TOPIC_TYPE = 0x01;
    public static final byte TEMP_QUEUE_TYPE = 0x02;
    public static final byte TEMP_TOPIC_TYPE = 0x03;

    private final UTF8BufferType utf8BufferEncoding;

    // For now Proton requires that we create a decoder to create an encoder
    private final DecoderImpl decoder = new DecoderImpl();
    private final EncoderImpl encoder = new EncoderImpl(decoder);
    {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        utf8BufferEncoding = new UTF8BufferType(encoder, decoder);

        encoder.register(utf8BufferEncoding);
    }

    @Override
    public EncodedMessage transform(ActiveMQMessage message) throws Exception {
        if (message == null) {
            return null;
        }

        long messageFormat = 0;
        Header header = null;
        Properties properties = null;
        Map<Symbol, Object> daMap = null;
        Map<Symbol, Object> maMap = null;
        Map<String,Object> apMap = null;
        Map<Object, Object> footerMap = null;

        Section body = convertBody(message);

        if (message.isPersistent()) {
            if (header == null) {
                header = new Header();
            }
            header.setDurable(true);
        }
        byte priority = message.getPriority();
        if (priority != Message.DEFAULT_PRIORITY) {
            if (header == null) {
                header = new Header();
            }
            header.setPriority(UnsignedByte.valueOf(priority));
        }
        String type = message.getType();
        if (type != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setSubject(type);
        }
        MessageId messageId = message.getMessageId();
        if (messageId != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setMessageId(getOriginalMessageId(message));
        }
        ActiveMQDestination destination = message.getDestination();
        if (destination != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setTo(destination.getQualifiedName());
            if (maMap == null) {
                maMap = new HashMap<>();
            }
            maMap.put(JMS_DEST_TYPE_MSG_ANNOTATION, destinationType(destination));
        }
        ActiveMQDestination replyTo = message.getReplyTo();
        if (replyTo != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setReplyTo(replyTo.getQualifiedName());
            if (maMap == null) {
                maMap = new HashMap<>();
            }
            maMap.put(JMS_REPLY_TO_TYPE_MSG_ANNOTATION, destinationType(replyTo));
        }
        String correlationId = message.getCorrelationId();
        if (correlationId != null) {
            if (properties == null) {
                properties = new Properties();
            }
            try {
                properties.setCorrelationId(AMQPMessageIdHelper.INSTANCE.toIdObject(correlationId));
            } catch (AmqpProtocolException e) {
                properties.setCorrelationId(correlationId);
            }
        }
        long expiration = message.getExpiration();
        if (expiration != 0) {
            long ttl = expiration - System.currentTimeMillis();
            if (ttl < 0) {
                ttl = 1;
            }

            if (header == null) {
                header = new Header();
            }
            header.setTtl(new UnsignedInteger((int) ttl));

            if (properties == null) {
                properties = new Properties();
            }
            properties.setAbsoluteExpiryTime(new Date(expiration));
        }
        long timeStamp = message.getTimestamp();
        if (timeStamp != 0) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setCreationTime(new Date(timeStamp));
        }

        // JMSX Message Properties
        int deliveryCount = message.getRedeliveryCounter();
        if (deliveryCount > 0) {
            if (header == null) {
                header = new Header();
            }
            header.setDeliveryCount(UnsignedInteger.valueOf(deliveryCount));
        }
        String userId = message.getUserID();
        if (userId != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setUserId(new Binary(userId.getBytes(StandardCharsets.UTF_8)));
        }
        String groupId = message.getGroupID();
        if (groupId != null) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setGroupId(groupId);
        }
        int groupSequence = message.getGroupSequence();
        if (groupSequence > 0) {
            if (properties == null) {
                properties = new Properties();
            }
            properties.setGroupSequence(UnsignedInteger.valueOf(groupSequence));
        }

        final Map<String, Object> entries;
        try {
            entries = message.getProperties();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }

        for (Map.Entry<String, Object> entry : entries.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith(JMS_AMQP_PREFIX)) {
                if (key.startsWith(NATIVE, JMS_AMQP_PREFIX_LENGTH)) {
                    // skip transformer appended properties
                    continue;
                } else if (key.startsWith(ORIGINAL_ENCODING, JMS_AMQP_PREFIX_LENGTH)) {
                    // skip transformer appended properties
                    continue;
                } else if (key.startsWith(MESSAGE_FORMAT, JMS_AMQP_PREFIX_LENGTH)) {
                    messageFormat = (long) TypeConversionSupport.convert(entry.getValue(), Long.class);
                    continue;
                } else if (key.startsWith(HEADER, JMS_AMQP_PREFIX_LENGTH)) {
                    if (header == null) {
                        header = new Header();
                    }
                    continue;
                } else if (key.startsWith(PROPERTIES, JMS_AMQP_PREFIX_LENGTH)) {
                    if (properties == null) {
                        properties = new Properties();
                    }
                    continue;
                } else if (key.startsWith(MESSAGE_ANNOTATION_PREFIX, JMS_AMQP_PREFIX_LENGTH)) {
                    if (maMap == null) {
                        maMap = new HashMap<>();
                    }
                    String name = key.substring(JMS_AMQP_MESSAGE_ANNOTATION_PREFIX.length());
                    maMap.put(Symbol.valueOf(name), value);
                    continue;
                } else if (key.startsWith(FIRST_ACQUIRER, JMS_AMQP_PREFIX_LENGTH)) {
                    if (header == null) {
                        header = new Header();
                    }
                    header.setFirstAcquirer((boolean) TypeConversionSupport.convert(value, Boolean.class));
                    continue;
                } else if (key.startsWith(CONTENT_TYPE, JMS_AMQP_PREFIX_LENGTH)) {
                    if (properties == null) {
                        properties = new Properties();
                    }
                    properties.setContentType(Symbol.getSymbol((String) TypeConversionSupport.convert(value, String.class)));
                    continue;
                } else if (key.startsWith(CONTENT_ENCODING, JMS_AMQP_PREFIX_LENGTH)) {
                    if (properties == null) {
                        properties = new Properties();
                    }
                    properties.setContentEncoding(Symbol.getSymbol((String) TypeConversionSupport.convert(value, String.class)));
                    continue;
                } else if (key.startsWith(REPLYTO_GROUP_ID, JMS_AMQP_PREFIX_LENGTH)) {
                    if (properties == null) {
                        properties = new Properties();
                    }
                    properties.setReplyToGroupId((String) TypeConversionSupport.convert(value, String.class));
                    continue;
                } else if (key.startsWith(DELIVERY_ANNOTATION_PREFIX, JMS_AMQP_PREFIX_LENGTH)) {
                    if (daMap == null) {
                        daMap = new HashMap<>();
                    }
                    String name = key.substring(JMS_AMQP_DELIVERY_ANNOTATION_PREFIX.length());
                    daMap.put(Symbol.valueOf(name), value);
                    continue;
                } else if (key.startsWith(FOOTER_PREFIX, JMS_AMQP_PREFIX_LENGTH)) {
                    if (footerMap == null) {
                        footerMap = new HashMap<>();
                    }
                    String name = key.substring(JMS_AMQP_FOOTER_PREFIX.length());
                    footerMap.put(Symbol.valueOf(name), value);
                    continue;
                }
            } else if (key.startsWith(AMQ_SCHEDULED_MESSAGE_PREFIX )) {
                // strip off the scheduled message properties
                continue;
            }

            // The property didn't map into any other slot so we store it in the
            // Application Properties section of the message.
            if (apMap == null) {
                apMap = new HashMap<>();
            }
            apMap.put(key, value);

            int messageType = message.getDataStructureType();
            if (messageType == CommandTypes.ACTIVEMQ_MESSAGE) {
                // Type of command to recognize advisory message
                Object data = message.getDataStructure();
                if(data != null) {
                    apMap.put("ActiveMqDataStructureType", data.getClass().getSimpleName());
                }
            }
        }

        final AmqpWritableBuffer buffer = new AmqpWritableBuffer();
        encoder.setByteBuffer(buffer);

        if (header != null) {
            encoder.writeObject(header);
        }
        if (daMap != null) {
            encoder.writeObject(new DeliveryAnnotations(daMap));
        }
        if (maMap != null) {
            encoder.writeObject(new MessageAnnotations(maMap));
        }
        if (properties != null) {
            encoder.writeObject(properties);
        }
        if (apMap != null) {
            encoder.writeObject(new ApplicationProperties(apMap));
        }
        if (body != null) {
            encoder.writeObject(body);
        }
        if (footerMap != null) {
            encoder.writeObject(new Footer(footerMap));
        }

        return new EncodedMessage(messageFormat, buffer.getArray(), 0, buffer.getArrayLength());
    }

    private Section convertBody(ActiveMQMessage message) throws JMSException {

        Section body = null;
        short orignalEncoding = AMQP_UNKNOWN;

        try {
            orignalEncoding = message.getShortProperty(JMS_AMQP_ORIGINAL_ENCODING);
        } catch (Exception ex) {
            // Ignore and stick with UNKNOWN
        }

        int messageType = message.getDataStructureType();

        if (messageType == CommandTypes.ACTIVEMQ_MESSAGE) {
        	Object data = message.getDataStructure();
            if (data instanceof ConnectionInfo) {
    			ConnectionInfo connectionInfo = (ConnectionInfo)data;
        		final HashMap<String, Object> connectionMap = new LinkedHashMap<String, Object>();
        		
        		connectionMap.put("ConnectionId", connectionInfo.getConnectionId().getValue());
        		connectionMap.put("ClientId", connectionInfo.getClientId());
        		connectionMap.put("ClientIp", connectionInfo.getClientIp());
        		connectionMap.put("UserName", connectionInfo.getUserName());
        		connectionMap.put("BrokerMasterConnector", connectionInfo.isBrokerMasterConnector());
        		connectionMap.put("Manageable", connectionInfo.isManageable());
        		connectionMap.put("ClientMaster", connectionInfo.isClientMaster());
        		connectionMap.put("FaultTolerant", connectionInfo.isFaultTolerant());
        		connectionMap.put("FailoverReconnect", connectionInfo.isFailoverReconnect());
        		
    			body = new AmqpValue(connectionMap);
            } else if (data instanceof RemoveInfo) {
    			RemoveInfo removeInfo = (RemoveInfo)message.getDataStructure();
        		final HashMap<String, Object> removeMap = new LinkedHashMap<String, Object>();
        		
            	if (removeInfo.isConnectionRemove()) {
            		removeMap.put(ConnectionId.class.getSimpleName(), ((ConnectionId)removeInfo.getObjectId()).getValue());
            	} else if (removeInfo.isConsumerRemove()) {
            		removeMap.put(ConsumerId.class.getSimpleName(), ((ConsumerId)removeInfo.getObjectId()).getValue());
            		removeMap.put("SessionId", ((ConsumerId)removeInfo.getObjectId()).getSessionId());
            		removeMap.put("ConnectionId", ((ConsumerId)removeInfo.getObjectId()).getConnectionId());
            		removeMap.put("ParentId", ((ConsumerId)removeInfo.getObjectId()).getParentId().getValue());
            	}
            	
            	body = new AmqpValue(removeMap);
            }
        } else if (messageType == CommandTypes.ACTIVEMQ_BYTES_MESSAGE) {
            Binary payload = getBinaryFromMessageBody((ActiveMQBytesMessage) message);

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
        } else if (messageType == CommandTypes.ACTIVEMQ_TEXT_MESSAGE) {
            switch (orignalEncoding) {
                case AMQP_NULL:
                    break;
                case AMQP_DATA:
                    body = new Data(getBinaryFromMessageBody((ActiveMQTextMessage) message));
                    break;
                case AMQP_VALUE_STRING:
                case AMQP_UNKNOWN:
                default:
                    body = new AmqpValue(((TextMessage) message).getText());
                    break;
            }
        } else if (messageType == CommandTypes.ACTIVEMQ_MAP_MESSAGE) {
            body = new AmqpValue(getMapFromMessageBody((ActiveMQMapMessage) message));
        } else if (messageType == CommandTypes.ACTIVEMQ_STREAM_MESSAGE) {
            ArrayList<Object> list = new ArrayList<>();
            final ActiveMQStreamMessage m = (ActiveMQStreamMessage) message;
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
        } else if (messageType == CommandTypes.ACTIVEMQ_OBJECT_MESSAGE) {
            Binary payload = getBinaryFromMessageBody((ActiveMQObjectMessage) message);

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
            if (!message.propertyExists(JMS_AMQP_CONTENT_TYPE)) {
                message.setReadOnlyProperties(false);
                message.setStringProperty(JMS_AMQP_CONTENT_TYPE, SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
                message.setReadOnlyProperties(true);
            }
        }

        return body;
    }

    private static byte destinationType(ActiveMQDestination destination) {
        if (destination.isQueue()) {
            if (destination.isTemporary()) {
                return TEMP_QUEUE_TYPE;
            } else {
                return QUEUE_TYPE;
            }
        } else if (destination.isTopic()) {
            if (destination.isTemporary()) {
                return TEMP_TOPIC_TYPE;
            } else {
                return TOPIC_TYPE;
            }
        }

        throw new IllegalArgumentException("Unknown Destination Type passed to JMS Transformer.");
    }

    private static Object getOriginalMessageId(ActiveMQMessage message) {
        Object result;
        MessageId messageId = message.getMessageId();
        if (messageId.getTextView() != null) {
            try {
                result = AMQPMessageIdHelper.INSTANCE.toIdObject(messageId.getTextView());
            } catch (AmqpProtocolException e) {
                result = messageId.getTextView();
            }
        } else {
            result = messageId.toString();
        }

        return result;
    }
}
