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

import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_CONTENT_ENCODING;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_CONTENT_TYPE;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_FIRST_ACQUIRER;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_FOOTER_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_HEADER;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_MESSAGE_ANNOTATION_PREFIX;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_PROPERTIES;
import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_REPLYTO_GROUP_ID;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;

public abstract class InboundTransformer {

    public static final String TRANSFORMER_NATIVE = "native";
    public static final String TRANSFORMER_RAW = "raw";
    public static final String TRANSFORMER_JMS = "jms";

    public abstract String getTransformerName();

    public abstract InboundTransformer getFallbackTransformer();

    public final ActiveMQMessage transform(EncodedMessage amqpMessage) throws Exception {
        InboundTransformer transformer = this;
        ActiveMQMessage message = null;

        while (transformer != null) {
            try {
                message = transformer.doTransform(amqpMessage);
                break;
            } catch (Exception e) {
                transformer = transformer.getFallbackTransformer();
            }
        }

        if (message == null) {
            throw new AmqpProtocolException("Failed to transform incoming delivery, skipping.", false);
        }

        return message;
    }

    protected abstract ActiveMQMessage doTransform(EncodedMessage amqpMessage) throws Exception;

    @SuppressWarnings("unchecked")
    protected void populateMessage(ActiveMQMessage jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
        Header header = amqp.getHeader();
        if (header != null) {
            jms.setBooleanProperty(JMS_AMQP_HEADER, true);

            if (header.getDurable() != null) {
                jms.setPersistent(header.getDurable().booleanValue());
            }

            if (header.getPriority() != null) {
                jms.setJMSPriority(header.getPriority().intValue());
            } else {
                jms.setPriority((byte) Message.DEFAULT_PRIORITY);
            }

            if (header.getFirstAcquirer() != null) {
                jms.setBooleanProperty(JMS_AMQP_FIRST_ACQUIRER, header.getFirstAcquirer());
            }

            if (header.getDeliveryCount() != null) {
                jms.setRedeliveryCounter(header.getDeliveryCount().intValue());
            }
        } else {
            jms.setPriority((byte) Message.DEFAULT_PRIORITY);
            jms.setPersistent(false);
        }

        final MessageAnnotations ma = amqp.getMessageAnnotations();
        if (ma != null) {
            for (Map.Entry<?, ?> entry : ma.getValue().entrySet()) {
                String key = entry.getKey().toString();
                if ("x-opt-delivery-time".equals(key) && entry.getValue() != null) {
                    long deliveryTime = ((Number) entry.getValue()).longValue();
                    long delay = deliveryTime - System.currentTimeMillis();
                    if (delay > 0) {
                        jms.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
                    }
                } else if ("x-opt-delivery-delay".equals(key) && entry.getValue() != null) {
                    long delay = ((Number) entry.getValue()).longValue();
                    if (delay > 0) {
                        jms.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
                    }
                } else if ("x-opt-delivery-repeat".equals(key) && entry.getValue() != null) {
                    int repeat = ((Number) entry.getValue()).intValue();
                    if (repeat > 0) {
                        jms.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
                    }
                } else if ("x-opt-delivery-period".equals(key) && entry.getValue() != null) {
                    long period = ((Number) entry.getValue()).longValue();
                    if (period > 0) {
                        jms.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
                    }
                } else if ("x-opt-delivery-cron".equals(key) && entry.getValue() != null) {
                    String cronEntry = (String) entry.getValue();
                    if (cronEntry != null) {
                        jms.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, cronEntry);
                    }
                }

                setProperty(jms, JMS_AMQP_MESSAGE_ANNOTATION_PREFIX + key, entry.getValue());
            }
        }

        final ApplicationProperties ap = amqp.getApplicationProperties();
        if (ap != null) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) ap.getValue()).entrySet()) {
                setProperty(jms,  entry.getKey(), entry.getValue());
            }
        }

        final Properties properties = amqp.getProperties();
        if (properties != null) {
            jms.setBooleanProperty(JMS_AMQP_PROPERTIES, true);
            if (properties.getMessageId() != null) {
                jms.setJMSMessageID(AMQPMessageIdHelper.INSTANCE.toBaseMessageIdString(properties.getMessageId()));
            }
            Binary userId = properties.getUserId();
            if (userId != null) {
                jms.setUserID(new String(userId.getArray(), userId.getArrayOffset(), userId.getLength(), StandardCharsets.UTF_8));
            }
            if (properties.getTo() != null) {
                jms.setDestination((ActiveMQDestination.createDestination(properties.getTo(), ActiveMQDestination.QUEUE_TYPE)));
            }
            if (properties.getSubject() != null) {
                jms.setType(properties.getSubject());
            }
            if (properties.getReplyTo() != null) {
                jms.setReplyTo((ActiveMQDestination.createDestination(properties.getReplyTo(), ActiveMQDestination.QUEUE_TYPE)));
            }
            if (properties.getCorrelationId() != null) {
                jms.setCorrelationId(AMQPMessageIdHelper.INSTANCE.toBaseMessageIdString(properties.getCorrelationId()));
            }
            if (properties.getContentType() != null) {
                jms.setStringProperty(JMS_AMQP_CONTENT_TYPE, properties.getContentType().toString());
            }
            if (properties.getContentEncoding() != null) {
                jms.setStringProperty(JMS_AMQP_CONTENT_ENCODING, properties.getContentEncoding().toString());
            }
            if (properties.getCreationTime() != null) {
                jms.setTimestamp(properties.getCreationTime().getTime());
            }
            if (properties.getGroupId() != null) {
                jms.setGroupID(properties.getGroupId());
            }
            if (properties.getGroupSequence() != null) {
                jms.setGroupSequence(properties.getGroupSequence().intValue());
            }
            if (properties.getReplyToGroupId() != null) {
                jms.setStringProperty(JMS_AMQP_REPLYTO_GROUP_ID, properties.getReplyToGroupId());
            }
            if (properties.getAbsoluteExpiryTime() != null) {
                jms.setExpiration(properties.getAbsoluteExpiryTime().getTime());
            }
        }

        // If the jms expiration has not yet been set...
        if (header != null && jms.getJMSExpiration() == 0) {
            // Then lets try to set it based on the message ttl.
            long ttl = Message.DEFAULT_TIME_TO_LIVE;
            if (header.getTtl() != null) {
                ttl = header.getTtl().longValue();
            }

            if (ttl != javax.jms.Message.DEFAULT_TIME_TO_LIVE) {
                jms.setExpiration(System.currentTimeMillis() + ttl);
            }
        }

        final Footer fp = amqp.getFooter();
        if (fp != null) {
            for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>) fp.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, JMS_AMQP_FOOTER_PREFIX + key, entry.getValue());
            }
        }
    }

    private void setProperty(Message msg, String key, Object value) throws JMSException {
        if (value instanceof UnsignedLong) {
            long v = ((UnsignedLong) value).longValue();
            msg.setLongProperty(key, v);
        } else if (value instanceof UnsignedInteger) {
            long v = ((UnsignedInteger) value).longValue();
            if (Integer.MIN_VALUE <= v && v <= Integer.MAX_VALUE) {
                msg.setIntProperty(key, (int) v);
            } else {
                msg.setLongProperty(key, v);
            }
        } else if (value instanceof UnsignedShort) {
            int v = ((UnsignedShort) value).intValue();
            if (Short.MIN_VALUE <= v && v <= Short.MAX_VALUE) {
                msg.setShortProperty(key, (short) v);
            } else {
                msg.setIntProperty(key, v);
            }
        } else if (value instanceof UnsignedByte) {
            short v = ((UnsignedByte) value).shortValue();
            if (Byte.MIN_VALUE <= v && v <= Byte.MAX_VALUE) {
                msg.setByteProperty(key, (byte) v);
            } else {
                msg.setShortProperty(key, v);
            }
        } else if (value instanceof Symbol) {
            msg.setStringProperty(key, value.toString());
        } else if (value instanceof Decimal128) {
            msg.setDoubleProperty(key, ((Decimal128) value).doubleValue());
        } else if (value instanceof Decimal64) {
            msg.setDoubleProperty(key, ((Decimal64) value).doubleValue());
        } else if (value instanceof Decimal32) {
            msg.setFloatProperty(key, ((Decimal32) value).floatValue());
        } else if (value instanceof Binary) {
            msg.setStringProperty(key, value.toString());
        } else {
            msg.setObjectProperty(key, value);
        }
    }
}
