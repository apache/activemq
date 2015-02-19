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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

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
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;

public abstract class InboundTransformer {

    JMSVendor vendor;

    public static final String TRANSFORMER_NATIVE = "native";
    public static final String TRANSFORMER_RAW = "raw";
    public static final String TRANSFORMER_JMS = "jms";

    String prefixVendor = "JMS_AMQP_";
    String prefixDeliveryAnnotations = "DA_";
    String prefixMessageAnnotations = "MA_";
    String prefixFooter = "FT_";

    int defaultDeliveryMode = javax.jms.Message.DEFAULT_DELIVERY_MODE;
    int defaultPriority = javax.jms.Message.DEFAULT_PRIORITY;
    long defaultTtl = javax.jms.Message.DEFAULT_TIME_TO_LIVE;

    public InboundTransformer(JMSVendor vendor) {
        this.vendor = vendor;
    }

    abstract public Message transform(EncodedMessage amqpMessage) throws Exception;

    public int getDefaultDeliveryMode() {
        return defaultDeliveryMode;
    }

    public void setDefaultDeliveryMode(int defaultDeliveryMode) {
        this.defaultDeliveryMode = defaultDeliveryMode;
    }

    public int getDefaultPriority() {
        return defaultPriority;
    }

    public void setDefaultPriority(int defaultPriority) {
        this.defaultPriority = defaultPriority;
    }

    public long getDefaultTtl() {
        return defaultTtl;
    }

    public void setDefaultTtl(long defaultTtl) {
        this.defaultTtl = defaultTtl;
    }

    public String getPrefixVendor() {
        return prefixVendor;
    }

    public void setPrefixVendor(String prefixVendor) {
        this.prefixVendor = prefixVendor;
    }

    public JMSVendor getVendor() {
        return vendor;
    }

    public void setVendor(JMSVendor vendor) {
        this.vendor = vendor;
    }

    @SuppressWarnings("unchecked")
    protected void populateMessage(Message jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
        Header header = amqp.getHeader();
        if (header == null) {
            header = new Header();
        }

        if (header.getDurable() != null) {
            jms.setJMSDeliveryMode(header.getDurable().booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
        } else {
            jms.setJMSDeliveryMode(defaultDeliveryMode);
        }
        if (header.getPriority() != null) {
            jms.setJMSPriority(header.getPriority().intValue());
        } else {
            jms.setJMSPriority(defaultPriority);
        }
        if (header.getFirstAcquirer() != null) {
            jms.setBooleanProperty(prefixVendor + "FirstAcquirer", header.getFirstAcquirer());
        }
        if (header.getDeliveryCount() != null) {
            vendor.setJMSXDeliveryCount(jms, header.getDeliveryCount().longValue());
        }

        final DeliveryAnnotations da = amqp.getDeliveryAnnotations();
        if (da != null) {
            for (Map.Entry<?, ?> entry : da.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, prefixVendor + prefixDeliveryAnnotations + key, entry.getValue());
            }
        }

        Class<? extends Destination> toAttributes = Destination.class;
        Class<? extends Destination> replyToAttributes = Destination.class;

        final MessageAnnotations ma = amqp.getMessageAnnotations();
        if (ma != null) {
            for (Map.Entry<?, ?> entry : ma.getValue().entrySet()) {
                String key = entry.getKey().toString();
                if ("x-opt-jms-type".equals(key.toString()) && entry.getValue() != null) {
                    jms.setJMSType(entry.getValue().toString());
                } else if ("x-opt-to-type".equals(key.toString())) {
                    toAttributes = toClassFromAttributes(entry.getValue().toString());
                } else if ("x-opt-reply-type".equals(key.toString())) {
                    replyToAttributes = toClassFromAttributes(entry.getValue().toString());
                } else {
                    setProperty(jms, prefixVendor + prefixMessageAnnotations + key, entry.getValue());
                }
            }
        }

        final ApplicationProperties ap = amqp.getApplicationProperties();
        if (ap != null) {
            for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>) ap.getValue().entrySet()) {
                String key = entry.getKey().toString();
                if ("JMSXGroupID".equals(key)) {
                    vendor.setJMSXGroupID(jms, entry.getValue().toString());
                } else if ("JMSXGroupSequence".equals(key)) {
                    vendor.setJMSXGroupSequence(jms, ((Number) entry.getValue()).intValue());
                } else if ("JMSXUserID".equals(key)) {
                    vendor.setJMSXUserID(jms, entry.getValue().toString());
                } else {
                    setProperty(jms, key, entry.getValue());
                }
            }
        }

        final Properties properties = amqp.getProperties();
        if (properties != null) {
            if (properties.getMessageId() != null) {
                jms.setJMSMessageID(properties.getMessageId().toString());
            }
            Binary userId = properties.getUserId();
            if (userId != null) {
                vendor.setJMSXUserID(jms, new String(userId.getArray(), userId.getArrayOffset(), userId.getLength(), "UTF-8"));
            }
            if (properties.getTo() != null) {
                jms.setJMSDestination(vendor.createDestination(properties.getTo(), toAttributes));
            }
            if (properties.getSubject() != null) {
                jms.setStringProperty(prefixVendor + "Subject", properties.getSubject());
            }
            if (properties.getReplyTo() != null) {
                jms.setJMSReplyTo(vendor.createDestination(properties.getReplyTo(), replyToAttributes));
            }
            if (properties.getCorrelationId() != null) {
                jms.setJMSCorrelationID(properties.getCorrelationId().toString());
            }
            if (properties.getContentType() != null) {
                jms.setStringProperty(prefixVendor + "ContentType", properties.getContentType().toString());
            }
            if (properties.getContentEncoding() != null) {
                jms.setStringProperty(prefixVendor + "ContentEncoding", properties.getContentEncoding().toString());
            }
            if (properties.getCreationTime() != null) {
                jms.setJMSTimestamp(properties.getCreationTime().getTime());
            }
            if (properties.getGroupId() != null) {
                vendor.setJMSXGroupID(jms, properties.getGroupId());
            }
            if (properties.getGroupSequence() != null) {
                vendor.setJMSXGroupSequence(jms, properties.getGroupSequence().intValue());
            }
            if (properties.getReplyToGroupId() != null) {
                jms.setStringProperty(prefixVendor + "ReplyToGroupID", properties.getReplyToGroupId());
            }
            if (properties.getAbsoluteExpiryTime() != null) {
                jms.setJMSExpiration(properties.getAbsoluteExpiryTime().getTime());
            }
        }

        // If the jms expiration has not yet been set...
        if (jms.getJMSExpiration() == 0) {
            // Then lets try to set it based on the message ttl.
            long ttl = defaultTtl;
            if (header.getTtl() != null) {
                ttl = header.getTtl().longValue();
            }
            if (ttl == 0) {
                jms.setJMSExpiration(0);
            } else {
                jms.setJMSExpiration(System.currentTimeMillis() + ttl);
            }
        }

        final Footer fp = amqp.getFooter();
        if (fp != null) {
            for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>) fp.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, prefixVendor + prefixFooter + key, entry.getValue());
            }
        }
    }

    private static final Set<String> QUEUE_ATTRIBUTES = createSet("queue");
    private static final Set<String> TOPIC_ATTRIBUTES = createSet("topic");
    private static final Set<String> TEMP_QUEUE_ATTRIBUTES = createSet("queue", "temporary");
    private static final Set<String> TEMP_TOPIC_ATTRIBUTES = createSet("topic", "temporary");

    private static Set<String> createSet(String... args) {
        HashSet<String> s = new HashSet<String>();
        for (String arg : args) {
            s.add(arg);
        }
        return Collections.unmodifiableSet(s);
    }

    Class<? extends Destination> toClassFromAttributes(String value) {
        if( value ==null ) {
            return null;
        }
        HashSet<String> attributes = new HashSet<String>();
        for( String x: value.split("\\s*,\\s*") ) {
            attributes.add(x);
        }
         if( QUEUE_ATTRIBUTES.equals(attributes) ) {
            return Queue.class;
        }
        if( TOPIC_ATTRIBUTES.equals(attributes) ) {
            return Topic.class;
        }
        if( TEMP_QUEUE_ATTRIBUTES.equals(attributes) ) {
            return TemporaryQueue.class;
        }
        if( TEMP_TOPIC_ATTRIBUTES.equals(attributes) ) {
            return TemporaryTopic.class;
        }
        return Destination.class;
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
