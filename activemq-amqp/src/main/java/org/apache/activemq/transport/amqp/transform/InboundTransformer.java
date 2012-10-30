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
package org.apache.activemq.transport.amqp.transform;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.type.Binary;
import org.apache.qpid.proton.type.messaging.ApplicationProperties;
import org.apache.qpid.proton.type.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.type.messaging.Footer;
import org.apache.qpid.proton.type.messaging.Header;
import org.apache.qpid.proton.type.messaging.MessageAnnotations;
import org.apache.qpid.proton.type.messaging.Properties;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public abstract class InboundTransformer {

    JMSVendor vendor;

    public static final String TRANSFORMER_NATIVE = "native";
    public static final String TRANSFORMER_RAW = "raw";
    public static final String TRANSFORMER_JMS = "jms";

    String prefixVendor = "JMS_AMQP_";
    String prefixDeliveryAnnotations = "DA_";
    String prefixMessageAnnotations= "MA_";
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

    protected void populateMessage(Message jms, org.apache.qpid.proton.message.Message amqp) throws Exception {
        Header header = amqp.getHeader();
        if( header==null ) {
            header = new Header();
        }

        if( header.getDurable()!=null ) {
            jms.setJMSDeliveryMode(header.getDurable().booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
        } else {
            jms.setJMSDeliveryMode(defaultDeliveryMode);
        }
        if( header.getPriority()!=null ) {
            jms.setJMSPriority(header.getPriority().intValue());
        } else {
            jms.setJMSPriority(defaultPriority);
        }
        if( header.getTtl()!=null ) {
            jms.setJMSExpiration(header.getTtl().longValue());
        } else {
            jms.setJMSExpiration(defaultTtl);
        }
        if( header.getFirstAcquirer() !=null ) {
            jms.setBooleanProperty(prefixVendor + "FirstAcquirer", header.getFirstAcquirer());
        }
        if( header.getDeliveryCount()!=null ) {
            vendor.setJMSXDeliveryCount(jms, header.getDeliveryCount().longValue());
        }

        final DeliveryAnnotations da = amqp.getDeliveryAnnotations();
        if( da!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)da.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, prefixVendor + prefixDeliveryAnnotations + key, entry.getValue());
            }
        }

        final MessageAnnotations ma = amqp.getMessageAnnotations();
        if( ma!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)ma.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, prefixVendor + prefixMessageAnnotations + key, entry.getValue());
            }
        }

        final Properties properties = amqp.getProperties();
        if( properties!=null ) {
            if( properties.getMessageId()!=null ) {
                jms.setJMSMessageID(properties.getMessageId().toString());
            }
            Binary userId = properties.getUserId();
            if( userId!=null ) {
                vendor.setJMSXUserID(jms, new String(userId.getArray(), userId.getArrayOffset(), userId.getLength(), "UTF-8"));
            }
            if( properties.getTo()!=null ) {
                jms.setJMSDestination(vendor.createDestination(properties.getTo()));
            }
            if( properties.getSubject()!=null ) {
                jms.setStringProperty(prefixVendor + "Subject", properties.getSubject());
            }
            if( properties.getReplyTo() !=null ) {
                jms.setJMSReplyTo(vendor.createDestination(properties.getReplyTo()));
            }
            if( properties.getCorrelationId() !=null ) {
                jms.setJMSCorrelationID(properties.getCorrelationId().toString());
            }
            if( properties.getContentType() !=null ) {
                jms.setStringProperty(prefixVendor + "ContentType", properties.getContentType().toString());
            }
            if( properties.getContentEncoding() !=null ) {
                jms.setStringProperty(prefixVendor + "ContentEncoding", properties.getContentEncoding().toString());
            }
            if( properties.getCreationTime()!=null ) {
                jms.setJMSTimestamp(properties.getCreationTime().getTime());
            }
            if( properties.getGroupId()!=null ) {
                vendor.setJMSXGroupID(jms, properties.getGroupId());
            }
            if( properties.getGroupSequence()!=null ) {
                vendor.setJMSXGroupSequence(jms, properties.getGroupSequence().intValue());
            }
            if( properties.getReplyToGroupId()!=null ) {
                jms.setStringProperty(prefixVendor + "ReplyToGroupID", properties.getReplyToGroupId());
            }
        }

        final ApplicationProperties ap = amqp.getApplicationProperties();
        if( ap !=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)ap.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, key, entry.getValue());
            }
        }

        final Footer fp = amqp.getFooter();
        if( fp !=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)fp.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(jms, prefixVendor + prefixFooter + key, entry.getValue());
            }
        }
    }

    private void setProperty(Message msg, String key, Object value) throws JMSException {
        //TODO support all types
        msg.setObjectProperty(key, value);
//        if( value instanceof String ) {
//            msg.setStringProperty(key, (String) value);
//        } else if( value instanceof Double ) {
//            msg.setDoubleProperty(key, ((Double) value).doubleValue());
//        } else if( value instanceof Integer ) {
//            msg.setIntProperty(key, ((Integer) value).intValue());
//        } else if( value instanceof Long ) {
//            msg.setLongProperty(key, ((Long) value).longValue());
//        } else {
//            throw new RuntimeException("Unexpected value type: "+value.getClass());
//        }
    }
}
