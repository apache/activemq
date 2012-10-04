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

import org.apache.qpid.proton.type.Binary;
import org.apache.qpid.proton.type.messaging.*;

import javax.jms.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public class JMSMappingInboundTransformer extends InboundTransformer {

    String prefixDeliveryAnnotations = "DA_";
    String prefixMessageAnnotations= "MA_";
    String prefixFooter = "FT_";

    public JMSMappingInboundTransformer(JMSVendor vendor) {
        super(vendor);
    }

    @Override
    public Message transform(EncodedMessage amqpMessage) throws Exception {
        org.apache.qpid.proton.message.Message amqp = new org.apache.qpid.proton.message.Message();

        int offset = amqpMessage.getArrayOffset();
        int len = amqpMessage.getLength();
        while( len > 0 ) {
            final int decoded = amqp.decode(amqpMessage.getArray(), offset, len);
            assert decoded > 0: "Make progress decoding the message";
            offset += decoded;
            len -= decoded;
        }

        Message rc;
        final Section body = amqp.getBody();
        if( body instanceof Data ) {
            Binary d = ((Data) body).getValue();
            BytesMessage m = vendor.createBytesMessage();
            m.writeBytes(d.getArray(), d.getArrayOffset(), d.getLength());
            rc = m;
        } else if (body instanceof AmqpSequence ) {
            AmqpSequence sequence = (AmqpSequence) body;
            StreamMessage m = vendor.createStreamMessage();
            throw new RuntimeException("not implemented");
//                jms = m;
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if( value == null ) {
                rc = vendor.createMessage();
            } if( value instanceof String ) {
                TextMessage m = vendor.createTextMessage();
                m.setText((String) value);
                rc = m;
            } else if( value instanceof Binary ) {
                Binary d = (Binary) value;
                BytesMessage m = vendor.createBytesMessage();
                m.writeBytes(d.getArray(), d.getArrayOffset(), d.getLength());
                rc = m;
            } else if( value instanceof List) {
                List d = (List) value;
                StreamMessage m = vendor.createStreamMessage();
                throw new RuntimeException("not implemented");
//                    jms = m;
            } else if( value instanceof Map) {
                Map d = (Map) value;
                MapMessage m = vendor.createMapMessage();
                throw new RuntimeException("not implemented");
//                    jms = m;
            } else {
                ObjectMessage m = vendor.createObjectMessage();
                throw new RuntimeException("not implemented");
//                    jms = m;
            }
        } else {
            throw new RuntimeException("Unexpected body type.");
        }
        rc.setJMSDeliveryMode(defaultDeliveryMode);
        rc.setJMSPriority(defaultPriority);
        rc.setJMSExpiration(defaultTtl);

        final Header header = amqp.getHeader();
        if( header!=null ) {
            if( header.getDurable()!=null ) {
                rc.setJMSDeliveryMode(header.getDurable().booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            }
            if( header.getPriority()!=null ) {
                rc.setJMSPriority(header.getPriority().intValue());
            }
            if( header.getTtl()!=null ) {
                rc.setJMSExpiration(header.getTtl().longValue());
            }
            if( header.getFirstAcquirer() !=null ) {
                rc.setBooleanProperty(prefixVendor + "FirstAcquirer", header.getFirstAcquirer());
            }
            if( header.getDeliveryCount()!=null ) {
                vendor.setJMSXDeliveryCount(rc, header.getDeliveryCount().longValue());
            }
        }

        final DeliveryAnnotations da = amqp.getDeliveryAnnotations();
        if( da!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)da.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(rc, prefixVendor + prefixDeliveryAnnotations + key, entry.getValue());
            }
        }

        final MessageAnnotations ma = amqp.getMessageAnnotations();
        if( ma!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)ma.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(rc, prefixVendor + prefixMessageAnnotations + key, entry.getValue());
            }
        }

        final Properties properties = amqp.getProperties();
        if( properties!=null ) {
            if( properties.getMessageId()!=null ) {
                rc.setJMSMessageID(properties.getMessageId().toString());
            }
            Binary userId = properties.getUserId();
            if( userId!=null ) {
                vendor.setJMSXUserID(rc, new String(userId.getArray(), userId.getArrayOffset(), userId.getLength(), "UTF-8"));
            }
            if( properties.getTo()!=null ) {
                rc.setJMSDestination(vendor.createDestination(properties.getTo()));
            }
            if( properties.getSubject()!=null ) {
                rc.setStringProperty(prefixVendor + "Subject", properties.getSubject());
            }
            if( properties.getReplyTo() !=null ) {
                rc.setJMSReplyTo(vendor.createDestination(properties.getReplyTo()));
            }
            if( properties.getCorrelationId() !=null ) {
                rc.setJMSCorrelationID(properties.getCorrelationId().toString());
            }
            if( properties.getContentType() !=null ) {
                rc.setStringProperty(prefixVendor + "ContentType", properties.getContentType().toString());
            }
            if( properties.getContentEncoding() !=null ) {
                rc.setStringProperty(prefixVendor + "ContentEncoding", properties.getContentEncoding().toString());
            }
            if( properties.getCreationTime()!=null ) {
                rc.setJMSTimestamp(properties.getCreationTime().getTime());
            }
            if( properties.getGroupId()!=null ) {
                vendor.setJMSXGroupID(rc, properties.getGroupId());
            }
            if( properties.getGroupSequence()!=null ) {
                vendor.setJMSXGroupSequence(rc, properties.getGroupSequence().intValue());
            }
            if( properties.getReplyToGroupId()!=null ) {
                rc.setStringProperty(prefixVendor + "ReplyToGroupID", properties.getReplyToGroupId());
            }
        }

        final ApplicationProperties ap = amqp.getApplicationProperties();
        if( da!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)ap.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(rc, key, entry.getValue());
            }
        }

        final Footer fp = amqp.getFooter();
        if( da!=null ) {
            for (Map.Entry entry : (Set<Map.Entry>)fp.getValue().entrySet()) {
                String key = entry.getKey().toString();
                setProperty(rc, prefixVendor + prefixFooter + key, entry.getValue());
            }
        }

        rc.setLongProperty(prefixVendor + "MESSAGE_FORMAT", amqpMessage.getMessageFormat());
        rc.setBooleanProperty(prefixVendor + "NATIVE", false);
        return rc;
    }

    private void setProperty(Message msg, String key, Object value) throws JMSException {
        if( value instanceof String ) {
            msg.setStringProperty(key, (String) value);
//        } else if( value instanceof Integer ) {
//            msg.setIntProperty(key, ((Integer) value).intValue());
//        } else if( value instanceof Long ) {
//            msg.setLongProperty(key, ((Long) value).longValue());
        } else {
            throw new RuntimeException("Unexpected value type: "+value.getClass());
        }
    }
}
