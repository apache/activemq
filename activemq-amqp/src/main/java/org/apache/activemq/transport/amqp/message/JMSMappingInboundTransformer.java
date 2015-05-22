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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

public class JMSMappingInboundTransformer extends InboundTransformer {

    public JMSMappingInboundTransformer(JMSVendor vendor) {
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

    @SuppressWarnings({ "unchecked" })
    @Override
    public Message transform(EncodedMessage amqpMessage) throws Exception {
        org.apache.qpid.proton.message.Message amqp = amqpMessage.decode();

        Message rc;
        final Section body = amqp.getBody();
        if (body == null) {
            rc = vendor.createMessage();
        } else if (body instanceof Data) {
            Binary d = ((Data) body).getValue();
            BytesMessage m = vendor.createBytesMessage();
            m.writeBytes(d.getArray(), d.getArrayOffset(), d.getLength());
            rc = m;
        } else if (body instanceof AmqpSequence) {
            AmqpSequence sequence = (AmqpSequence) body;
            StreamMessage m = vendor.createStreamMessage();
            for (Object item : sequence.getValue()) {
                m.writeObject(item);
            }
            rc = m;
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value == null) {
                rc = vendor.createObjectMessage();
            }
            if (value instanceof String) {
                TextMessage m = vendor.createTextMessage();
                m.setText((String) value);
                rc = m;
            } else if (value instanceof Binary) {
                Binary d = (Binary) value;
                BytesMessage m = vendor.createBytesMessage();
                m.writeBytes(d.getArray(), d.getArrayOffset(), d.getLength());
                rc = m;
            } else if (value instanceof List) {
                StreamMessage m = vendor.createStreamMessage();
                for (Object item : (List<Object>) value) {
                    m.writeObject(item);
                }
                rc = m;
            } else if (value instanceof Map) {
                MapMessage m = vendor.createMapMessage();
                final Set<Map.Entry<String, Object>> set = ((Map<String, Object>) value).entrySet();
                for (Map.Entry<String, Object> entry : set) {
                    m.setObject(entry.getKey(), entry.getValue());
                }
                rc = m;
            } else {
                ObjectMessage m = vendor.createObjectMessage();
                m.setObject((Serializable) value);
                rc = m;
            }
        } else {
            throw new RuntimeException("Unexpected body type: " + body.getClass());
        }
        rc.setJMSDeliveryMode(defaultDeliveryMode);
        rc.setJMSPriority(defaultPriority);
        rc.setJMSExpiration(defaultTtl);

        populateMessage(rc, amqp);

        rc.setLongProperty(prefixVendor + "MESSAGE_FORMAT", amqpMessage.getMessageFormat());
        rc.setBooleanProperty(prefixVendor + "NATIVE", false);
        return rc;
    }
}
