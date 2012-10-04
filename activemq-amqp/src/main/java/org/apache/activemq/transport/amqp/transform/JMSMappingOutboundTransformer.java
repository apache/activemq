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

import org.apache.qpid.proton.codec.CompositeWritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.type.Binary;
import org.apache.qpid.proton.type.Symbol;
import org.apache.qpid.proton.type.UnsignedByte;
import org.apache.qpid.proton.type.UnsignedInteger;
import org.apache.qpid.proton.type.messaging.*;

import javax.jms.*;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;

/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public class JMSMappingOutboundTransformer extends OutboundTransformer {

    String prefixDeliveryAnnotations = "DA_";
    String prefixMessageAnnotations= "MA_";
    String prefixFooter = "FT_";

    public JMSMappingOutboundTransformer(JMSVendor vendor) {
        super(vendor);
    }

    @Override
    public EncodedMessage transform(Message msg) throws Exception {
        if( msg == null )
            return null;
        try {
            if( msg.getBooleanProperty(prefixVendor + "NATIVE") ) {
                return null;
            }
        } catch (MessageFormatException e) {
            return null;
        }
        return transform(this, msg);
    }

    static EncodedMessage transform(JMSMappingOutboundTransformer options, Message msg) throws JMSException, UnsupportedEncodingException {
        final JMSVendor vendor = options.vendor;

        final String messageFormatKey = options.prefixVendor + "MESSAGE_FORMAT";
        final String nativeKey = options.prefixVendor + "NATIVE";
        final String firstAcquirerKey = options.prefixVendor + "FirstAcquirer";
        final String prefixDeliveryAnnotationsKey = options.prefixVendor + options.prefixDeliveryAnnotations;
        final String prefixMessageAnnotationsKey = options.prefixVendor + options.prefixMessageAnnotations;
        final String subjectKey =  options.prefixVendor +"Subject";
        final String contentTypeKey = options.prefixVendor +"ContentType";
        final String contentEncodingKey = options.prefixVendor +"ContentEncoding";
        final String replyToGroupIDKey = options.prefixVendor +"ReplyToGroupID";
        final String prefixFooterKey = options.prefixVendor + options.prefixFooter;

        long messageFormat;
        try {
            messageFormat = msg.getLongProperty(messageFormatKey);
        } catch (MessageFormatException e) {
            return null;
        }

        Header header = new Header();
        Properties props=new Properties();
        HashMap daMap = null;
        HashMap maMap = null;
        HashMap apMap = null;
        Section body=null;
        HashMap footerMap = null;
        if( msg instanceof BytesMessage ) {
            BytesMessage m = (BytesMessage)msg;
            byte data[] = new byte[(int) m.getBodyLength()];
            m.readBytes(data);
            body = new Data(new Binary(data));
        } if( msg instanceof TextMessage ) {
            body = new AmqpValue(((TextMessage) msg).getText());
        } if( msg instanceof MapMessage ) {
            throw new RuntimeException("Not implemented");
        } if( msg instanceof StreamMessage ) {
            throw new RuntimeException("Not implemented");
        } if( msg instanceof ObjectMessage ) {
            throw new RuntimeException("Not implemented");
        }

        header.setDurable(msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? true : false);
        header.setPriority(new UnsignedByte((byte) msg.getJMSPriority()));
        if( msg.getJMSExpiration() != 0 ) {
            header.setTtl(new UnsignedInteger((int) msg.getJMSExpiration()));
        }
        if( msg.getJMSType()!=null ) {
            if( maMap==null ) maMap = new HashMap();
            maMap.put("x-opt-jms-type", msg.getJMSType());
        }
        if( msg.getJMSMessageID()!=null ) {
            props.setMessageId(msg.getJMSMessageID());
        }
        if( msg.getJMSDestination()!=null ) {
            props.setTo(vendor.toAddress(msg.getJMSDestination()));
        }
        if( msg.getJMSReplyTo()!=null ) {
            props.setReplyTo(vendor.toAddress(msg.getJMSDestination()));
        }
        if( msg.getJMSCorrelationID()!=null ) {
            props.setCorrelationId(msg.getJMSCorrelationID());
        }
        if( msg.getJMSExpiration() != 0 ) {
            props.setAbsoluteExpiryTime(new Date(msg.getJMSExpiration()));
        }
        if( msg.getJMSTimestamp()!= 0 ) {
            props.setCreationTime(new Date(msg.getJMSTimestamp()));
        }

        final Enumeration keys = msg.getPropertyNames();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if( key.equals(messageFormatKey) || key.equals(nativeKey)) {
                // skip..
            } else if( key.equals(firstAcquirerKey) ) {
                header.setFirstAcquirer(msg.getBooleanProperty(key));
            } else if( key.startsWith("JMSXDeliveryCount") ) {
                header.setDeliveryCount(new UnsignedInteger(msg.getIntProperty(key)));
            } else if( key.startsWith("JMSXUserID") ) {
                props.setUserId(new Binary(msg.getStringProperty(key).getBytes("UTF-8")));
            } else if( key.startsWith("JMSXGroupID") ) {
                props.setGroupId(msg.getStringProperty(key));
            } else if( key.startsWith("JMSXGroupSeq") ) {
                props.setGroupSequence(new UnsignedInteger(msg.getIntProperty(key)));
            } else if( key.startsWith(prefixDeliveryAnnotationsKey) ) {
                if( daMap == null ) daMap = new HashMap();
                String name = key.substring(prefixDeliveryAnnotationsKey.length());
                daMap.put(name, msg.getObjectProperty(key));
            } else if( key.startsWith(prefixMessageAnnotationsKey) ) {
                if( maMap==null ) maMap = new HashMap();
                String name = key.substring(prefixMessageAnnotationsKey.length());
                maMap.put(name, msg.getObjectProperty(key));
            } else if( key.equals(subjectKey) ) {
                props.setSubject(msg.getStringProperty(key));
            } else if( key.equals(contentTypeKey) ) {
                props.setContentType(Symbol.getSymbol(msg.getStringProperty(key)));
            } else if( key.equals(contentEncodingKey) ) {
                props.setContentEncoding(Symbol.getSymbol(msg.getStringProperty(key)));
            } else if( key.equals(replyToGroupIDKey) ) {
                props.setReplyToGroupId(msg.getStringProperty(key));
            } else if( key.startsWith(prefixFooterKey) ) {
                if( footerMap==null ) footerMap = new HashMap();
                String name = key.substring(prefixFooterKey.length());
                footerMap.put(name, msg.getObjectProperty(key));
            } else {
                if( apMap==null ) apMap = new HashMap();
                apMap.put(key, msg.getObjectProperty(key));
            }
        }


        MessageAnnotations ma=null;
        if( maMap!=null ) ma = new MessageAnnotations(maMap);
        DeliveryAnnotations da=null;
        if( daMap!=null ) da = new DeliveryAnnotations(daMap);
        ApplicationProperties ap=null;
        if( apMap!=null ) ap = new ApplicationProperties(apMap);
        Footer footer=null;
        if( footerMap!=null ) footer = new Footer(footerMap);

        org.apache.qpid.proton.message.Message amqp = new org.apache.qpid.proton.message.Message(header, da, ma, props, ap, body, footer);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[1024*4]);
        final DroppingWritableBuffer overflow = new DroppingWritableBuffer();
        int c = amqp.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
        if( overflow.position() > 0 ) {
            buffer = ByteBuffer.wrap(new byte[1024*4+overflow.position()]);
            c = amqp.encode(new WritableBuffer.ByteBufferWrapper(buffer));
        }

        return new EncodedMessage(messageFormat, buffer.array(), 0, c);
    }
}
