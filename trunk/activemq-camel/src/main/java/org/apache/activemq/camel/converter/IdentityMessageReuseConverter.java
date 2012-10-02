/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel.converter;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQMessage;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.util.ObjectUtils;

/**
 * Identity conversion, return the original ActiveMQMessage as is, useful when camel does message
 * redelivery routing. ReadOnlyPropertes flag inverted to allow 
 * additional properties to be appended or existing properties to be modified
 */
public class IdentityMessageReuseConverter implements MessageConverter {

    /* (non-Javadoc)
     * @see org.springframework.jms.support.converter.MessageConverter#fromMessage(javax.jms.Message)
     */
    public Object fromMessage(Message message) throws JMSException, MessageConversionException {
        return message;
    }

    /* (non-Javadoc)
     * @see org.springframework.jms.support.converter.MessageConverter#toMessage(java.lang.Object, javax.jms.Session)
     */
    public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {
        if (object instanceof ActiveMQMessage) {
            // allow setting additional properties
            ((ActiveMQMessage)object).setReadOnlyProperties(false);
            return (Message)object;
        } else {
            throw new MessageConversionException("Cannot reuse object of type [" +
                    ObjectUtils.nullSafeClassName(object) + "] as ActiveMQMessage message. Message must already be an ActiveMQMessage.");
        }
    }
}
