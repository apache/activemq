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

package org.apache.activemq.util.oxm;

import java.io.Serializable;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.MessageTransformerSupport;

/**
 * Abstract class used as a base for implementing transformers from object to text messages (in XML/JSON format)
 * and vice versa using.
 * Supports plugging of custom marshallers
 */
public abstract class AbstractXMLMessageTransformer extends
		MessageTransformerSupport {

    protected MessageTransform transformType;

    /**
     * Defines the type of transformation. If XML (default), - producer
     * transformation transforms from Object to XML. - consumer transformation
     * transforms from XML to Object. If OBJECT, - producer transformation
     * transforms from XML to Object. - consumer transformation transforms from
     * Object to XML. If ADAPTIVE, - producer transformation transforms from
     * Object to XML, or XML to Object depending on the type of the original
     * message - consumer transformation transforms from XML to Object, or
     * Object to XML depending on the type of the original message
     */
    public enum MessageTransform {
        XML, OBJECT, ADAPTIVE
    };


    public AbstractXMLMessageTransformer() {
        this(MessageTransform.XML);
    }

    public AbstractXMLMessageTransformer(MessageTransform transformType) {
        this.transformType = transformType;
    }

    public Message consumerTransform(Session session, MessageConsumer consumer, Message message) throws JMSException {
        switch (transformType) {
        case XML:
            return (message instanceof TextMessage) ? textToObject(session, (TextMessage)message) : message;
        case OBJECT:
            return (message instanceof ObjectMessage) ? objectToText(session, (ObjectMessage)message) : message;
        case ADAPTIVE:
            return (message instanceof TextMessage) ? textToObject(session, (TextMessage)message) : (message instanceof ObjectMessage) ? objectToText(session, (ObjectMessage)message) : message;
        default:
        }
        return message;
    }

    public Message producerTransform(Session session, MessageProducer producer, Message message) throws JMSException {
        switch (transformType) {
        case XML:
            return (message instanceof ObjectMessage) ? objectToText(session, (ObjectMessage)message) : message;
        case OBJECT:
            return (message instanceof TextMessage) ? textToObject(session, (TextMessage)message) : message;
        case ADAPTIVE:
            return (message instanceof TextMessage) ? textToObject(session, (TextMessage)message) : (message instanceof ObjectMessage) ? objectToText(session, (ObjectMessage)message) : message;
        default:
        }
        return message;
    }

    public MessageTransform getTransformType() {
        return transformType;
    }

    public void setTransformType(MessageTransform transformType) {
        this.transformType = transformType;
    }

    /**
     * Transforms an incoming XML encoded {@link TextMessage} to an
     * {@link ObjectMessage}
     * 
     * @param session - JMS session currently being used
     * @param textMessage - text message to transform to object message
     * @return ObjectMessage
     * @throws JMSException
     */
    protected ObjectMessage textToObject(Session session, TextMessage textMessage) throws JMSException {
        Object object = unmarshall(session, textMessage);
        if (object instanceof Serializable) {
            ObjectMessage answer = session.createObjectMessage((Serializable)object);
            copyProperties(textMessage, answer);
            return answer;
        } else {
            throw new JMSException("Object is not serializable: " + object);
        }
    }

    /**
     * Transforms an incoming {@link ObjectMessage} to an XML encoded
     * {@link TextMessage}
     * 
     * @param session - JMS session currently being used
     * @param objectMessage - object message to transform to text message
     * @return XML encoded TextMessage
     * @throws JMSException
     */
    protected TextMessage objectToText(Session session, ObjectMessage objectMessage) throws JMSException {
        TextMessage answer = session.createTextMessage(marshall(session, objectMessage));
        copyProperties(objectMessage, answer);
        return answer;
    }

    /**
     * Marshalls the Object in the {@link ObjectMessage} to a string using XML
     * encoding
     */
    protected abstract String marshall(Session session, ObjectMessage objectMessage) throws JMSException;

    /**
     * Unmarshalls the XML encoded message in the {@link TextMessage} to an
     * Object
     */
    protected abstract Object unmarshall(Session session, TextMessage textMessage) throws JMSException;

}
