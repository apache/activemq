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
package org.apache.activemq.util.xstream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import org.apache.activemq.MessageTransformerSupport;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Transforms object messages to text messages and vice versa using {@link XStream}
 *
 * @version $Revision$
 */
public class XStreamMessageTransformer extends MessageTransformerSupport {
    private XStream xStream;

    /**
     * Defines the type of transformation.
     * If XML (default),
     *  - producer transformation transforms from Object to XML.
     *  - consumer transformation transforms from XML to Object.
     * If OBJECT,
     *  - producer transformation transforms from XML to Object.
     *  - consumer transformation transforms from Object to XML.
     * If ADAPTIVE,
     *  - producer transformation transforms from Object to XML, or XML to Object
     *    depending on the type of the original message
     *  - consumer transformation transforms from XML to Object, or Object to XML
     *    depending on the type of the original message
	 */
	public enum MessageTransform {XML, OBJECT, ADAPTIVE};

    protected MessageTransform transformType;

    public XStreamMessageTransformer() {
		this(MessageTransform.XML);
	}

	public XStreamMessageTransformer(MessageTransform transformType) {
		this.transformType = transformType;
	}

    public Message consumerTransform(Session session, MessageConsumer consumer, Message message) throws JMSException {
		switch (transformType) {
            case XML:
                return (message instanceof TextMessage) ?
                        textToObject(session, (TextMessage)message) :
                        message;
            case OBJECT:
                return (message instanceof ObjectMessage) ?
                        objectToText(session, (ObjectMessage)message) :
                        message;
            case ADAPTIVE:
                return (message instanceof TextMessage) ?
                        textToObject(session, (TextMessage)message) :
                       (message instanceof ObjectMessage) ?
                        objectToText(session, (ObjectMessage)message) :
                        message;
        }
        return message;
    }

	public Message producerTransform(Session session, MessageProducer producer, Message message) throws JMSException {
		switch (transformType) {
            case XML:
                return (message instanceof ObjectMessage) ?
                        objectToText(session, (ObjectMessage)message) :
                        message;
            case OBJECT:
                return (message instanceof TextMessage) ?
                        textToObject(session, (TextMessage)message) :
                        message;
            case ADAPTIVE:
                return (message instanceof TextMessage) ?
                        textToObject(session, (TextMessage)message) :
                       (message instanceof ObjectMessage) ?
                        objectToText(session, (ObjectMessage)message) :
                        message;
        }
        return message;
	}

    // Properties
    // -------------------------------------------------------------------------
    public XStream getXStream() {
        if (xStream == null) {
            xStream = createXStream();
        }
        return xStream;
    }

    public void setXStream(XStream xStream) {
        this.xStream = xStream;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected XStream createXStream() {
        return new XStream();
    }

    public MessageTransform getTransformType() {
        return transformType;
    }

    public void setTransformType(MessageTransform transformType) {
        this.transformType = transformType;
    }

    /**
     * Transforms an incoming XML encoded {@link TextMessage} to an {@link ObjectMessage}
     * @param session - JMS session currently being used
     * @param textMessage - text message to transform to object message
     * @return ObjectMessage
     * @throws JMSException
     */
    protected ObjectMessage textToObject(Session session, TextMessage textMessage) throws JMSException {
        Object object = unmarshall(session, textMessage);
        if (object instanceof Serializable) {
            ObjectMessage answer = session.createObjectMessage((Serializable) object);
            copyProperties(textMessage, answer);
            return answer;
        }
        else {
            throw new JMSException("Object is not serializable: " + object);
        }
	}

    /**
     * Transforms an incoming {@link ObjectMessage} to an XML encoded {@link TextMessage}
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
     * Marshalls the Object in the {@link ObjectMessage} to a string using XML encoding
     */
    protected String marshall(Session session, ObjectMessage objectMessage) throws JMSException {
        Serializable object = objectMessage.getObject();
        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out = new PrettyPrintWriter(buffer);
        getXStream().marshal(object, out);
        return buffer.toString();
    }


	/**
     * Unmarshalls the XML encoded message in the {@link TextMessage} to an Object
     */
    protected Object unmarshall(Session session, TextMessage textMessage) throws JMSException {
        HierarchicalStreamReader in = new XppReader(new StringReader(textMessage.getText()));
        return getXStream().unmarshal(in);
    }

}
