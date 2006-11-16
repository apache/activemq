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
 * Transforms object messages to text messages using {@link XStream}
 *
 * @version $Revision$
 */
public class XStreamMessageTransformer extends MessageTransformerSupport {
    private XStream xStream;


    public Message producerTransform(Session session, MessageProducer producer, Message message) throws JMSException {
        if (message instanceof ObjectMessage) {
            TextMessage answer = session.createTextMessage(marshall(session, producer, (ObjectMessage) message));
            copyProperties(message, answer);
            return answer;
        }
        return message;
    }


    public Message consumerTransform(Session session, MessageConsumer consumer, Message message) throws JMSException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            Object object = unmarshall(session, consumer, textMessage);
            if (object instanceof Serializable) {
                ObjectMessage answer = session.createObjectMessage((Serializable) object);
                copyProperties(message, answer);
                return answer;
            }
            else {
                throw new JMSException("Object is not serializable: " + object);
            }
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

    /**
     * Marshalls the Object in the {@link ObjectMessage} to a string using XML encoding
     */
    protected String marshall(Session session, MessageProducer producer, ObjectMessage objectMessage) throws JMSException {
        Serializable object = objectMessage.getObject();
        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out = new PrettyPrintWriter(buffer);
        getXStream().marshal(object, out);
        return buffer.toString();
    }

    /**
     * Unmarshalls the Object using XML encoding of the String
     */
    protected Object unmarshall(Session session, MessageConsumer consumer, TextMessage textMessage) throws JMSException {
        HierarchicalStreamReader in = new XppReader(new StringReader(textMessage.getText()));
        return getXStream().unmarshal(in);
    }

}
