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
package org.apache.activemq.transport.stomp;

import static org.apache.activemq.transport.stomp.FrameTranslator.Helper.copyStandardHeadersFromFrameToMessage;
import static org.apache.activemq.transport.stomp.FrameTranslator.Helper.copyStandardHeadersFromMessageToFrame;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerContextAware;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.transport.stomp.Stomp.Headers;
import org.apache.activemq.transport.stomp.Stomp.Responses;
import org.apache.activemq.transport.stomp.Stomp.Transformations;
import org.codehaus.jettison.mapped.Configuration;
import org.fusesource.hawtbuf.UTF8Buffer;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.basic.AbstractSingleValueConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.json.JsonHierarchicalStreamDriver;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XppReader;
import com.thoughtworks.xstream.io.xml.xppdom.XppFactory;

/**
 * Frame translator implementation that uses XStream to convert messages to and
 * from XML and JSON
 */
public class JmsFrameTranslator extends LegacyFrameTranslator implements BrokerContextAware {

    XStream xStream = null;
    BrokerContext brokerContext;

    @Override
    public ActiveMQMessage convertFrame(ProtocolConverter converter, StompFrame command) throws JMSException, ProtocolException {
        Map<String, String> headers = command.getHeaders();
        ActiveMQMessage msg;
        String transformation = headers.get(Headers.TRANSFORMATION);
        if (headers.containsKey(Headers.CONTENT_LENGTH) || transformation.equals(Transformations.JMS_BYTE.toString())) {
            msg = super.convertFrame(converter, command);
        } else {
            HierarchicalStreamReader in;

            try {
                String text = new String(command.getContent(), "UTF-8");
                switch (Transformations.getValue(transformation)) {
                    case JMS_OBJECT_XML:
                        in = new XppReader(new StringReader(text), XppFactory.createDefaultParser());
                        msg = createObjectMessage(in);
                        break;
                    case JMS_OBJECT_JSON:
                        in = new JettisonMappedXmlDriver().createReader(new StringReader(text));
                        msg = createObjectMessage(in);
                        break;
                    case JMS_MAP_XML:
                        in = new XppReader(new StringReader(text), XppFactory.createDefaultParser());
                        msg = createMapMessage(in);
                        break;
                    case JMS_MAP_JSON:
                        in = new JettisonMappedXmlDriver().createReader(new StringReader(text));
                        msg = createMapMessage(in);
                        break;
                    default:
                        throw new Exception("Unknown transformation: " + transformation);
                }
            } catch (Throwable e) {
                command.getHeaders().put(Headers.TRANSFORMATION_ERROR, e.getMessage());
                msg = super.convertFrame(converter, command);
            }
        }

        copyStandardHeadersFromFrameToMessage(converter, command, msg, this);
        return msg;
    }

    @Override
    public StompFrame convertMessage(ProtocolConverter converter, ActiveMQMessage message) throws IOException, JMSException {

        StompFrame command = new StompFrame();
        command.setAction(Responses.MESSAGE);
        Map<String, String> headers = new HashMap<String, String>(25);
        command.setHeaders(headers);

        copyStandardHeadersFromMessageToFrame(converter, message, command, this);

        String transformation = headers.get(Headers.TRANSFORMATION);

        if (message.getDataStructureType() == ActiveMQObjectMessage.DATA_STRUCTURE_TYPE) {

            if (Transformations.JMS_XML.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_OBJECT_XML.toString());
            } else if (Transformations.JMS_JSON.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_OBJECT_JSON.toString());
            }

            if (!headers.containsKey(Headers.TRANSFORMATION)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_OBJECT_XML.toString());
            }

            ActiveMQObjectMessage msg = (ActiveMQObjectMessage) message.copy();
            command.setContent(marshall(msg.getObject(), headers.get(Headers.TRANSFORMATION)).getBytes("UTF-8"));

        } else if (message.getDataStructureType() == ActiveMQMapMessage.DATA_STRUCTURE_TYPE) {

            if (Transformations.JMS_XML.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_MAP_XML.toString());
            } else if (Transformations.JMS_JSON.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_MAP_JSON.toString());
            }

            if (!headers.containsKey(Headers.TRANSFORMATION)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_MAP_XML.toString());
            }

            ActiveMQMapMessage msg = (ActiveMQMapMessage) message.copy();
            command.setContent(marshall((Serializable) msg.getContentMap(), headers.get(Headers.TRANSFORMATION)).getBytes("UTF-8"));

        } else if (message.getDataStructureType() == ActiveMQMessage.DATA_STRUCTURE_TYPE && AdvisorySupport.ADIVSORY_MESSAGE_TYPE.equals(message.getType())) {

            if (Transformations.JMS_XML.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_ADVISORY_XML.toString());
            } else if (Transformations.JMS_JSON.equals(transformation)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_ADVISORY_JSON.toString());
            }

            if (!headers.containsKey(Headers.TRANSFORMATION)) {
                headers.put(Headers.TRANSFORMATION, Transformations.JMS_ADVISORY_JSON.toString());
            }

            String body = marshallAdvisory(message.getDataStructure(), headers.get(Headers.TRANSFORMATION));
            command.setContent(body.getBytes("UTF-8"));

        } else {
            command = super.convertMessage(converter, message);
        }

        return command;
    }

    /**
     * Marshal the Object to a string using XML or JSON encoding
     *
     * @param object
     *        the object to marshal
     * @param transformation
     *        the transformation to apply to the object.
     *
     * @returns the marshaled form of the given object, in JSON or XML.
     *
     * @throws JMSException if an error occurs during the marshal operation.
     */
    protected String marshall(Serializable object, String transformation) throws JMSException {
        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out;
        if (transformation.toLowerCase(Locale.ENGLISH).endsWith("json")) {
            out = new JettisonMappedXmlDriver(new Configuration(), false).createWriter(buffer);
        } else {
            out = new PrettyPrintWriter(buffer);
        }
        getXStream().marshal(object, out);
        return buffer.toString();
    }

    protected ActiveMQObjectMessage createObjectMessage(HierarchicalStreamReader in) throws JMSException {
        ActiveMQObjectMessage objMsg = new ActiveMQObjectMessage();
        Object obj = getXStream().unmarshal(in);
        objMsg.setObject((Serializable) obj);
        return objMsg;
    }

    @SuppressWarnings("unchecked")
    protected ActiveMQMapMessage createMapMessage(HierarchicalStreamReader in) throws JMSException {
        ActiveMQMapMessage mapMsg = new ActiveMQMapMessage();
        Map<String, Object> map = (Map<String, Object>) getXStream().unmarshal(in);
        for (String key : map.keySet()) {
            mapMsg.setObject(key, map.get(key));
        }
        return mapMsg;
    }

    protected String marshallAdvisory(final DataStructure ds, String transformation) {

        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out;
        if (transformation.toLowerCase(Locale.ENGLISH).endsWith("json")) {
            out = new JettisonMappedXmlDriver().createWriter(buffer);
        } else {
            out = new PrettyPrintWriter(buffer);
        }

        XStream xstream = getXStream();
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.aliasPackage("", "org.apache.activemq.command");
        xstream.marshal(ds, out);
        return buffer.toString();
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
    @SuppressWarnings("unchecked")
    protected XStream createXStream() {
        XStream xstream = null;
        if (brokerContext != null) {
            Map<String, XStream> beans = brokerContext.getBeansOfType(XStream.class);
            for (XStream bean : beans.values()) {
                if (bean != null) {
                    xstream = bean;
                    break;
                }
            }
        }

        if (xstream == null) {
            xstream = XStreamSupport.createXStream();
            xstream.ignoreUnknownElements();
        }

        // For any object whose elements contains an UTF8Buffer instance instead
        // of a String type we map it to String both in and out such that we don't
        // marshal UTF8Buffers out
        xstream.registerConverter(new AbstractSingleValueConverter() {

            @Override
            public Object fromString(String str) {
                return str;
            }

            @SuppressWarnings("rawtypes")
            @Override
            public boolean canConvert(Class type) {
                return type.equals(UTF8Buffer.class);
            }
        });

        xstream.alias("string", UTF8Buffer.class);

        return xstream;
    }

    @Override
    public void setBrokerContext(BrokerContext brokerContext) {
        this.brokerContext = brokerContext;
    }

    @Override
    public BrokerContext getBrokerContext() {
        return this.brokerContext;
    }

    /**
     * Return an Advisory message as a JSON formatted string
     *
     * @param ds
     *        the DataStructure instance that is being marshaled.
     *
     * @return the JSON marshaled form of the given DataStructure instance.
     */
    protected String marshallAdvisory(final DataStructure ds) {
        XStream xstream = new XStream(new JsonHierarchicalStreamDriver());
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.aliasPackage("", "org.apache.activemq.command");
        return xstream.toXML(ds);
    }
}
