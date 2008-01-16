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

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XppReader;

/**
 * Frame translator implementation that uses XStream to convert messages to and from XML and JSON
 * @author <a href="mailto:dejan@nighttale.net">Dejan Bosanac</a> 
 */
public class XStreamFrameTranslator extends LegacyFrameTranslator {

	XStream xStream = new XStream();
	
	public ActiveMQMessage convertFrame(ProtocolConverter converter,
			StompFrame command) throws JMSException, ProtocolException {
        Map headers = command.getHeaders();
        ActiveMQMessage msg;
        if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH)) {
        	msg = super.convertFrame(converter, command);
        } else {
        	try {
        		ActiveMQObjectMessage objMsg = new ActiveMQObjectMessage();
        		Object obj = unmarshall(new String(command.getContent(), "UTF-8"), (String)headers.get(Stomp.Headers.TRANSFORMATION));
        		objMsg.setObject((Serializable)obj);
        		msg = objMsg;
        	} catch (Throwable e) {
        		msg = super.convertFrame(converter, command);
            }
        }
        FrameTranslator.Helper.copyStandardHeadersFromFrameToMessage(converter, command, msg, this);
        return msg;
	}

	public StompFrame convertMessage(ProtocolConverter converter,
			ActiveMQMessage message) throws IOException, JMSException {
        if (message.getDataStructureType() == ActiveMQObjectMessage.DATA_STRUCTURE_TYPE) {
        	StompFrame command = new StompFrame();
            command.setAction(Stomp.Responses.MESSAGE);
            Map<String, String> headers = new HashMap<String, String>(25);
            command.setHeaders(headers);

            FrameTranslator.Helper.copyStandardHeadersFromMessageToFrame(converter, message, command, this);
            ActiveMQObjectMessage msg = (ActiveMQObjectMessage)message.copy();
            command.setContent(marshall(msg.getObject(), headers.get(Stomp.Headers.TRANSFORMATION)).getBytes("UTF-8"));
            return command;

        } else {
        	return super.convertMessage(converter, message);
        }
	}
	
    /**
     * Marshalls the Object to a string using XML or JSON
     * encoding
     */
    protected String marshall(Serializable object, String transformation) throws JMSException {
        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out;
        if (transformation.equalsIgnoreCase("jms-json")) {
        	out = new JettisonMappedXmlDriver().createWriter(buffer);
        } else {
        	out = new PrettyPrintWriter(buffer);
        }
        getXStream().marshal(object, out);
        return buffer.toString();
    }
    
    /**
     * Unmarshalls the XML or JSON encoded message to an
     * Object
     */
    protected Object unmarshall(String text, String transformation) {
    	HierarchicalStreamReader in;
    	if (transformation.equalsIgnoreCase("jms-json")) {
    		in = new JettisonMappedXmlDriver().createReader(new StringReader(text));
    	} else {
    		in = new XppReader(new StringReader(text));
    	}
        return getXStream().unmarshal(in);
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

}
