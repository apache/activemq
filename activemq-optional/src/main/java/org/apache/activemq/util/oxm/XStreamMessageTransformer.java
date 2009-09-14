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
import java.io.StringReader;
import java.io.StringWriter;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.XppReader;

/**
 * Transforms object messages to text messages and vice versa using
 * {@link XStream}
 * 
 */
public class XStreamMessageTransformer extends AbstractXMLMessageTransformer {

    private XStream xStream;
    
    /**
     * Specialized driver to be used with stream readers and writers
     */
    private HierarchicalStreamDriver streamDriver;
    
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

    public HierarchicalStreamDriver getStreamDriver() {
		return streamDriver;
	}

	public void setStreamDriver(HierarchicalStreamDriver streamDriver) {
		this.streamDriver = streamDriver;
	}
	
	// Implementation methods
    // -------------------------------------------------------------------------
    protected XStream createXStream() {
        return new XStream();
    }	
	
    /**
     * Marshalls the Object in the {@link ObjectMessage} to a string using XML
     * encoding
     */
    protected String marshall(Session session, ObjectMessage objectMessage) throws JMSException {
        Serializable object = objectMessage.getObject();
        StringWriter buffer = new StringWriter();
        HierarchicalStreamWriter out;
        if (streamDriver != null) {
        	out = streamDriver.createWriter(buffer);
        } else {
        	out = new PrettyPrintWriter(buffer);
        }
        getXStream().marshal(object, out);
        return buffer.toString();
    }

    /**
     * Unmarshalls the XML encoded message in the {@link TextMessage} to an
     * Object
     */
    protected Object unmarshall(Session session, TextMessage textMessage) throws JMSException {
        HierarchicalStreamReader in;
        if (streamDriver != null) {
        	in = streamDriver.createReader(new StringReader(textMessage.getText()));
        } else {
        	in = new XppReader(new StringReader(textMessage.getText()));
        }
        return getXStream().unmarshal(in);
    }

}
