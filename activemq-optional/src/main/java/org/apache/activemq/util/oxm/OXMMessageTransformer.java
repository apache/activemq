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

import java.io.StringReader;
import java.io.StringWriter;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.springframework.oxm.support.AbstractMarshaller;


/**
 * Transforms object messages to text messages and vice versa using Spring OXM.
 *
 */
public class OXMMessageTransformer extends AbstractXMLMessageTransformer {

	/**
	 * OXM marshaller used to marshall/unmarshall messages
	 */
	private AbstractMarshaller marshaller;
	
	public AbstractMarshaller getMarshaller() {
		return marshaller;
	}

	public void setMarshaller(AbstractMarshaller marshaller) {
		this.marshaller = marshaller;
	}

    /**
     * Marshalls the Object in the {@link ObjectMessage} to a string using XML
     * encoding
     */
	protected String marshall(Session session, ObjectMessage objectMessage)
			throws JMSException {
		try {
            StringWriter writer = new StringWriter();
            Result result = new StreamResult(writer);
            marshaller.marshal(objectMessage.getObject(), result);
            writer.flush();
			return writer.toString();
		} catch (Exception e) {
			throw new JMSException(e.getMessage());
		}
	} 
	
    /**
     * Unmarshalls the XML encoded message in the {@link TextMessage} to an
     * Object
     */
	protected Object unmarshall(Session session, TextMessage textMessage)
			throws JMSException {
		try {
            String text = textMessage.getText();
            Source source = new StreamSource(new StringReader(text));
			return marshaller.unmarshal(source);
		} catch (Exception e) {
			throw new JMSException(e.getMessage());
		}
	}

}
