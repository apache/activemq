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

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.oxm.AbstractMarshaller;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.xml.transform.StringResult;
import org.springframework.xml.transform.StringSource;

/**
 * Transforms object messages to text messages and vice versa using {@link Spring OXM}
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
		StringResult result = new StringResult();
		try {
			marshaller.marshal(objectMessage.getObject(), result);
			return result.toString();
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
			return marshaller.unmarshal(new StringSource(textMessage.getText()));
		} catch (Exception e) {
			throw new JMSException(e.getMessage());
		}
	}

}
