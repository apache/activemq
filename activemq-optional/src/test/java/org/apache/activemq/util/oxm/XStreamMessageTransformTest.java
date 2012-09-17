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

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.util.xstream.SamplePojo;

import static org.apache.activemq.util.oxm.AbstractXMLMessageTransformer.MessageTransform.ADAPTIVE;

public class XStreamMessageTransformTest extends
		AbstractXMLMessageTransformerTest {

	protected AbstractXMLMessageTransformer createTransformer() {
		return new XStreamMessageTransformer();
	}

	public void testStreamDriverTransform() throws Exception {
		XStreamMessageTransformer transformer = (XStreamMessageTransformer) createTransformer();
		transformer.setTransformType(ADAPTIVE);
		transformer.setStreamDriver(new JettisonMappedXmlDriver());
		connection = createConnection(transformer);

		// lets create the consumers
		Session adaptiveSession = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = adaptiveSession.createTopic(getClass()
				.getName());
		MessageConsumer adaptiveConsumer = adaptiveSession
				.createConsumer(destination);

		Session origSession = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		MessageConsumer origConsumer = origSession.createConsumer(destination);
		// lets clear the transformer on this consumer so we see the message as
		// it really is
		((ActiveMQMessageConsumer) origConsumer).setTransformer(null);

		// Create producer
		Session producerSession = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = producerSession.createProducer(destination);

		Message message;
		ObjectMessage objectMessage;
		TextMessage textMessage;
		SamplePojo body;
		Object object;
		String text;

		// Send a text message
		String xmlText = "{\"org.apache.activemq.util.xstream.SamplePojo\":{\"name\":\"James\",\"city\":\"London\"}}";

		TextMessage txtRequest = producerSession.createTextMessage(xmlText);
		producer.send(txtRequest);

		// lets consume it as a text message
		message = adaptiveConsumer.receive(timeout);
		assertNotNull("Should have received a message!", message);
		assertTrue("Should be a TextMessage but was: " + message,
				message instanceof TextMessage);
		textMessage = (TextMessage) message;
		text = textMessage.getText();
		assertTrue("Text should be non-empty!", text != null
				&& text.length() > 0);

		// lets consume it as an object message
		message = origConsumer.receive(timeout);
		assertNotNull("Should have received a message!", message);
		assertTrue("Should be an ObjectMessage but was: " + message,
				message instanceof ObjectMessage);
		objectMessage = (ObjectMessage) message;
		object = objectMessage.getObject();
		assertTrue("object payload of wrong type: " + object,
				object instanceof SamplePojo);
		body = (SamplePojo) object;
		assertEquals("name", "James", body.getName());
		assertEquals("city", "London", body.getCity());

		// Send object message
		ObjectMessage objRequest = producerSession
				.createObjectMessage(new SamplePojo("James", "London"));
		producer.send(objRequest);

		// lets consume it as an object message
		message = adaptiveConsumer.receive(timeout);
		assertNotNull("Should have received a message!", message);
		assertTrue("Should be an ObjectMessage but was: " + message,
				message instanceof ObjectMessage);
		objectMessage = (ObjectMessage) message;
		object = objectMessage.getObject();
		assertTrue("object payload of wrong type: " + object,
				object instanceof SamplePojo);
		body = (SamplePojo) object;
		assertEquals("name", "James", body.getName());
		assertEquals("city", "London", body.getCity());

		// lets consume it as a text message
		message = origConsumer.receive(timeout);
		assertNotNull("Should have received a message!", message);
		assertTrue("Should be a TextMessage but was: " + message,
				message instanceof TextMessage);
		textMessage = (TextMessage) message;
		text = textMessage.getText();
		assertTrue("Text should be non-empty!", text != null
				&& text.length() > 0);
		System.out.println("Received JSON...");
		System.out.println(text);

	}

}
