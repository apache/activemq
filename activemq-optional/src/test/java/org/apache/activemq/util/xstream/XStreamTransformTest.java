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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;

import static org.apache.activemq.util.xstream.XStreamMessageTransformer.MessageTransform.*;

/**
 * @version $Revision$
 */
public class XStreamTransformTest extends TestCase {
    protected ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    protected Connection connection;
    protected long timeout = 5000;

    public void testSendObjectMessageReceiveAsTextMessageAndObjectMessage() throws Exception {
        connectionFactory.setTransformer(new XStreamMessageTransformer(XML));
        connection = connectionFactory.createConnection();
        connection.start();

        // lets create the consumers
        Session objectSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = objectSession.createTopic(getClass().getName());
        MessageConsumer objectConsumer = objectSession.createConsumer(destination);

        Session textSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer textConsumer = textSession.createConsumer(destination);
        // lets clear the transformer on this consumer so we see the message as it really is
        ((ActiveMQMessageConsumer) textConsumer).setTransformer(null);


        // send a message
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);

        ObjectMessage request = producerSession.createObjectMessage(new SamplePojo("James", "London"));
        producer.send(request);


        // lets consume it as an object message
        Message message = objectConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be an ObjectMessage but was: " + message, message instanceof ObjectMessage);
        ObjectMessage objectMessage = (ObjectMessage) message;
        Object object = objectMessage.getObject();
        assertTrue("object payload of wrong type: " + object, object instanceof SamplePojo);
        SamplePojo body = (SamplePojo) object;
        assertEquals("name", "James", body.getName());
        assertEquals("city", "London", body.getCity());


        // lets consume it as a text message
        message = textConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be a TextMessage but was: " + message, message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        String text = textMessage.getText();
        assertTrue("Text should be non-empty!", text != null && text.length() > 0);
        System.out.println("Received XML...");
        System.out.println(text);
    }

    public void testSendTextMessageReceiveAsObjectMessageAndTextMessage() throws Exception {
        connectionFactory.setTransformer(new XStreamMessageTransformer(OBJECT));
        connection = connectionFactory.createConnection();
        connection.start();

        // lets create the consumers
        Session textSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = textSession.createTopic(getClass().getName());
        MessageConsumer textConsumer = textSession.createConsumer(destination);

        Session objectSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer objectConsumer = objectSession.createConsumer(destination);
        // lets clear the transformer on this consumer so we see the message as it really is
        ((ActiveMQMessageConsumer) objectConsumer).setTransformer(null);


        // send a message
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);

        String xmlText =
                "<org.apache.activemq.util.xstream.SamplePojo>" +
                "<name>James</name>" +
                "<city>London</city>" +
                "</org.apache.activemq.util.xstream.SamplePojo>";

        TextMessage request = producerSession.createTextMessage(xmlText);
        producer.send(request);

        Message message;
        // lets consume it as a text message
        message = textConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be a TextMessage but was: " + message, message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        String text = textMessage.getText();
        assertTrue("Text should be non-empty!", text != null && text.length() > 0);

        // lets consume it as an object message
        message = objectConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be an ObjectMessage but was: " + message, message instanceof ObjectMessage);
        ObjectMessage objectMessage = (ObjectMessage) message;
        Object object = objectMessage.getObject();
        assertTrue("object payload of wrong type: " + object, object instanceof SamplePojo);
        SamplePojo body = (SamplePojo) object;
        assertEquals("name", "James", body.getName());
        assertEquals("city", "London", body.getCity());

    }

    public void testAdaptiveTransform() throws Exception {
        connectionFactory.setTransformer(new XStreamMessageTransformer(ADAPTIVE));
        connection = connectionFactory.createConnection();
        connection.start();

        // lets create the consumers
        Session adaptiveSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = adaptiveSession.createTopic(getClass().getName());
        MessageConsumer adaptiveConsumer = adaptiveSession.createConsumer(destination);

        Session origSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer origConsumer = origSession.createConsumer(destination);
        // lets clear the transformer on this consumer so we see the message as it really is
        ((ActiveMQMessageConsumer) origConsumer).setTransformer(null);

        // Create producer
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);

        Message message;
        ObjectMessage objectMessage;
        TextMessage textMessage;
        SamplePojo body;
        Object object;
        String text;

        // Send a text message
        String xmlText =
                "<org.apache.activemq.util.xstream.SamplePojo>" +
                "<name>James</name>" +
                "<city>London</city>" +
                "</org.apache.activemq.util.xstream.SamplePojo>";

        TextMessage txtRequest = producerSession.createTextMessage(xmlText);
        producer.send(txtRequest);

        // lets consume it as a text message
        message = adaptiveConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be a TextMessage but was: " + message, message instanceof TextMessage);
        textMessage = (TextMessage) message;
        text = textMessage.getText();
        assertTrue("Text should be non-empty!", text != null && text.length() > 0);

        // lets consume it as an object message
        message = origConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be an ObjectMessage but was: " + message, message instanceof ObjectMessage);
        objectMessage = (ObjectMessage) message;
        object = objectMessage.getObject();
        assertTrue("object payload of wrong type: " + object, object instanceof SamplePojo);
        body = (SamplePojo) object;
        assertEquals("name", "James", body.getName());
        assertEquals("city", "London", body.getCity());

        // Send object message
        ObjectMessage objRequest = producerSession.createObjectMessage(new SamplePojo("James", "London"));
        producer.send(objRequest);

        // lets consume it as an object message
        message = adaptiveConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be an ObjectMessage but was: " + message, message instanceof ObjectMessage);
        objectMessage = (ObjectMessage) message;
        object = objectMessage.getObject();
        assertTrue("object payload of wrong type: " + object, object instanceof SamplePojo);
        body = (SamplePojo) object;
        assertEquals("name", "James", body.getName());
        assertEquals("city", "London", body.getCity());


        // lets consume it as a text message
        message = origConsumer.receive(timeout);
        assertNotNull("Should have received a message!", message);
        assertTrue("Should be a TextMessage but was: " + message, message instanceof TextMessage);
        textMessage = (TextMessage) message;
        text = textMessage.getText();
        assertTrue("Text should be non-empty!", text != null && text.length() > 0);
        System.out.println("Received XML...");
        System.out.println(text);

    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
