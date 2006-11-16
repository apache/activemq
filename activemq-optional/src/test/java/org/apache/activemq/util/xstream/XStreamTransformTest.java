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

import junit.framework.TestCase;

import javax.jms.*;

import org.apache.activemq.*;

/**
 * @version $Revision$
 */
public class XStreamTransformTest extends TestCase {
    protected ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    protected Connection connection;
    protected long timeout = 5000;

    public void testSendObjectMessageReceiveAsTextMessageAndObjectMessage() throws Exception {
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


    protected void setUp() throws Exception {
        connectionFactory.setTransformer(new XStreamMessageTransformer());
        connection = connectionFactory.createConnection();
        connection.start();
    }


    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
