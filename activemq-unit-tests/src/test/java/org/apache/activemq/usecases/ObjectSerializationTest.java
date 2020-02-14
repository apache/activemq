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

package org.apache.activemq.usecases;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.test.TestSupport;

/**
 * Test that java.util Object serialization is not allowed by default
 */
public class ObjectSerializationTest extends TestSupport {

    private static final String VALUE_NAME = "value";

    public void testDoChangeSentMessage() throws Exception {
        Destination destination = createDestination("test-" + ObjectSerializationTest.class.getName());
        Connection connection = createConnection();
        connection.start();
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Session publisherSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = publisherSession.createProducer(destination);
        
        HashMap<String, Integer> map = new HashMap<>();
        ObjectMessage message = publisherSession.createObjectMessage();
        map.put(VALUE_NAME, Integer.valueOf(1));
        message.setObject(map);
        producer.send(message);
        
        ObjectMessage msg = (ObjectMessage)consumer.receive();
        try {
            msg.getObject();
            fail("Failure expected on trying to deserialize a forbidden package");
        } catch (JMSException ex) {
            // expected
        }
    }
}
