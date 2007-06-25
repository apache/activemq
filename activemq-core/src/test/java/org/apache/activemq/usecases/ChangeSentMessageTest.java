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

package org.apache.activemq.usecases;
import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.test.TestSupport;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ChangeSentMessageTest extends TestSupport {
    private static final int COUNT = 200;
    private static final String VALUE_NAME = "value";

    /**
     * test Object messages can be changed after sending with no side-affects
     * @throws Exception
     */
    public void testDoChangeSentMessage() throws Exception {
        Destination destination = createDestination("test-"+ChangeSentMessageTest.class.getName());
        Connection connection = createConnection();
        connection.start();
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Session publisherSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = publisherSession.createProducer(destination);
        HashMap map = new HashMap();
        ObjectMessage message = publisherSession.createObjectMessage();
        for (int i = 0;i < COUNT;i++) {
            map.put(VALUE_NAME, Integer.valueOf(i));
            message.setObject(map);
            producer.send(message);
            assertTrue(message.getObject()==map);
        }
        for (int i = 0;i < COUNT;i++) {
            ObjectMessage msg = (ObjectMessage) consumer.receive();
            HashMap receivedMap = (HashMap) msg.getObject();
            Integer intValue = (Integer) receivedMap.get(VALUE_NAME);
            assertTrue(intValue.intValue() == i);
        }
    }
}
