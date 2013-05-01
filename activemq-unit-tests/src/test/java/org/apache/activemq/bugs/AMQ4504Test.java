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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertNotNull;

public class AMQ4504Test {

    BrokerService brokerService;

    @Before
    public void setup() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();
    }

    @After
    public void stop() throws Exception {
        brokerService.stop();
    }

    @Test
    public void testCompositeDestConsumer() throws Exception {

        final int numDests = 20;
        final int numMessages = 200;
        StringBuffer stringBuffer = new StringBuffer();
        for (int i=0; i<numDests; i++) {
            if (stringBuffer.length() != 0) {
                stringBuffer.append(',');
            }
            stringBuffer.append("ST." + i);
        }
        stringBuffer.append("?consumer.prefetchSize=100");
        ActiveMQQueue activeMQQueue = new ActiveMQQueue(stringBuffer.toString());
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        Connection connection = factory.createConnection();
        connection.start();
        MessageProducer producer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createProducer(activeMQQueue);
        for (int i=0; i<numMessages; i++) {
            producer.send(new ActiveMQTextMessage());
        }

        MessageConsumer consumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(activeMQQueue);
        try {
            for (int i=0; i< numMessages * numDests; i++) {
                assertNotNull("recieved:"  + i, consumer.receive(4000));
            }
        } finally {
            connection.close();
        }
    }
}
