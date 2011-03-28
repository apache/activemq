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


import static org.junit.Assert.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;

public class AMQ2383Test {

    @Test
    public void activeMQTest() throws Exception {
        Destination dest = ActiveMQQueue.createDestination("testQueue", ActiveMQQueue.QUEUE_TYPE);
        ConnectionFactory factory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.useJmx=false&broker.persistent=false");
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(dest);
        TextMessage sentMsg = producerSession.createTextMessage("test...");
        producer.send(sentMsg);
        producerSession.close();

        Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(dest);
        TextMessage receivedMsg = (TextMessage)consumer.receive();
        consumerSession.rollback();
        consumerSession.close();

        assertEquals(sentMsg, receivedMsg);

        Thread.sleep(10000);

        producerConnection.close();
        consumerConnection.close();
    }
}
