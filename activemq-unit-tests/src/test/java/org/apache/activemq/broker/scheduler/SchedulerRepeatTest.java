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
package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.ServerSocket;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.scheduler.memory.InMemoryJobSchedulerStore;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;

public class SchedulerRepeatTest {
    
    private static BrokerService broker;
    private static String brokerAddress;
    
    @org.junit.BeforeClass
    public static void startBroker() throws Exception {
        
        broker = new BrokerService();
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
        broker.setJobSchedulerStore(new InMemoryJobSchedulerStore());
        broker.setDataDirectory("target/activemq-data");
        broker.setSchedulerSupport(true);
        
        ServerSocket serverSocket = new ServerSocket(0);
        int brokerPort = serverSocket.getLocalPort();
        serverSocket.close();
        
        brokerAddress = "tcp://localhost:" + brokerPort;
        broker.addConnector(brokerAddress);
        broker.start();
    }
    
    @org.junit.AfterClass
    public static void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }
    
    @org.junit.Test
    public void testSendLotsofMessages() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerAddress);
        Connection connection = factory.createConnection();
        connection.start();
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue("testqueue");
        MessageProducer producer = session.createProducer(queue);
        
        TextMessage message = session.createTextMessage("Some txt");
        message.setStringProperty("some header", "some value");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 0L);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 0L);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 2000);
        
        try {
            producer.send(message);
            fail("Failure expected on too large a repeat value");
        } catch (MessageFormatException ex) {
            assertEquals("The scheduled repeat value is too large", ex.getMessage());
        }

        connection.close();
    }
    
    @org.junit.Test
    public void testRepeat() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerAddress);
        Connection connection = factory.createConnection();
        connection.start();
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue("testqueue");
        MessageProducer producer = session.createProducer(queue);
        
        TextMessage message = session.createTextMessage("Some txt");
        message.setStringProperty("some header", "some value");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 0L);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 0L);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, 900);
        
        producer.send(message);

        connection.close();
    }
}
