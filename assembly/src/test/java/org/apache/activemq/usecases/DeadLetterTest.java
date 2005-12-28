/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.activemq.TestSupport;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;


/**
 * @version $Revision: 1.1.1.1 $
 */
public class DeadLetterTest extends TestSupport {
    
    private static final int MESSAGE_COUNT = 10;
    private static final long TIME_TO_LIVE = 250;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    private ActiveMQDestination destination;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private boolean durableSubscriber = false;
   
    protected void setUp() throws Exception{
        super.setUp();
        connection = createConnection();
        connection.setClientID(toString());
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    
    protected void tearDown() throws Exception {
        connection.close();
    }
    
    protected void doTest() throws Exception{
        destination = (ActiveMQDestination) createDestination(getClass().getName());
        producer = session.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);
        producer.setTimeToLive(TIME_TO_LIVE);

        // TODO FIXME
        
        /*
        DeadLetterPolicy dlq = new DeadLetterPolicy();
        String dlqName = dlq.getDeadLetterNameFromDestination(destination);
        */
        String dlqName = "FOO";
        
        Destination dlqDestination = session.createQueue(dlqName);
        MessageConsumer dlqConsumer = session.createConsumer(dlqDestination); 
        
        if (durableSubscriber){
            consumer = session.createDurableSubscriber((Topic)destination, destination.toString());
        }else {
            consumer = session.createConsumer(destination);
        }
        
        for (int i =0; i < MESSAGE_COUNT;i++){
            Message message = session.createTextMessage("msg: " + i);
            producer.send(message);
        }
        Thread.sleep(TIME_TO_LIVE * 2);
        connection.start();
        
        
        for (int i =0; i < MESSAGE_COUNT; i++){
            Message msg = consumer.receive(10);
            assertNull("Should be null message", msg);
        }
        
        for (int i =0; i < MESSAGE_COUNT; i++){
            Message msg = dlqConsumer.receive(1000);
            assertNotNull("Should be  message", msg);
        }
        
        
    }
    
   
    
    public void testDurableTopicMessageExpiration() throws Exception{
        super.topic = true;
        deliveryMode = DeliveryMode.PERSISTENT;
        durableSubscriber = true;
        doTest();
    }
    
    public void testTransientQueueMessageExpiration() throws Exception{
        super.topic = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        durableSubscriber = false;
        doTest();
    }
    
    public void testDurableQueueMessageExpiration() throws Exception{
        super.topic = false;
        deliveryMode = DeliveryMode.PERSISTENT;
        durableSubscriber = false;
        doTest();
    }
   
}
