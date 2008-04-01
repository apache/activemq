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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.5 $
 * A Test case for AMQ-1479
 */
public class DurableConsumerTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(DurableConsumerTest.class);
    private static int COUNT = 1024*10;
    private static String CONSUMER_NAME = "DURABLE_TEST";
    protected BrokerService broker;
   
    protected String bindAddress="tcp://localhost:61616";
  
    
   
    
  
    protected byte[] payload = new byte[1024*16];
    protected ConnectionFactory factory;
   
  
    
    public void testConsumer() throws Exception{
        factory = createConnectionFactory();
        Connection consumerConnection = factory.createConnection();
        consumerConnection.setClientID(CONSUMER_NAME);
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic  = consumerSession.createTopic(getClass().getName());
        MessageConsumer consumer = consumerSession.createDurableSubscriber(topic, CONSUMER_NAME);
        consumerConnection.start();
        consumerConnection.close();
        broker.stop();
        broker =createBroker(false);
        
        Connection producerConnection = factory.createConnection();
       
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        MessageProducer producer = producerSession.createProducer(topic);
        producerConnection.start();
        for (int i =0; i < COUNT;i++) {
            BytesMessage msg = producerSession.createBytesMessage();
            msg.writeBytes(payload);
            producer.send(msg);
            if (i != 0 && i%1000==0) {
                LOG.info("Sent msg " + i);
            }
        }
        producerConnection.close();
        broker.stop();
        broker =createBroker(false);
        
        consumerConnection = factory.createConnection();
        consumerConnection.setClientID(CONSUMER_NAME);
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
       
        consumer = consumerSession.createDurableSubscriber(topic, CONSUMER_NAME);
        consumerConnection.start();
        for (int i =0; i < COUNT;i++) {
            Message msg =  consumer.receive(5000);            
            assertNotNull("Missing message: "+i, msg);
            if (i != 0 && i%1000==0) {
                LOG.info("Received msg " + i);
            }
            
        }
        consumerConnection.close();
        
        
    }
    
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker(true);
        }
        
       
       
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    protected Topic creatTopic(Session s, String destinationName) throws JMSException {
        return s.createTopic(destinationName);
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     */
    protected BrokerService createBroker(boolean deleteStore) throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer,deleteStore);
        answer.start();
        return answer;
    }

    

    protected void configureBroker(BrokerService answer,boolean deleteStore) throws Exception {
        answer.setDeleteAllMessagesOnStartup(deleteStore);
        answer.addConnector(bindAddress);
        answer.setUseShutdownHook(false);
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(bindAddress);
    }

   
}
