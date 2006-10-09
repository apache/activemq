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
package org.apache.activemq.broker.region.cursors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
/**
 * @version $Revision: 1.3 $
 */
public class CursorDurableTest extends TestCase{
    
    protected static final Log log = LogFactory.getLog(CursorDurableTest.class);

    protected static final int MESSAGE_COUNT=50;
    protected static final int PREFETCH_SIZE = 5;
    protected BrokerService broker;
    protected String bindAddress="tcp://localhost:60706";
    protected int topicCount=0;

    public void testSendFirstThenConsume() throws Exception{
        ConnectionFactory factory=createConnectionFactory();
        Connection consumerConnection= getConsumerConnection(factory);
        //create durable subs
        MessageConsumer consumer = getConsumer(consumerConnection);
        consumerConnection.close();
        
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Session session = producerConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(getTopic(session));
        List senderList = new ArrayList();
        for (int i =0; i < MESSAGE_COUNT; i++) {
            Message msg=session.createTextMessage("test"+i);
            senderList.add(msg);
            producer.send(msg);
        }
        producerConnection.close();
        
        //now consume the messages
        consumerConnection= getConsumerConnection(factory);
        //create durable subs
        consumer = getConsumer(consumerConnection);
        List consumerList = new ArrayList();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = consumer.receive();
            consumerList.add(msg);
        }
        assertEquals(senderList,consumerList);
        consumerConnection.close();       
    }
    
    public void testSendWhilstConsume() throws Exception{
        ConnectionFactory factory=createConnectionFactory();
        Connection consumerConnection= getConsumerConnection(factory);
        //create durable subs
        MessageConsumer consumer = getConsumer(consumerConnection);
        consumerConnection.close();
        
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Session session = producerConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(getTopic(session));
        List senderList = new ArrayList();
        for (int i =0; i < MESSAGE_COUNT/10; i++) {
            TextMessage msg=session.createTextMessage("test"+i);
            senderList.add(msg);
            producer.send(msg);
        }
        
        
        //now consume the messages
        consumerConnection= getConsumerConnection(factory);
        //create durable subs
        consumer = getConsumer(consumerConnection);
        final List consumerList = new ArrayList();
        
        final CountDownLatch latch = new CountDownLatch(1);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message msg){
                try{
                    //sleep to act as a slow consumer
                    //which will force a mix of direct and polled dispatching
                    //using the cursor on the broker
                    Thread.sleep(50);
                }catch(Exception e){
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                consumerList.add(msg);
                if (consumerList.size()==MESSAGE_COUNT) {
                    latch.countDown();
                }
                
            }
            
        });
        for (int i =MESSAGE_COUNT/10; i < MESSAGE_COUNT; i++) {
            TextMessage msg=session.createTextMessage("test"+i);
            senderList.add(msg);
            producer.send(msg);
        }   
        
        
        latch.await(300000,TimeUnit.MILLISECONDS);
        assertEquals("Still dipatching - count down latch not sprung" , latch.getCount(),0);
        assertEquals("cosumerList - expected: " + MESSAGE_COUNT + " but was: " + consumerList.size(),consumerList.size(),senderList.size());
        assertEquals(senderList,consumerList);
        producerConnection.close();
        consumerConnection.close();       
    }
    
    

    protected Topic getTopic(Session session) throws JMSException{
        String topicName=getClass().getName();
        return session.createTopic(topicName);
    }
    
    protected Connection getConsumerConnection(ConnectionFactory fac) throws JMSException{
        Connection connection=fac.createConnection();
        connection.setClientID("testConsumer");
        connection.start();
        return connection;
        
    }
    
    protected MessageConsumer getConsumer(Connection connection) throws Exception{
        Session consumerSession = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Topic topic = getTopic(consumerSession);
        MessageConsumer  consumer = consumerSession.createDurableSubscriber(topic,"testConsumer");
        return consumer;
    }

    

    protected void setUp() throws Exception{
        if(broker==null){
            broker=createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception{
        super.tearDown();
        
        if(broker!=null){
          broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception{
        ActiveMQConnectionFactory cf=new ActiveMQConnectionFactory(bindAddress);
        Properties props = new Properties();
        props.setProperty("prefetchPolicy.durableTopicPrefetch","" + PREFETCH_SIZE);
        props.setProperty("prefetchPolicy.optimizeDurableTopicPrefetch","" + PREFETCH_SIZE);
        cf.setProperties(props);
        return cf;
    }
    
   

    protected BrokerService createBroker() throws Exception{
        BrokerService answer=new BrokerService();
        configureBroker(answer);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception{
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}
