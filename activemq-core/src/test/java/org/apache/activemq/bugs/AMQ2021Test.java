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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a test case for the issue reported at:
 * https://issues.apache.org/activemq/browse/AMQ-2021 
 * Bug is modification of inflight message properties so the failure can manifest itself in a bunch
 * or ways, from message receipt with null properties to marshall errors
 */
public class AMQ2021Test extends TestCase implements ExceptionListener, UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(AMQ2021Test.class);
    BrokerService brokerService;
    ArrayList<Thread> threads = new ArrayList<Thread>();
    Vector<Throwable> exceptions;
    
    AMQ2021Test testCase;
    
    String ACTIVEMQ_BROKER_BIND = "tcp://localhost:61616";    
    String ACTIVEMQ_BROKER_URL = ACTIVEMQ_BROKER_BIND + "?jms.redeliveryPolicy.maximumRedeliveries=1&jms.redeliveryPolicy.initialRedeliveryDelay=0";
    
    private int numMessages = 1000;
    private int numConsumers = 2;
    private int dlqMessages = numMessages/2;
    
    CountDownLatch receivedLatch;
    private ActiveMQTopic destination;
    public CountDownLatch started;

    @Override
    protected void setUp() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(this);
        testCase = this;
        
        // Start an embedded broker up.
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.addConnector(ACTIVEMQ_BROKER_BIND);
        brokerService.start();
        destination = new ActiveMQTopic(getName());
        exceptions = new Vector<Throwable>();
        
        receivedLatch = 
            new CountDownLatch(numConsumers * (numMessages + dlqMessages));
        started = new CountDownLatch(1);
    }
    
    @Override
    protected void tearDown() throws Exception {
        for (Thread t : threads) {
            t.interrupt();
            t.join();
        }        
        brokerService.stop();
    }

    public void testConcurrentTopicResendToDLQ() throws Exception {

        for (int i=0; i<numConsumers;i++) {
            ConsumerThread c1 = new ConsumerThread("Consumer-" + i);
            threads.add(c1);
            c1.start();
        }
        
        assertTrue(started.await(10, TimeUnit.SECONDS));
        
        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    produce(numMessages);
                } catch (Exception e) {
                }
            }
        };
        threads.add(producer);
        producer.start();
        
        boolean allGood = receivedLatch.await(30, TimeUnit.SECONDS);
        for (Throwable t: exceptions) {
            log.error("failing test with first exception", t);
            fail("exception during test : " + t);
        }        
        assertTrue("excepted messages received within time limit", allGood);
        
        assertEquals(0, exceptions.size());
    
        for (int i=0; i<numConsumers; i++) {
            // last recovery sends message to deq so is not received again
            assertEquals(dlqMessages*2, ((ConsumerThread)threads.get(i)).recoveries);
            assertEquals(numMessages + dlqMessages, ((ConsumerThread)threads.get(i)).counter);
        }
       
        // half of the messages for each consumer should go to the dlq but duplicates will
        // be suppressed
        consumeFromDLQ(dlqMessages);
        
    }    
    
    private void consumeFromDLQ( int messageCount) throws Exception {
        ActiveMQConnectionFactory connectionFactory = 
            new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();
                    
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer dlqConsumer = session.createConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));
        int count = 0;
        for (int i=0; i< messageCount; i++) {
            if (dlqConsumer.receive(1000) == null) {
                break;
            }
            count++;
        }
        assertEquals(messageCount, count);
    }

    public void produce(int count) throws Exception {
        Connection connection=null;
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_BIND);
            connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            producer.setTimeToLive(0);
            connection.start();
            
            for (int i=0 ; i< count; i++) {
                int id = i+1;
                TextMessage message = session.createTextMessage(getName()+" Message "+ id);
                message.setIntProperty("MsgNumber", id);
                producer.send(message);
                
                if (id % 500 == 0) {
                    log.info("sent " + id + ", ith " + message);
                }
            }
        } catch (JMSException e) {
            log.error("unexpected ex on produce", e);
            exceptions.add(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Throwable e) {
            }
        }
    }
    
    public class ConsumerThread extends Thread implements MessageListener   {
        public long counter = 0;
        public long recoveries = 0;
        private Session session;

        public ConsumerThread(String threadId) {
            super(threadId);
        }

        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = 
                    new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_URL);
                Connection connection = connectionFactory.createConnection();
                connection.setExceptionListener(testCase);
                connection.setClientID(getName());            
                session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageConsumer consumer = session.createDurableSubscriber(destination, getName());                            
                consumer.setMessageListener(this);
                connection.start();
                
                started .countDown();
            
            } catch (JMSException exception) {
                log.error("unexpected ex in consumer run", exception);
                exceptions.add(exception);
            }
        }
        
        public void onMessage(Message message) {
            try {
                counter++;
                int messageNumber=message.getIntProperty("MsgNumber");
                if(messageNumber%2==0){
                    session.recover();
                    recoveries++;
                } else {
                    message.acknowledge();
                }     
                
                if (counter % 200 == 0) {
                    log.info("recoveries:" + recoveries + ", Received " + counter + ", counter'th " + message);
                }
                receivedLatch.countDown();
            }catch (Exception e) {
                log.error("unexpected ex on onMessage", e);
                exceptions.add(e);
            }
        }
        
    }

    public void onException(JMSException exception) {
        log.info("Unexpected JMSException", exception);
        exceptions.add(exception);
    }

    public void uncaughtException(Thread thread, Throwable exception) {
        log.info("Unexpected exception from thread " + thread + ", ex: " + exception);
        exceptions.add(exception);
    }

}
