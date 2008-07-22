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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a test case for the issue reported at:
 * https://issues.apache.org/activemq/browse/AMQ-1866
 * 
 * If you have a JMS producer sending messages to multiple fast consumers and 
 * one slow consumer, eventually all consumers will run as slow as 
 * the slowest consumer.  
 */
public class AMQ1866 extends TestCase {

    private static final Log log = LogFactory.getLog(ConsumerThread.class);
    private BrokerService brokerService;
    private ArrayList<Thread> threads = new ArrayList<Thread>();
    
    String ACTIVEMQ_BROKER_BIND = "tcp://localhost:61616";    
    String ACTIVEMQ_BROKER_URI = "tcp://localhost:61616";
    
    AtomicBoolean shutdown = new AtomicBoolean();
    private ActiveMQQueue destination;

    @Override
    protected void setUp() throws Exception {
        // Start an embedded broker up.
        brokerService = new BrokerService();
        brokerService.addConnector(ACTIVEMQ_BROKER_BIND);
        brokerService.start();
        destination = new ActiveMQQueue(getName());
    }
    
    @Override
    protected void tearDown() throws Exception {
        // Stop any running threads.
        shutdown.set(true);
        for (Thread t : threads) {
            t.join();
        }        
        brokerService.stop();
    }

    // Failing
    public void testConsumerSlowDownPrefetch0() throws InterruptedException {
        ACTIVEMQ_BROKER_URI = "tcp://localhost:61616?jms.prefetchPolicy.queuePrefetch=0";
        doTestConsumerSlowDown();
    }

    // Failing
    public void testConsumerSlowDownPrefetch10() throws InterruptedException {
        ACTIVEMQ_BROKER_URI = "tcp://localhost:61616?jms.prefetchPolicy.queuePrefetch=10";
        doTestConsumerSlowDown();
    }
    
    // Passing
    public void testConsumerSlowDownDefaultPrefetch() throws InterruptedException {
        ACTIVEMQ_BROKER_URI = "tcp://localhost:61616";
        doTestConsumerSlowDown();
    }

    public void doTestConsumerSlowDown() throws InterruptedException {
        ProducerThread p1 = new ProducerThread("Producer-1");
        threads.add(p1);
        p1.start();
        
        // Wait a bit before starting the consumers to load up the queues a bit..
        // If the queue is loaded up it seems that the even the Default Prefetch size case fails.
        Thread.sleep(10000);

        ConsumerThread c1 = new ConsumerThread("Consumer-1");
        threads.add(c1);
        c1.start();

        ConsumerThread c2 = new ConsumerThread("Consumer-2");
        threads.add(c2);
        c2.start();

        for ( int i=0; i < 30; i++) {
            Thread.sleep(1000);
            long p1Counter = p1.counter.getAndSet(0);
            long c1Counter = c1.counter.getAndSet(0);
            long c2Counter = c2.counter.getAndSet(0);
            System.out.println("p1: "+p1Counter+", c1: "+c1Counter+", c2: "+c2Counter);
            
            // Once message have been flowing for a few seconds, start asserting that c2 always gets messages.  It should be receiving about 100 / sec
            if( i > 3 ) {
                assertTrue("Consumer 2 should be receiving new messages every second.", c2Counter > 0);
            }
        }
    }    
    
    public class ProducerThread extends Thread {
        final AtomicLong counter = new AtomicLong();
        
        public ProducerThread(String threadId) {
            super(threadId);
        }

        public void run() {
            Connection connection=null;
            try {
                log.debug(getName() + ": is running");
                
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_URI);
                factory.setDispatchAsync(true);
                
                connection = factory.createConnection();
                
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(destination);
                connection.start();
                
                int i = 0;
                while (!shutdown.get()) {
                    producer.send(session.createTextMessage(getName()+" Message "+(++i)));
                    counter.incrementAndGet();
                    Thread.sleep(1);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (Throwable e) {
                }
            }
        }

    }

    public class ConsumerThread extends Thread {
        final AtomicLong counter = new AtomicLong();

        public ConsumerThread(String threadId) {
            super(threadId);
        }

        public void run() {
            Connection connection=null;
            try {
                log.debug(getName() + ": is running");
                
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_URI);
                factory.setDispatchAsync(true);
                
                connection = factory.createConnection();
                
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(destination);
                connection.start();
                
                while (!shutdown.get()) {
                    TextMessage msg = (TextMessage)consumer.receive(1000);
                    if ( msg!=null ) {
                        int sleepingTime;
                        if (getName().equals("Consumer-1")) {
                            sleepingTime = 10 * 1000;
                        } else {
                            sleepingTime = 1; 
                        }
                        Thread.sleep(sleepingTime);
                        counter.incrementAndGet();
                    }
                }
                
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (Throwable e) {
                }
            }
        }

    }

}
