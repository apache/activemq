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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.leveldb.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a test case for the issue reported at:
 * https://issues.apache.org/activemq/browse/AMQ-1866
 *
 * If you have a JMS producer sending messages to multiple fast consumers and
 * one slow consumer, eventually all consumers will run as slow as
 * the slowest consumer.
 */
public class AMQ1866 extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerThread.class);
    private BrokerService brokerService;
    private ArrayList<Thread> threads = new ArrayList<Thread>();

    private final String ACTIVEMQ_BROKER_BIND = "tcp://localhost:0";
    private String ACTIVEMQ_BROKER_URI;

    AtomicBoolean shutdown = new AtomicBoolean();
    private ActiveMQQueue destination;

    @Override
    protected void setUp() throws Exception {
        // Start an embedded broker up.
        brokerService = new BrokerService();
        LevelDBStore adaptor = new LevelDBStore();
        brokerService.setPersistenceAdapter(adaptor);
        brokerService.deleteAllMessages();

        // A small max page size makes this issue occur faster.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry pe = new PolicyEntry();
        pe.setMaxPageSize(1);
        policyMap.put(new ActiveMQQueue(">"), pe);
        brokerService.setDestinationPolicy(policyMap);

        brokerService.addConnector(ACTIVEMQ_BROKER_BIND);
        brokerService.start();

        ACTIVEMQ_BROKER_URI = brokerService.getTransportConnectors().get(0).getPublishableConnectString();
        destination = new ActiveMQQueue(getName());
    }

    @Override
    protected void tearDown() throws Exception {
        // Stop any running threads.
        shutdown.set(true);
        for (Thread t : threads) {
            t.interrupt();
            t.join();
        }
        brokerService.stop();
    }

    public void testConsumerSlowDownPrefetch0() throws Exception {
        ACTIVEMQ_BROKER_URI = ACTIVEMQ_BROKER_URI + "?jms.prefetchPolicy.queuePrefetch=0";
        doTestConsumerSlowDown();
    }

    public void testConsumerSlowDownPrefetch10() throws Exception {
        ACTIVEMQ_BROKER_URI = ACTIVEMQ_BROKER_URI + "?jms.prefetchPolicy.queuePrefetch=10";
        doTestConsumerSlowDown();
    }

    public void testConsumerSlowDownDefaultPrefetch() throws Exception {
        doTestConsumerSlowDown();
    }

    public void doTestConsumerSlowDown() throws Exception {

        // Preload the queue.
        produce(20000);

        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    while(!shutdown.get()) {
                        produce(1000);
                    }
                } catch (Exception e) {
                }
            }
        };
        threads.add(producer);
        producer.start();

        // This is the slow consumer.
        ConsumerThread c1 = new ConsumerThread("Consumer-1");
        threads.add(c1);
        c1.start();

        // Wait a bit so that the slow consumer gets assigned most of the messages.
        Thread.sleep(500);
        ConsumerThread c2 = new ConsumerThread("Consumer-2");
        threads.add(c2);
        c2.start();

        int totalReceived = 0;
        for ( int i=0; i < 30; i++) {
            Thread.sleep(1000);
            long c1Counter = c1.counter.getAndSet(0);
            long c2Counter = c2.counter.getAndSet(0);
            log.debug("c1: "+c1Counter+", c2: "+c2Counter);
            totalReceived += c1Counter;
            totalReceived += c2Counter;

            // Once message have been flowing for a few seconds, start asserting that c2 always gets messages.  It should be receiving about 100 / sec
            if( i > 10 ) {
                assertTrue("Total received=" + totalReceived + ", Consumer 2 should be receiving new messages every second.", c2Counter > 0);
            }
        }
    }

    public void produce(int count) throws Exception {
        Connection connection=null;
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ACTIVEMQ_BROKER_URI);
            factory.setDispatchAsync(true);

            connection = factory.createConnection();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            connection.start();

            for( int i=0 ; i< count; i++ ) {
                producer.send(session.createTextMessage(getName()+" Message "+(++i)));
            }

        } finally {
            try {
                connection.close();
            } catch (Throwable e) {
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
                            sleepingTime = 1000 * 1000;
                        } else {
                            sleepingTime = 1;
                        }
                        counter.incrementAndGet();
                        Thread.sleep(sleepingTime);
                    }
                }

            } catch (Exception e) {
            } finally {
                log.debug(getName() + ": is stopping");
                try {
                    connection.close();
                } catch (Throwable e) {
                }
            }
        }

    }

}
