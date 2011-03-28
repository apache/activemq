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

import junit.framework.TestCase;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;


public class AMQ1917Test extends TestCase {

        private static final int NUM_MESSAGES = 4000;
        private static final int NUM_THREADS = 10;
        public static final String REQUEST_QUEUE = "mock.in.queue";
        public static final String REPLY_QUEUE = "mock.out.queue";

        Destination requestDestination = ActiveMQDestination.createDestination(
                REQUEST_QUEUE, ActiveMQDestination.QUEUE_TYPE);
        Destination replyDestination = ActiveMQDestination.createDestination(
                REPLY_QUEUE, ActiveMQDestination.QUEUE_TYPE);

        CountDownLatch roundTripLatch = new CountDownLatch(NUM_MESSAGES);
        CountDownLatch errorLatch = new CountDownLatch(1);
        ThreadPoolExecutor tpe;
        final String BROKER_URL = "tcp://localhost:61616";
        BrokerService broker = null;
        private boolean working = true;
        
        // trival session/producer pool
        final Session[] sessions = new Session[NUM_THREADS];
        final MessageProducer[] producers = new MessageProducer[NUM_THREADS];

        public void setUp() throws Exception {
            broker = new BrokerService();
            broker.setPersistent(false);
            broker.addConnector(BROKER_URL);
            broker.start();
            
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(10000);
            tpe = new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 60000,
                    TimeUnit.MILLISECONDS, queue);
            ThreadFactory limitedthreadFactory = new LimitedThreadFactory(tpe.getThreadFactory());  
            tpe.setThreadFactory(limitedthreadFactory);
        }

        public void tearDown() throws Exception {
            broker.stop();
            tpe.shutdown();
        }
        
        public void testLoadedSendRecieveWithCorrelationId() throws Exception {            
           
            ActiveMQConnectionFactory connectionFactory = new org.apache.activemq.ActiveMQConnectionFactory();
            connectionFactory.setBrokerURL(BROKER_URL);
            Connection connection = connectionFactory.createConnection();          
            setupReceiver(connection);

            connection = connectionFactory.createConnection();
            connection.start();
            
            // trival session/producer pool   
            for (int i=0; i<NUM_THREADS; i++) {
                sessions[i] = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producers[i] = sessions[i].createProducer(requestDestination);
            }
            
            for (int i = 0; i < NUM_MESSAGES; i++) {
                MessageSenderReceiver msr = new MessageSenderReceiver(requestDestination,
                        replyDestination, "Test Message : " + i);
                tpe.execute(msr);
            }
            
            while (!roundTripLatch.await(4000, TimeUnit.MILLISECONDS)) {
                if (errorLatch.await(1000, TimeUnit.MILLISECONDS)) {
                    fail("there was an error, check the console for thread or thread allocation failure");
                    break;
                }
            }
            working = false;
        }

        private void setupReceiver(final Connection connection) throws Exception {

            final Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session
                    .createConsumer(requestDestination);
            final MessageProducer sender = session.createProducer(replyDestination);
            connection.start();

            new Thread() {
                public void run() {
                    while (working) {
                        // wait for messages in infinitive loop
                        // time out is set to show the client is awaiting
                        try {
                            TextMessage msg = (TextMessage) consumer.receive(20000);
                            if (msg == null) {
                                errorLatch.countDown();
                                fail("Response timed out." 
                                        + " latchCount=" + roundTripLatch.getCount());
                            } else {
                                String result = msg.getText();
                                //System.out.println("Request:" + (i++)
                                //        + ", msg=" + result + ", ID" + msg.getJMSMessageID());
                                TextMessage response = session.createTextMessage();
                                response.setJMSCorrelationID(msg.getJMSMessageID());
                                response.setText(result);
                                sender.send(response);
                            }
                        } catch (JMSException e) {
                            if (working) {
                                errorLatch.countDown();
                                fail("Unexpected exception:" + e);
                            }
                        }
                    }
                }
            }.start();
        }

        class MessageSenderReceiver implements Runnable {

            Destination reqDest;
            Destination replyDest;
            String origMsg;

            public MessageSenderReceiver(Destination reqDest,
                    Destination replyDest, String msg) throws Exception {
                this.replyDest = replyDest;
                this.reqDest = reqDest;
                this.origMsg = msg;
            }

            private int getIndexFromCurrentThread() {
                String name = Thread.currentThread().getName();
                String num = name.substring(name.lastIndexOf('-') +1);
                int idx = Integer.parseInt(num) -1;
                assertTrue("idx is in range: idx=" + idx,  idx < NUM_THREADS);
                return idx;
            }

            public void run() {
                try {
                    // get thread session and producer from pool
                    int threadIndex = getIndexFromCurrentThread();
                    Session session = sessions[threadIndex];
                    MessageProducer producer = producers[threadIndex];

                    final Message sendJmsMsg = session.createTextMessage(origMsg);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    producer.send(sendJmsMsg);

                    String jmsId = sendJmsMsg.getJMSMessageID();
                    String selector = "JMSCorrelationID='" + jmsId + "'";

                    MessageConsumer consumer = session.createConsumer(replyDest,
                            selector);
                    Message receiveJmsMsg = consumer.receive(2000);
                    consumer.close();
                    if (receiveJmsMsg == null) {
                        errorLatch.countDown();
                        fail("Unable to receive response for:" + origMsg
                                + ", with selector=" + selector);
                    } else {
                        //System.out.println("received response message :"
                        //        + ((TextMessage) receiveJmsMsg).getText()
                        //        + " with selector : " + selector);
                        roundTripLatch.countDown();
                    }
                } catch (JMSException e) {
                    fail("unexpected exception:" + e);
                }
            }
        }
        
        public class LimitedThreadFactory implements ThreadFactory {
            int threadCount;
            private ThreadFactory factory;
            public LimitedThreadFactory(ThreadFactory threadFactory) {
                this.factory = threadFactory;
            }

            public Thread newThread(Runnable arg0) {
                if (++threadCount > NUM_THREADS) {
                    errorLatch.countDown();
                    fail("too many threads requested");
                }       
                return factory.newThread(arg0);
            }
        }
    }

