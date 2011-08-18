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
package org.apache.activemq;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// see: https://issues.apache.org/activemq/browse/AMQ-2651
public class OnePrefetchAsyncConsumerTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(OnePrefetchAsyncConsumerTest.class);

    private TestMutex testMutex;
    protected Connection connection;
    protected ConnectionConsumer connectionConsumer;
    protected Queue queue;
    protected CountDownLatch messageTwoDelay = new CountDownLatch(1);

    public void testPrefetchExtension() throws Exception {
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(queue);

        // when Msg1 is acked, the PrefetchSubscription will (incorrectly?) increment its prefetchExtension
        producer.send(session.createTextMessage("Msg1"));

        // Msg2 will exhaust the ServerSessionPool (since it only has 1 ServerSession)
        producer.send(session.createTextMessage("Msg2"));

        // Msg3 will cause the test to fail as it will attempt to retrieve an additional ServerSession from
        // an exhausted ServerSessionPool due to the (incorrectly?) incremented prefetchExtension in the PrefetchSubscription
        producer.send(session.createTextMessage("Msg3"));

        session.commit();

        // wait for test to complete and the test result to get set
        // this happens asynchronously since the messages are delivered asynchronously
        synchronized (testMutex) {
           while (!testMutex.testCompleted) {
              testMutex.wait();
           }
        }

        //test completed, result is ready
        assertTrue("Attempted to retrieve more than one ServerSession at a time", testMutex.testSuccessful);
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    protected void setUp() throws Exception {
        setAutoFail(true);
        bindAddress = "tcp://localhost:0";
        super.setUp();

        testMutex = new TestMutex();
        connection = createConnection();
        queue = createQueue();
        // note the last arg of 1, this becomes the prefetchSize in PrefetchSubscription
        connectionConsumer = connection.createConnectionConsumer(
           queue, null, new TestServerSessionPool(connection), 1);
        connection.start();
    }

    protected void tearDown() throws Exception {
        connectionConsumer.close();
        connection.close();
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = super.createBroker();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        // ensure prefetch is exact. only delivery next when current is acked
        defaultEntry.setUsePrefetchExtension(false);
        policyMap.setDefaultEntry(defaultEntry);
        answer.setDestinationPolicy(policyMap);
        return answer;
    }

    protected Queue createQueue() {
        return new ActiveMQQueue(getDestinationString());
    }

    // simulates a ServerSessionPool with only 1 ServerSession
    private class TestServerSessionPool implements ServerSessionPool {
         Connection connection;
         TestServerSession serverSession;
         boolean serverSessionInUse = false;

         public TestServerSessionPool(Connection connection) throws JMSException {
             this.connection = connection;
             serverSession = new TestServerSession(this);
         }

         public ServerSession getServerSession() throws JMSException {
             synchronized (this) {
                 if (serverSessionInUse) {
                     LOG.info("asked for session while in use, not serialised delivery");
                     synchronized (testMutex) {
                        testMutex.testSuccessful = false;
                        testMutex.testCompleted = true;
                     }
                 }
                 serverSessionInUse = true;
                 return serverSession;
             }
         }
    }

    private class TestServerSession implements ServerSession {
         TestServerSessionPool pool;
         Session session;

         public TestServerSession(TestServerSessionPool pool) throws JMSException {
             this.pool = pool;
             session = pool.connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
             session.setMessageListener(new TestMessageListener());
         }

         public Session getSession() throws JMSException {
             return session;
         }

         public void start() throws JMSException {
             // use a separate thread to process the message asynchronously
             new Thread() {
                 public void run() {
                     // let the session deliver the message
                     session.run();

                     // commit the tx and
                     // return ServerSession to pool
                     synchronized (pool) {
                        try {
                            session.commit();
                        }
                        catch (JMSException e) {
                        }
                        pool.serverSessionInUse = false;
                     }

                     // let the test check if the test was completed
                     synchronized (testMutex) {
                         testMutex.notify();
                     }
                 }
              }.start();
         }
    }

    private class TestMessageListener implements MessageListener {
        public void onMessage(Message message) {
            try {
               String text = ((TextMessage)message).getText();
               LOG.info("got message: " + text);
               if (text.equals("Msg3")) {
                  // if we get here, Exception in getServerSession() was not thrown, test is successful
                  // this obviously doesn't happen now,
                  // need to fix prefetchExtension computation logic in PrefetchSubscription to get here
                  synchronized (testMutex) {
                      if (!testMutex.testCompleted) {
                          testMutex.testSuccessful = true;
                          testMutex.testCompleted = true;
                      }
                  }
               }
               else if (text.equals("Msg2")) {
                  // simulate long message processing so that Msg3 comes when Msg2 is still being processed
                  // and thus the single ServerSession is in use
                  TimeUnit.SECONDS.sleep(4);
               }
            }
            catch (JMSException e) {
            }
            catch (InterruptedException e) {
            }
        }
    }

    private class TestMutex {
        boolean testCompleted = false;
        boolean testSuccessful = true;
    }
}
