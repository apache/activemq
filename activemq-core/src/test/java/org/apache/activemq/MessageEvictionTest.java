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

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.FilePendingSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.OldestMessageEvictionStrategy;
import org.apache.activemq.broker.region.policy.PendingSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Test;

public class MessageEvictionTest {
    static final Log LOG = LogFactory.getLog(MessageEvictionTest.class);
    private BrokerService broker;
    private ConnectionFactory connectionFactory;
    Connection connection;
    private Session session;
    private Topic destination;
    private final String destinationName = "verifyEvection";
    protected int numMessages = 2000;
    protected String payload = new String(new byte[1024*2]);

    public void setUp(PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy) throws Exception {
        broker = createBroker(pendingSubscriberPolicy);
        broker.start();
        connectionFactory = createConnectionFactory();
        connection = connectionFactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = session.createTopic(destinationName);
    }
    
    @After
    public void tearDown() throws Exception {
        connection.stop();
        broker.stop();
    }
    
    @Test
    public void testMessageEvictionMemoryUsageFileCursor() throws Exception {
        setUp(new FilePendingSubscriberMessageStoragePolicy());
        doTestMessageEvictionMemoryUsage();
    }
    
    @Test
    public void testMessageEvictionMemoryUsageVmCursor() throws Exception {
        setUp(new VMPendingSubscriberMessageStoragePolicy());
        doTestMessageEvictionMemoryUsage();
    }
    
    @Test
    public void testMessageEvictionDiscardedAdvisory() throws Exception {
        setUp(new VMPendingSubscriberMessageStoragePolicy());
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final CountDownLatch consumerRegistered = new CountDownLatch(1);
        final CountDownLatch gotAdvisory = new CountDownLatch(1);
        final CountDownLatch advisoryIsGood = new CountDownLatch(1);
        
        executor.execute(new Runnable() {
            public void run() {
                try {
                    ActiveMQTopic discardedAdvisoryDestination = 
                        AdvisorySupport.getMessageDiscardedAdvisoryTopic(destination);
                    // use separate session rather than asyncDispatch on consumer session 
                    // as we want consumer session to block
                    Session advisorySession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageConsumer consumer = advisorySession.createConsumer(discardedAdvisoryDestination);
                    consumer.setMessageListener(new MessageListener() {
                        int advisoriesReceived = 0;
                        public void onMessage(Message message) {
                            try {
                                LOG.info("advisory:" + message);
                                ActiveMQMessage activeMQMessage = (ActiveMQMessage) message;
                                assertNotNull(activeMQMessage.getStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID));
                                assertEquals(++advisoriesReceived, activeMQMessage.getIntProperty(AdvisorySupport.MSG_PROPERTY_DISCARDED_COUNT));
                                message.acknowledge();
                                advisoryIsGood.countDown();
                            } catch (JMSException e) {
                                e.printStackTrace();
                                fail(e.toString());
                            } finally {
                                gotAdvisory.countDown();
                            }
                        }           
                    });
                    consumerRegistered.countDown();
                    gotAdvisory.await(120, TimeUnit.SECONDS);
                    consumer.close();
                    advisorySession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.toString());
                }
            }
        });
        assertTrue("we have an advisory consumer", consumerRegistered.await(60, TimeUnit.SECONDS));
        doTestMessageEvictionMemoryUsage();
        assertTrue("got an advisory for discarded", gotAdvisory.await(0, TimeUnit.SECONDS));
        assertTrue("advisory is good",advisoryIsGood.await(0, TimeUnit.SECONDS));
    }
    
    public void doTestMessageEvictionMemoryUsage() throws Exception {
        
        ExecutorService executor = Executors.newCachedThreadPool();
        final CountDownLatch doAck = new CountDownLatch(1);
        final CountDownLatch ackDone = new CountDownLatch(1);
        final CountDownLatch consumerRegistered = new CountDownLatch(1);
        executor.execute(new Runnable() {
            public void run() {
                try {
                    final MessageConsumer consumer = session.createConsumer(destination);
                    consumer.setMessageListener(new MessageListener() {
                        public void onMessage(Message message) {
                            try {
                                // very slow, only ack once
                                doAck.await(60, TimeUnit.SECONDS);
                                LOG.info("acking: " + message.getJMSMessageID());
                                message.acknowledge();
                                ackDone.countDown();
                            } catch (Exception e) {
                                e.printStackTrace();   
                                fail(e.toString());
                            } finally {
                                consumerRegistered.countDown();
                                ackDone.countDown();
                            }
                        }           
                    });
                    consumerRegistered.countDown();
                    ackDone.await(60, TimeUnit.SECONDS);
                    consumer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.toString());
                }
            }
        });
        
        assertTrue("we have a consumer", consumerRegistered.await(10, TimeUnit.SECONDS));
        
        final AtomicInteger sent = new AtomicInteger(0);
        final CountDownLatch sendDone = new CountDownLatch(1);
        executor.execute(new Runnable() {
            public void run() {
               MessageProducer producer;
               try {
                   producer = session.createProducer(destination);
                   for (int i=0; i< numMessages; i++) {
                       producer.send(session.createTextMessage(payload));
                       sent.incrementAndGet();
                       TimeUnit.MILLISECONDS.sleep(10);
                   }
                   producer.close();
                   sendDone.countDown();
               } catch (Exception e) {
                   sendDone.countDown();
                   e.printStackTrace();
                   fail(e.toString());
               }
            }
        });
        
        assertTrue("messages sending done", sendDone.await(180, TimeUnit.SECONDS));
        assertEquals("all message were sent", numMessages, sent.get());
        
        doAck.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        
        assertTrue("usage goes to 0 once consumer goes away", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return 0 == TestSupport.getDestination(broker, 
                        ActiveMQDestination.transform(destination)).getMemoryUsage().getPercentUsage();
            }
        }));
    }

    BrokerService createBroker(PendingSubscriberMessageStoragePolicy pendingSubscriberPolicy) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setUseJmx(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        
        // spooling to disk early so topic memory limit is not reached
        brokerService.getSystemUsage().getMemoryUsage().setLimit(500*1024);
        
        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setTopic(">");
        
        entry.setAdvisoryForDiscardingMessages(true);
        
        // so consumer does not get over run while blocked limit the prefetch
        entry.setTopicPrefetch(50);
        
        
        entry.setPendingSubscriberPolicy(pendingSubscriberPolicy);
        
        // limit the number of outstanding messages, large enough to use the file store
        // or small enough not to blow memory limit
        int pendingMessageLimit = 50;
        if (pendingSubscriberPolicy instanceof FilePendingSubscriberMessageStoragePolicy) {
            pendingMessageLimit = 500;
        }
        ConstantPendingMessageLimitStrategy pendingMessageLimitStrategy = new ConstantPendingMessageLimitStrategy();
        pendingMessageLimitStrategy.setLimit(pendingMessageLimit);
        entry.setPendingMessageLimitStrategy(pendingMessageLimitStrategy);

        // to keep the limit in check and up to date rather than just the first few, evict some
        OldestMessageEvictionStrategy messageEvictionStrategy = new OldestMessageEvictionStrategy();
        // whether to check expiry before eviction, default limit 1000 is fine as no ttl set in this test
        //messageEvictionStrategy.setEvictExpiredMessagesHighWatermark(1000);
        entry.setMessageEvictionStrategy(messageEvictionStrategy);
        
        // let evicted messaged disappear
        entry.setDeadLetterStrategy(null);
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        brokerService.setDestinationPolicy(policyMap);
        
        return brokerService;
    }

    ConnectionFactory createConnectionFactory() throws Exception {
        String url = ((TransportConnector) broker.getTransportConnectors().get(0)).getServer().getConnectURI().toString();
        ActiveMQConnectionFactory factory =  new ActiveMQConnectionFactory(url);
        factory.setWatchTopicAdvisories(false);
        return factory;
    }

}
