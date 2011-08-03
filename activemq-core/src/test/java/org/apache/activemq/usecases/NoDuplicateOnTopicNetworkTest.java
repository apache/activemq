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
package org.apache.activemq.usecases;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.PriorityNetworkDispatchPolicy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoDuplicateOnTopicNetworkTest extends CombinationTestSupport {
    private static final Logger LOG = LoggerFactory
            .getLogger(NoDuplicateOnTopicNetworkTest.class);

    private static final String MULTICAST_DEFAULT = "multicast://default";
    private static final String BROKER_1 = "tcp://localhost:61626";
    private static final String BROKER_2 = "tcp://localhost:61636";
    private static final String BROKER_3 = "tcp://localhost:61646";
    private final static String TOPIC_NAME = "broadcast";
    private BrokerService broker1;
    private BrokerService broker2;
    private BrokerService broker3;

    public boolean suppressDuplicateTopicSubs = false;
    public DispatchPolicy dispatchPolicy = new SimpleDispatchPolicy();
    public boolean durableSub = false;
    AtomicInteger idCounter = new AtomicInteger(0);
    
    private boolean dynamicOnly = false;
    // no duplicates in cyclic network if networkTTL <=1
    // when > 1, subscriptions percolate around resulting in duplicates as there is no
    // memory of the original subscription.
    // solution for 6.0 using org.apache.activemq.command.ConsumerInfo.getNetworkConsumerIds()
    private int ttl = 3;
    
    
  
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        broker3 = createAndStartBroker("broker3", BROKER_3);
        Thread.sleep(3000);
        broker2 = createAndStartBroker("broker2", BROKER_2);
        Thread.sleep(3000);
        broker1 = createAndStartBroker("broker1", BROKER_1);
        Thread.sleep(1000);
        
        waitForBridgeFormation();
    }
    
    public static Test suite() {
        return suite(NoDuplicateOnTopicNetworkTest.class);
    }
    
    protected void waitForBridgeFormation() throws Exception {
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return !broker3.getNetworkConnectors().get(0).activeBridges().isEmpty();
            }});
 
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return !broker2.getNetworkConnectors().get(0).activeBridges().isEmpty();
            }});

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return !broker1.getNetworkConnectors().get(0).activeBridges().isEmpty();
            }});
    }

    private BrokerService createAndStartBroker(String name, String addr)
            throws Exception {
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName(name);
        broker.addConnector(addr).setDiscoveryUri(new URI(MULTICAST_DEFAULT));
        broker.setUseJmx(false);

        NetworkConnector networkConnector = broker
                .addNetworkConnector(MULTICAST_DEFAULT);
        networkConnector.setDecreaseNetworkConsumerPriority(true);
        networkConnector.setDynamicOnly(dynamicOnly);
        networkConnector.setNetworkTTL(ttl);
        networkConnector.setSuppressDuplicateTopicSubscriptions(suppressDuplicateTopicSubs);

        
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(dispatchPolicy);
        // the audit will suppress the duplicates as it defaults to true so this test
        // checking for dups will fail. it is good to have it on in practice.
        policy.setEnableAudit(false);
        policyMap.put(new ActiveMQTopic(TOPIC_NAME), policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
       
        return broker;
    }

    @Override
    protected void tearDown() throws Exception {
        broker1.stop();
        broker2.stop();
        broker3.stop();
        super.tearDown();
    }

    public void initCombosForTestProducerConsumerTopic() {
        this.addCombinationValues("suppressDuplicateTopicSubs", new Object[]{Boolean.TRUE, Boolean.FALSE});
        this.addCombinationValues("dispatchPolicy", new Object[]{new PriorityNetworkDispatchPolicy(), new SimpleDispatchPolicy()});
        this.addCombinationValues("durableSub", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testProducerConsumerTopic() throws Exception {
        
        final CountDownLatch consumerStarted = new CountDownLatch(1);
        
        Thread producerThread = new Thread(new Runnable() {
            public void run() {
                TopicWithDuplicateMessages producer = new TopicWithDuplicateMessages();
                producer.setBrokerURL(BROKER_1);
                producer.setTopicName(TOPIC_NAME);
                try {
                    producer.produce();
                } catch (JMSException e) {
                    fail("Unexpected " + e);
                }
            }
        });

        final TopicWithDuplicateMessages consumer = new TopicWithDuplicateMessages();
        Thread consumerThread = new Thread(new Runnable() {
            public void run() {
                consumer.setBrokerURL(BROKER_2);
                consumer.setTopicName(TOPIC_NAME);
                try {
                    consumer.consumer();
                    consumerStarted.countDown();
                    consumer.getLatch().await(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    fail("Unexpected " + e);
                }
            }
        });

        consumerThread.start();
        LOG.info("Started Consumer");
        
        assertTrue("consumer started eventually", consumerStarted.await(10, TimeUnit.SECONDS));
        
        // ensure subscription has percolated though the network
        Thread.sleep(2000);
        
        producerThread.start();
        LOG.info("Started Producer");
        producerThread.join();
        consumerThread.join();

        int duplicateCount = 0;
        Map<String, String> map = new HashMap<String, String>();
        for (String msg : consumer.getMessageStrings()) {
            if (map.containsKey(msg)) {
                LOG.info("got duplicate: " + msg);
                duplicateCount++;
            }
            map.put(msg, msg);
        }
        consumer.unSubscribe();
        if (suppressDuplicateTopicSubs || dispatchPolicy instanceof PriorityNetworkDispatchPolicy) {
            assertEquals("no duplicates", 0, duplicateCount);
            assertEquals("got all required messages: " + map.size(), consumer
                    .getNumMessages(), map.size());
        } else {
            assertTrue("we can get some duplicates: " + duplicateCount, duplicateCount >= 0);
            if (duplicateCount == 0) {
               assertEquals("got all required messages: " + map.size(), consumer
                    .getNumMessages(), map.size()); 
            }
        }
    }

    class TopicWithDuplicateMessages {
        private String brokerURL;
        private String topicName;
        private Connection connection;
        private Session session;
        private Topic topic;
        private MessageProducer producer;
        private MessageConsumer consumer;
        private final String durableID = "DURABLE_ID";

        private List<String> receivedStrings = Collections.synchronizedList(new ArrayList<String>());
        private int numMessages = 10;
        private CountDownLatch recievedLatch = new CountDownLatch(numMessages);

        public CountDownLatch getLatch() {
            return recievedLatch;
        }
        
        public List<String> getMessageStrings() {
            synchronized(receivedStrings) {
                return new ArrayList<String>(receivedStrings);
            }
        }

        public String getBrokerURL() {
            return brokerURL;
        }

        public void setBrokerURL(String brokerURL) {
            this.brokerURL = brokerURL;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        private void createConnection() throws JMSException {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                    brokerURL);
            connection = factory.createConnection();
            connection.setClientID("ID" + idCounter.incrementAndGet());
        }

        private void createTopic() throws JMSException {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            topic = session.createTopic(topicName);
        }

        private void createProducer() throws JMSException {
            producer = session.createProducer(topic);
        }

        private void createConsumer() throws JMSException {
            if (durableSub) {
                consumer = session.createDurableSubscriber(topic, durableID);
            } else {
                consumer = session.createConsumer(topic);
            }
            consumer.setMessageListener(new MessageListener() {

                public void onMessage(Message arg0) {
                    TextMessage msg = (TextMessage) arg0;
                    try {
                        LOG.debug("Received message [" + msg.getText() + "]");
                        receivedStrings.add(msg.getText());
                        recievedLatch.countDown();
                    } catch (JMSException e) {
                        fail("Unexpected :" + e);
                    }
                }

            });
        }

        private void publish() throws JMSException {
            for (int i = 0; i < numMessages; i++) {
                TextMessage textMessage = session.createTextMessage();
                String message = "message: " + i;
                LOG.debug("Sending message[" + message + "]");
                textMessage.setText(message);
                producer.send(textMessage);
            }
        }

        public void produce() throws JMSException {
            createConnection();
            createTopic();
            createProducer();
            connection.start();
            publish();
        }

        public void consumer() throws JMSException {
            createConnection();
            createTopic();
            createConsumer();
            connection.start();
        }

        public int getNumMessages() {
            return numMessages;
        }

        public void unSubscribe() throws Exception {
            consumer.close();
            if (durableSub) {
                session.unsubscribe(durableID);
                // ensure un-subscription has percolated though the network
                Thread.sleep(2000);
            }
        }
    }
}
