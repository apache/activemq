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

package org.apache.activemq.store;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract public class MessagePriorityTest extends CombinationTestSupport {
    
    private static final Log LOG = LogFactory.getLog(MessagePriorityTest.class);

    BrokerService broker;
    PersistenceAdapter adapter;
    
    protected ActiveMQConnectionFactory factory;
    protected Connection conn;
    protected Session sess;
    
    public boolean useCache = true;
    public boolean dispatchAsync = true;
    public boolean prioritizeMessages = true;
    public int prefetchVal = 500;

    public int MSG_NUM = 600;
    public int HIGH_PRI = 7;
    public int LOW_PRI = 3;
    
    abstract protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception;
    
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("priorityTest");
        broker.setAdvisorySupport(false);
        adapter = createPersistenceAdapter(true);
        broker.setPersistenceAdapter(adapter);
        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(prioritizeMessages);
        policy.setUseCache(useCache);
        StorePendingDurableSubscriberMessageStoragePolicy durableSubPending =
                new StorePendingDurableSubscriberMessageStoragePolicy();
        durableSubPending.setImmediatePriorityDispatch(true);
        policy.setPendingDurableSubscriberPolicy(durableSubPending);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(new ActiveMQQueue("TEST"), policy);
        policyMap.put(new ActiveMQTopic("TEST"), policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
        
        factory = new ActiveMQConnectionFactory("vm://priorityTest");
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setAll(prefetchVal);
        factory.setPrefetchPolicy(prefetch);
        factory.setWatchTopicAdvisories(false);
        factory.setDispatchAsync(dispatchAsync);
        conn = factory.createConnection();
        conn.setClientID("priority");
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    
    protected void tearDown() throws Exception {
        try {
            sess.close();
            conn.close();
        } catch (Exception ignored) {
        } finally {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
    
    public void testStoreConfigured() throws Exception {
        Queue queue = sess.createQueue("TEST");
        Topic topic = sess.createTopic("TEST");
        
        MessageProducer queueProducer = sess.createProducer(queue);
        MessageProducer topicProducer = sess.createProducer(topic);
        
        
        Thread.sleep(500); // get it all propagated
        
        assertTrue(broker.getRegionBroker().getDestinationMap().get(queue).getMessageStore().isPrioritizedMessages());
        assertTrue(broker.getRegionBroker().getDestinationMap().get(topic).getMessageStore().isPrioritizedMessages());
        
        queueProducer.close();
        topicProducer.close();
        
    }
    
    protected class ProducerThread extends Thread {

        int priority;
        int messageCount;
        ActiveMQDestination dest;
        
        public ProducerThread(ActiveMQDestination dest, int messageCount, int priority) {
            this.messageCount = messageCount;
            this.priority = priority;
            this.dest = dest;
        }
        
        public void run() {
            try {
                MessageProducer producer = sess.createProducer(dest);
                producer.setPriority(priority);
                for (int i = 0; i < messageCount; i++) {
                    producer.send(sess.createTextMessage("message priority: " + priority));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void setMessagePriority(int priority) {
            this.priority = priority;
        }

        public void setMessageCount(int messageCount) {
            this.messageCount = messageCount;    
        }
        
    }
    
    public void initCombosForTestQueues() {
        addCombinationValues("useCache", new Object[] {new Boolean(true), new Boolean(false)});
    }
    
    public void testQueues() throws Exception {
        ActiveMQQueue queue = (ActiveMQQueue)sess.createQueue("TEST");

        ProducerThread lowPri = new ProducerThread(queue, MSG_NUM, LOW_PRI);
        ProducerThread highPri = new ProducerThread(queue, MSG_NUM, HIGH_PRI);
        
        lowPri.start();
        highPri.start();
        
        lowPri.join();
        highPri.join();
        
        MessageConsumer queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < MSG_NUM * 2; i++) {
            Message msg = queueConsumer.receive(5000);
            LOG.debug("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", i < MSG_NUM ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
        }
    }
    
    protected Message createMessage(int priority) throws Exception {
        final String text = "priority " + priority;
        Message msg = sess.createTextMessage(text);
        LOG.info("Sending  " + text);
        return msg;
    }

    public void initCombosForTestDurableSubs() {
        addCombinationValues("prefetchVal", new Object[] {new Integer(1000), new Integer(MSG_NUM/4)});
    }
    
    public void testDurableSubs() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic)sess.createTopic("TEST");
        TopicSubscriber sub = sess.createDurableSubscriber(topic, "priority");
        sub.close();
        
        ProducerThread lowPri = new ProducerThread(topic, MSG_NUM, LOW_PRI);
        ProducerThread highPri = new ProducerThread(topic, MSG_NUM, HIGH_PRI);
        
        lowPri.start();
        highPri.start();
        
        lowPri.join();
        highPri.join();
        
        sub = sess.createDurableSubscriber(topic, "priority");
        for (int i = 0; i < MSG_NUM * 2; i++) {
            Message msg = sub.receive(5000);
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", i < MSG_NUM ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
        }


        // verify that same broker/store can deal with non priority dest also
        topic = (ActiveMQTopic)sess.createTopic("HAS_NO_PRIORITY");
        sub = sess.createDurableSubscriber(topic, "no_priority");
        sub.close();

        lowPri = new ProducerThread(topic, MSG_NUM, LOW_PRI);
        highPri = new ProducerThread(topic, MSG_NUM, HIGH_PRI);

        lowPri.start();
        highPri.start();

        lowPri.join();
        highPri.join();

        sub = sess.createDurableSubscriber(topic, "no_priority");
        // verify we got them all
        for (int i = 0; i < MSG_NUM * 2; i++) {
            Message msg = sub.receive(5000);
            assertNotNull("Message " + i + " was null", msg);
        }

    }

    public void initCombosForTestDurableSubsReconnect() {
        addCombinationValues("prefetchVal", new Object[] {new Integer(1000), new Integer(MSG_NUM/2)});
        // REVISIT = is dispatchAsync = true a problem or is it just the test?
        addCombinationValues("dispatchAsync", new Object[] {Boolean.FALSE});
        addCombinationValues("useCache", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testDurableSubsReconnect() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic)sess.createTopic("TEST");
        final String subName = "priorityDisconnect";
        TopicSubscriber sub = sess.createDurableSubscriber(topic, subName);
        sub.close();

        ProducerThread lowPri = new ProducerThread(topic, MSG_NUM, LOW_PRI);
        ProducerThread highPri = new ProducerThread(topic, MSG_NUM, HIGH_PRI);

        lowPri.start();
        highPri.start();

        lowPri.join();
        highPri.join();


        final int closeFrequency = MSG_NUM/4;
        sub = sess.createDurableSubscriber(topic, subName);
        for (int i = 0; i < MSG_NUM * 2; i++) {
            Message msg = sub.receive(15000);
            LOG.debug("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", i < MSG_NUM ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
            if (i>0 && i%closeFrequency==0) {
                LOG.info("Closing durable sub.. on: " + i);
                sub.close();
                sub = sess.createDurableSubscriber(topic, subName);
            }
        }
    }

    public void testHighPriorityDelivery() throws Exception {

        // get zero prefetch
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setAll(0);
        factory.setPrefetchPolicy(prefetch);
        conn.close();
        conn = factory.createConnection();
        conn.setClientID("priority");
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQTopic topic = (ActiveMQTopic)sess.createTopic("TEST");
        final String subName = "priorityDisconnect";
        TopicSubscriber sub = sess.createDurableSubscriber(topic, subName);
        sub.close();

        ProducerThread producerThread = new ProducerThread(topic, 5000, LOW_PRI);
        producerThread.run();
        LOG.info("Low priority messages sent");

        sub = sess.createDurableSubscriber(topic, subName);
        for (int i=0; i<200;i++) {
            Message msg = sub.receive(15000);
            LOG.debug("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", LOW_PRI, msg.getJMSPriority());
        }

        producerThread.setMessagePriority(HIGH_PRI);
        producerThread.setMessageCount(1);
        producerThread.run();
        LOG.info("High priority message sent");

        // try and get the high priority message
        Message msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        LOG.info("received: " + msg);
        assertEquals("high priority", HIGH_PRI, msg.getJMSPriority());
    }
    
}
