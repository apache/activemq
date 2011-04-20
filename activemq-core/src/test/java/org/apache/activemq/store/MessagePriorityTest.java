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
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class MessagePriorityTest extends CombinationTestSupport {
    
    private static final Logger LOG = LoggerFactory.getLogger(MessagePriorityTest.class);

    BrokerService broker;
    PersistenceAdapter adapter;
    
    protected ActiveMQConnectionFactory factory;
    protected Connection conn;
    protected Session sess;
    
    public boolean useCache = true;
    public boolean dispatchAsync = true;
    public boolean prioritizeMessages = true;
    public boolean immediatePriorityDispatch = true;
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
        durableSubPending.setImmediatePriorityDispatch(immediatePriorityDispatch);
        durableSubPending.setUseCache(useCache);
        policy.setPendingDurableSubscriberPolicy(durableSubPending);
        PolicyMap policyMap = new PolicyMap();
        policyMap.put(new ActiveMQQueue("TEST"), policy);
        policyMap.put(new ActiveMQTopic("TEST"), policy);

        // do not process expired for one test
        PolicyEntry ignoreExpired = new PolicyEntry();
        SharedDeadLetterStrategy ignoreExpiredStrategy = new SharedDeadLetterStrategy();
        ignoreExpiredStrategy.setProcessExpired(false);
        ignoreExpired.setDeadLetterStrategy(ignoreExpiredStrategy);
        policyMap.put(new ActiveMQTopic("TEST_CLEANUP_NO_PRIORITY"), ignoreExpired);

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

        final int numToProduce = 2000;
        final int[] dups = new int[numToProduce*2];
        ProducerThread producerThread = new ProducerThread(topic, numToProduce, LOW_PRI+1);
        producerThread.run();
        LOG.info("Low priority messages sent");

        sub = sess.createDurableSubscriber(topic, subName);
        final int batchSize = 250;
        int lowLowCount = 0;
        for (int i=0; i<numToProduce; i++) {
            Message msg = sub.receive(15000);
            LOG.info("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() + ", priority:" + msg.getJMSPriority() : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", LOW_PRI+1, msg.getJMSPriority());
            assertTrue("not duplicate ", dups[i] == 0);
            dups[i] = 1;

            if (i % batchSize == 0) {
                producerThread.setMessagePriority(HIGH_PRI);
                producerThread.setMessageCount(1);
                producerThread.run();
                LOG.info("High priority message sent, should be able to receive immediately");

                if (i % batchSize*2 == 0) {
                    producerThread.setMessagePriority(HIGH_PRI -1);
                    producerThread.setMessageCount(1);
                    producerThread.run();
                    LOG.info("High -1 priority message sent, should be able to receive immediately");
                }

                if (i % batchSize*4 == 0) {
                    producerThread.setMessagePriority(LOW_PRI);
                    producerThread.setMessageCount(1);
                    producerThread.run();
                    lowLowCount++;
                    LOG.info("Low low priority message sent, should not be able to receive immediately");
                }

                msg = sub.receive(15000);
                assertNotNull("Message was null", msg);
                LOG.info("received hi? : " + msg);
                assertEquals("high priority", HIGH_PRI, msg.getJMSPriority());
                            
                if (i % batchSize*2 == 0) {
                    msg = sub.receive(15000);
                    assertNotNull("Message was null", msg);
                    LOG.info("received hi -1 ? i=" + i + ", " + msg);
                    assertEquals("high priority", HIGH_PRI -1, msg.getJMSPriority());
                }
            }
        }
        for (int i=0; i<lowLowCount; i++) {
            Message msg = sub.receive(15000);
            LOG.debug("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", LOW_PRI, msg.getJMSPriority());
        }
    }


    public void initCombosForTestHighPriorityDeliveryInterleaved() {
        addCombinationValues("useCache", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testHighPriorityDeliveryInterleaved() throws Exception {

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

        ProducerThread producerThread = new ProducerThread(topic, 1, HIGH_PRI);
        producerThread.run();

        producerThread.setMessagePriority(HIGH_PRI -1);
        producerThread.setMessageCount(1);
        producerThread.run();

        producerThread.setMessagePriority(LOW_PRI);
        producerThread.setMessageCount(1);
        producerThread.run();
        LOG.info("Ordered priority messages sent");

        sub = sess.createDurableSubscriber(topic, subName);
        int count = 0;

        Message msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        LOG.info("received " + msg.getJMSMessageID() + ", priority:" + msg.getJMSPriority());
        assertEquals("Message has wrong priority", HIGH_PRI, msg.getJMSPriority());

        producerThread.setMessagePriority(LOW_PRI+1);
        producerThread.setMessageCount(1);
        producerThread.run();

        msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        LOG.info("received " + msg.getJMSMessageID() + ", priority:" + msg.getJMSPriority());
        assertEquals("high priority", HIGH_PRI -1, msg.getJMSPriority());

        msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        LOG.info("received hi? : " + msg);
        assertEquals("high priority", LOW_PRI +1, msg.getJMSPriority());

        msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        LOG.info("received hi? : " + msg);
        assertEquals("high priority", LOW_PRI, msg.getJMSPriority());

        msg = sub.receive(4000);
        assertNull("Message was null", msg);
    }

    // immediatePriorityDispatch is only relevant when cache is exhausted
    public void initCombosForTestHighPriorityDeliveryThroughBackLog() {
        addCombinationValues("useCache", new Object[] {Boolean.FALSE});
        addCombinationValues("immediatePriorityDispatch", new Object[] {Boolean.TRUE});
    }

    public void testHighPriorityDeliveryThroughBackLog() throws Exception {

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

        ProducerThread producerThread = new ProducerThread(topic, 600, LOW_PRI);
        producerThread.run();


        sub = sess.createDurableSubscriber(topic, subName);
        int count = 0;

        for (;count < 300; count++) {
            Message msg = sub.receive(15000);
            assertNotNull("Message was null", msg);
            assertEquals("high priority", LOW_PRI, msg.getJMSPriority());
        }

        producerThread.setMessagePriority(HIGH_PRI);
        producerThread.setMessageCount(1);
        producerThread.run();

        Message msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        assertEquals("high priority", HIGH_PRI, msg.getJMSPriority());

        for (;count < 600; count++) {
            msg = sub.receive(15000);
            assertNotNull("Message was null", msg);
            assertEquals("high priority", LOW_PRI, msg.getJMSPriority());
        }
    }


    public void initCombosForTestHighPriorityNonDeliveryThroughBackLog() {
        addCombinationValues("useCache", new Object[] {Boolean.FALSE});
        addCombinationValues("immediatePriorityDispatch", new Object[] {Boolean.FALSE});
    }

    public void testHighPriorityNonDeliveryThroughBackLog() throws Exception {

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

        ProducerThread producerThread = new ProducerThread(topic, 600, LOW_PRI);
        producerThread.run();


        sub = sess.createDurableSubscriber(topic, subName);
        int count = 0;

        for (;count < 300; count++) {
            Message msg = sub.receive(15000);
            assertNotNull("Message was null", msg);
            assertEquals("high priority", LOW_PRI, msg.getJMSPriority());
        }

        producerThread.setMessagePriority(HIGH_PRI);
        producerThread.setMessageCount(1);
        producerThread.run();

        for (;count < 400; count++) {
            Message msg = sub.receive(15000);
            assertNotNull("Message was null", msg);
            assertEquals("high priority", LOW_PRI, msg.getJMSPriority());
        }

        Message msg = sub.receive(15000);
        assertNotNull("Message was null", msg);
        assertEquals("high priority", HIGH_PRI, msg.getJMSPriority());

        for (;count < 600; count++) {
            msg = sub.receive(15000);
            assertNotNull("Message was null", msg);
            assertEquals("high priority", LOW_PRI, msg.getJMSPriority());
        }
    }

}
