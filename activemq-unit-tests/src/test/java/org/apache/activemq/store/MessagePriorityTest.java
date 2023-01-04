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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
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
    public int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    public boolean dispatchAsync = true;
    public boolean prioritizeMessages = true;
    public boolean immediatePriorityDispatch = true;
    public int prefetchVal = 500;
    public int expireMessagePeriod = 30000;

    public int MSG_NUM = 600;
    public int HIGH_PRI = 7;
    public int LOW_PRI = 3;
    public int MED_PRI = 4;

    abstract protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("priorityTest");
        broker.setAdvisorySupport(false);
        adapter = createPersistenceAdapter(true);
        broker.setPersistenceAdapter(adapter);
        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(prioritizeMessages);
        policy.setUseCache(useCache);
        policy.setExpireMessagesPeriod(expireMessagePeriod);
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

        PolicyEntry noCachePolicy = new PolicyEntry();
        noCachePolicy.setUseCache(false);
        noCachePolicy.setPrioritizedMessages(true);
        policyMap.put(new ActiveMQQueue("TEST_LOW_THEN_HIGH_10"), noCachePolicy);

        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        factory = new ActiveMQConnectionFactory("vm://priorityTest");
        factory.setMessagePrioritySupported(true);
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

    @Override
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
        final Queue queue = sess.createQueue("TEST");
        final Topic topic = sess.createTopic("TEST");

        MessageProducer queueProducer = sess.createProducer(queue);
        MessageProducer topicProducer = sess.createProducer(topic);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getRegionBroker().getDestinationMap().get(queue) != null;
            }
        });
        assertTrue(broker.getRegionBroker().getDestinationMap().get(queue).getMessageStore().isPrioritizedMessages());

        Wait.waitFor(new Wait.Condition(){
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getRegionBroker().getDestinationMap().get(topic) != null;
            }
        });
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

        @Override
        public void run() {
            try {
                MessageProducer producer = sess.createProducer(dest);
                producer.setPriority(priority);
                producer.setDeliveryMode(deliveryMode);
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
        addCombinationValues("useCache", new Object[] {Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
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
        addCombinationValues("prefetchVal", new Object[] {Integer.valueOf(1000), Integer.valueOf(MSG_NUM/4)});
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
        addCombinationValues("prefetchVal", new Object[] {Integer.valueOf(1000), Integer.valueOf(MSG_NUM/2)});
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

    public void initCombosForTestQueueBacklog() {
        // the cache limits the priority ordering to available memory
        addCombinationValues("useCache", new Object[] {Boolean.FALSE});
        // expiry processing can fill the cursor with a snapshot of the producer
        // priority, before producers are complete
        addCombinationValues("expireMessagePeriod", new Object[] {Integer.valueOf(0)});
    }

    public void testQueueBacklog() throws Exception {
        final int backlog = 1800;
        ActiveMQQueue queue = (ActiveMQQueue)sess.createQueue("TEST");

        ProducerThread lowPri = new ProducerThread(queue, backlog, LOW_PRI);
        ProducerThread highPri = new ProducerThread(queue, 10, HIGH_PRI);

        lowPri.start();
        lowPri.join();
        highPri.start();
        highPri.join();

        LOG.info("Starting consumer...");
        MessageConsumer queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 500; i++) {
            Message msg = queueConsumer.receive(20000);
            LOG.debug("received i=" + i + ", " + (msg!=null? msg.getJMSMessageID() : null));
            if (msg == null) dumpAllThreads("backlog");
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", i < 10 ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
        }

        final DestinationStatistics destinationStatistics = ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics();
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Enqueues: " + destinationStatistics.getEnqueues().getCount() + ", Dequeues: " + destinationStatistics.getDequeues().getCount());
                return destinationStatistics.getEnqueues().getCount() == backlog + 10 && destinationStatistics.getDequeues().getCount() == 500;
            }
        }, 10000));
    }

    public void initCombosForTestLowThenHighBatch() {
        // the cache limits the priority ordering to available memory
        addCombinationValues("useCache", new Object[] {Boolean.FALSE});
        // expiry processing can fill the cursor with a snapshot of the producer
        // priority, before producers are complete
        addCombinationValues("expireMessagePeriod", new Object[] {Integer.valueOf(0)});
    }

    public void testLowThenHighBatch() throws Exception {
        ActiveMQQueue queue = (ActiveMQQueue)sess.createQueue("TEST_LOW_THEN_HIGH_10");

        ProducerThread producerThread = new ProducerThread(queue, 10, LOW_PRI);
        producerThread.run();

        MessageConsumer queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 10; i++) {
            Message message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", LOW_PRI, message.getJMSPriority());
        }
        queueConsumer.close();

        producerThread.priority = HIGH_PRI;
        producerThread.run();

        queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 10; i++) {
            Message message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", HIGH_PRI, message.getJMSPriority());
        }
        queueConsumer.close();

        producerThread.priority = LOW_PRI;
        producerThread.run();
        producerThread.priority = MED_PRI;
        producerThread.run();

        queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 10; i++) {
            Message message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", MED_PRI, message.getJMSPriority());
        }
        for (int i = 0; i < 10; i++) {
            Message message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", LOW_PRI, message.getJMSPriority());
        }
        queueConsumer.close();

        producerThread.priority = HIGH_PRI;
        producerThread.run();

        queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 10; i++) {
            Message message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", HIGH_PRI, message.getJMSPriority());
        }
        queueConsumer.close();
    }

    public void testInterleaveHiNewConsumerGetsHi() throws Exception {
        ActiveMQQueue queue = (ActiveMQQueue) sess.createQueue("TEST");
        doTestInterleaveHiNewConsumerGetsHi(queue);
    }

    public void testInterleaveHiNewConsumerGetsHiPull() throws Exception {
        ActiveMQQueue queue = (ActiveMQQueue) sess.createQueue("TEST?consumer.prefetchSize=0");
        doTestInterleaveHiNewConsumerGetsHi(queue);
    }

    public void doTestInterleaveHiNewConsumerGetsHi(ActiveMQQueue queue) throws Exception {

        // one hi sandwich
        ProducerThread producerThread = new ProducerThread(queue, 3, LOW_PRI);
        producerThread.run();
        producerThread = new ProducerThread(queue, 1, HIGH_PRI);
        producerThread.run();
        producerThread = new ProducerThread(queue, 3, LOW_PRI);
        producerThread.run();

        // consume hi
        MessageConsumer queueConsumer = sess.createConsumer(queue);
        Message message = queueConsumer.receive(10000);
        assertNotNull("expect #", message);
        assertEquals("correct priority", HIGH_PRI, message.getJMSPriority());
        queueConsumer.close();

        // last hi
        producerThread = new ProducerThread(queue, 3, LOW_PRI);
        producerThread.run();
        producerThread = new ProducerThread(queue, 1, HIGH_PRI);
        producerThread.run();

        // consume hi
        queueConsumer = sess.createConsumer(queue);
        message = queueConsumer.receive(10000);
        assertNotNull("expect #", message);
        assertEquals("correct priority", HIGH_PRI, message.getJMSPriority());
        queueConsumer.close();

        // consume the rest
        queueConsumer = sess.createConsumer(queue);
        for (int i = 0; i < 9; i++) {
            message = queueConsumer.receive(10000);
            assertNotNull("expect #" + i, message);
            assertEquals("correct priority", LOW_PRI, message.getJMSPriority());
        }
        queueConsumer.close();
    }

    public void initCombosForTestEveryXHi() {
        // the cache limits the priority ordering to available memory
        addCombinationValues("useCache", new Object[] {Boolean.FALSE, Boolean.TRUE});
        // expiry processing can fill the cursor with a snapshot of the producer
        // priority, before producers are complete
        addCombinationValues("expireMessagePeriod", new Object[] {Integer.valueOf(0)});
    }

    public void testEveryXHi() throws Exception {

        ActiveMQQueue queue = (ActiveMQQueue)sess.createQueue("TEST");

        // ensure we hit the limit to disable the cache
        broker.getDestinationPolicy().getEntryFor(queue).setMemoryLimit(50*1024);
        final String payload = new String(new byte[1024]);
        final int numMessages = 500;

        final AtomicInteger received = new AtomicInteger(0);
        MessageConsumer queueConsumer = sess.createConsumer(queue);
        queueConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                received.incrementAndGet();

                if (received.get() < 20) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        MessageProducer producer = sess.createProducer(queue);
        for (int i = 0; i < numMessages; i++) {
            Message message = sess.createMessage();
            message.setStringProperty("payload", payload);
            if (i % 5 == 0) {
                message.setJMSPriority(9);
            } else {
                message.setJMSPriority(4);
            }
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, message.getJMSPriority(), Message.DEFAULT_TIME_TO_LIVE);
        }

        assertTrue("Got all", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return numMessages == received.get();
            }
        }));


        final DestinationStatistics destinationStatistics = ((RegionBroker)broker.getRegionBroker()).getDestinationStatistics();
        assertTrue("Nothing else Like dlq involved", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Enqueues: " + destinationStatistics.getEnqueues().getCount() + ", Dequeues: " + destinationStatistics.getDequeues().getCount());
                return destinationStatistics.getEnqueues().getCount() == numMessages && destinationStatistics.getDequeues().getCount() == numMessages;
            }
        }, 10000));

        // do it again!
        received.set(0);
        destinationStatistics.reset();
        for (int i = 0; i < numMessages; i++) {
            Message message = sess.createMessage();
            message.setStringProperty("payload", payload);
            if (i % 5 == 0) {
                message.setJMSPriority(9);
            } else {
                message.setJMSPriority(4);
            }
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, message.getJMSPriority(), Message.DEFAULT_TIME_TO_LIVE);
        }

        assertTrue("Got all", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return numMessages == received.get();
            }
        }));


        assertTrue("Nothing else Like dlq involved", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Enqueues: " + destinationStatistics.getEnqueues().getCount() + ", Dequeues: " + destinationStatistics.getDequeues().getCount());
                return destinationStatistics.getEnqueues().getCount() == numMessages && destinationStatistics.getDequeues().getCount() == numMessages;
            }
        }, 10000));

        queueConsumer.close();
    }
}
