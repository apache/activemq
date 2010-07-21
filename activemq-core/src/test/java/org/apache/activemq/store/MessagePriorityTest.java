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
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract public class MessagePriorityTest extends CombinationTestSupport {
    
    private static final Log LOG = LogFactory.getLog(MessagePriorityTest.class);

    BrokerService broker;
    PersistenceAdapter adapter;
    
    ActiveMQConnectionFactory factory;
    Connection conn;
    Session sess;
    
    public boolean useCache;
    
    int MSG_NUM = 1000;
    int HIGH_PRI = 7;
    int LOW_PRI = 3;
    
    abstract protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception;
    
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("priorityTest");
        adapter = createPersistenceAdapter(true);
        broker.setPersistenceAdapter(adapter);
        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(true);
        policy.setUseCache(useCache);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
        
        factory = new ActiveMQConnectionFactory("vm://priorityTest");
        conn = factory.createConnection();
        conn.setClientID("priority");
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    
    protected void tearDown() throws Exception {
        sess.close();
        conn.close();
        
        broker.stop();
        broker.waitUntilStopped();
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
    
    class ProducerThread extends Thread {

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
            Message msg = queueConsumer.receive(1000);
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", i < MSG_NUM ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
        }
    }
    
    protected Message createMessage(int priority) throws Exception {
        final String text = "Message with priority " + priority;
        Message msg = sess.createTextMessage(text);
        LOG.info("Sending  " + text);
        return msg;
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
            Message msg = sub.receive(1000);
            assertNotNull(msg);
            assertEquals("Message " + i + " has wrong priority", i < MSG_NUM ? HIGH_PRI : LOW_PRI, msg.getJMSPriority());
        }
        
    }
    
}
