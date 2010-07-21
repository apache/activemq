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
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

abstract public class MessagePriorityTest extends TestCase {

    BrokerService broker;
    PersistenceAdapter adapter;
    
    ActiveMQConnectionFactory factory;
    Connection conn;
    Session sess;
    
    abstract protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception;
    
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("priorityTest");
        adapter = createPersistenceAdapter(true);
        broker.setPersistenceAdapter(adapter);
        PolicyEntry policy = new PolicyEntry();
        policy.setPrioritizedMessages(true);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();
        
        factory = new ActiveMQConnectionFactory("vm://priorityTest");
        conn = factory.createConnection();
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
        
        
        Thread.sleep(100); // get it all propagated
        
        assertTrue(broker.getRegionBroker().getDestinationMap().get(queue).getMessageStore().isPrioritizedMessages());
        assertTrue(broker.getRegionBroker().getDestinationMap().get(topic).getMessageStore().isPrioritizedMessages());
        
        queueProducer.close();
        topicProducer.close();
        
    }
    
}
