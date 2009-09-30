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
package org.apache.activemq.broker.region.cursors;

/**
 * A StoreBasedCursorTest
 *
 */

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.usage.SystemUsage;

import junit.framework.TestCase;

public class StoreBasedCursorTest extends TestCase {
    protected String bindAddress = "tcp://localhost:60706";
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    int messageSize = 1024;
    int memoryLimit = 5 * messageSize;
    
    protected void setUp() throws Exception {
        super.setUp();
        if (broker == null) {
            broker = new BrokerService();
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }
    
    protected void start() throws Exception {
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://localhost?jms.alwaysSyncSend=true");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("QUEUE." + this.getClass().getName());
    }
    
    protected void stop() throws Exception {
        session.close();
        connection.close();
        broker.stop();
        broker = null;
    }
    
    protected void configureBroker(long memoryLimit, long systemLimit) throws Exception {
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector(bindAddress);
        broker.setPersistent(true);
        
        SystemUsage systemUsage = broker.getSystemUsage();
        systemUsage.setSendFailIfNoSpace(true);
        systemUsage.getMemoryUsage().setLimit(systemLimit);
        
        PolicyEntry policy = new PolicyEntry();
        policy.setProducerFlowControl(true);
        policy.setUseCache(true);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
    }
    
    protected String createMessageText(int index) {
        StringBuffer buffer = new StringBuffer(messageSize);
        buffer.append("Message: " + index + " sent at: " + new Date());
        if (buffer.length() > messageSize) {
            return buffer.substring(0, messageSize);
        }
        for (int i = buffer.length(); i < messageSize; i++) {
            buffer.append(' ');
        }
        return buffer.toString();
    }
    
    protected void sendMessages(int deliveryMode) throws Exception {
        start();
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(deliveryMode);
        int i =0;
        try {
            for (i = 0; i < 200; i++) {
                TextMessage message = session.createTextMessage(createMessageText(i));
                producer.send(message);
            }
        } catch (javax.jms.ResourceAllocationException e) {
        	e.printStackTrace();
            fail(e.getMessage() + " num msgs = " + i + ". percentUsage = " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
        }
        stop();
    }
    
    // use QueueStorePrefetch
    public void testTwoUsageEqualPersistent() throws Exception {
        configureBroker(memoryLimit, memoryLimit);
        sendMessages(DeliveryMode.PERSISTENT);
    }
    
    public void testUseCachePersistent() throws Exception {
        int limit = memoryLimit / 2;
        configureBroker(limit, memoryLimit);
        sendMessages(DeliveryMode.PERSISTENT);
    }
    
    public void testMemoryUsageLowPersistent() throws Exception {
        configureBroker(memoryLimit, 10 * memoryLimit);
        sendMessages(DeliveryMode.PERSISTENT);
    }
    
    // use FilePendingMessageCursor
    public void testTwoUsageEqualNonPersistent() throws Exception {
        configureBroker(memoryLimit, memoryLimit);
        sendMessages(DeliveryMode.NON_PERSISTENT);
    }
    
    public void testMemoryUsageLowNonPersistent() throws Exception {
        configureBroker(memoryLimit, 10 * memoryLimit);
        sendMessages(DeliveryMode.NON_PERSISTENT);
    }
}